from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.paginator import Paginator
from .models import StyliaProduct
from .tasks import run_full_sync, push_product
import json
from django.conf import settings
from django.shortcuts import redirect
from django.urls import reverse
from .models import ShopifyApp
import requests
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt

from . import services
import logging

logger = logging.getLogger(__name__)


def dashboard(request):
    # Filters
    # Default to 'all' so users see everything (including failed) at first load
    show = request.GET.get("show", "all")  # pushed|all|pending|failed|deleted

    qs = StyliaProduct.objects.all().order_by("-updated_at")
    if show == "pushed":
        qs = qs.exclude(shopify_id__isnull=True).exclude(shopify_id="")
    elif show == "pending":
        qs = qs.filter(sync_status="pending")
    elif show == "failed":
        qs = qs.filter(sync_status="failed")
    elif show == "deleted":
        qs = qs.filter(sync_status="deleted")

    paginator = Paginator(qs, 25)
    page = request.GET.get("page")
    products = paginator.get_page(page)

    stats = {
        "total": StyliaProduct.objects.count(),
        "pushed": StyliaProduct.objects.exclude(shopify_id__isnull=True)
        .exclude(shopify_id="")
        .count(),
        "pending": StyliaProduct.objects.filter(sync_status="pending").count(),
        "failed": StyliaProduct.objects.filter(sync_status="failed").count(),
        "deleted": StyliaProduct.objects.filter(sync_status="deleted").count(),
    }

    return render(
        request,
        "syncapp/dashboard.html",
        {
            "products": products,
            "stats": stats,
            "show": show,
            "shopify_store_url": settings.SHOPIFY_STORE_URL,
        },
    )


@csrf_exempt
def api_trigger_sync(request):
    if request.method != "POST":
        return JsonResponse({"success": False, "error": "POST required"}, status=405)
    # Queue sync asynchronously
    task = run_full_sync.delay()
    return JsonResponse({"success": True, "task_id": task.id})


@csrf_exempt
def api_reenable_product(request, pk):
    if request.method != "POST":
        return JsonResponse({"success": False, "error": "POST required"}, status=405)
    product = get_object_or_404(StyliaProduct, pk=pk)
    payload = json.loads(request.body) if request.body else {}
    # Re-enable by setting back to pending; do not clear shopify_id by default
    product.sync_status = "pending"
    product.save()
    # Optionally trigger an immediate sync if requested
    push_now = request.GET.get("push") == "1" or payload.get("push_now") is True
    if push_now:
        try:
            # Use the single-product push task to avoid starting a full sync
            task = push_product.delay(product.pk)
            return JsonResponse(
                {"success": True, "task_id": task.id, "pushed_immediately": True}
            )
        except Exception as e:
            return JsonResponse({"success": False, "error": str(e)}, status=500)
    return JsonResponse({"success": True, "pushed_immediately": False})


def shopify_install(request):
    """Redirect merchant to Shopify OAuth grant page."""
    shop = request.GET.get("shop") or settings.SHOPIFY_STORE_URL
    if not shop:
        return JsonResponse(
            {"success": False, "error": "shop parameter required"}, status=400
        )

    api_key = getattr(settings, "SHOPIFY_API_KEY", None)
    redirect_uri = getattr(settings, "APP_BASE_URL", "http://localhost:8000") + reverse(
        "shopify_callback"
    )
    scopes = getattr(
        settings,
        "SHOPIFY_OAUTH_SCOPES",
        "write_products,write_inventory,read_products,read_locations",
    )

    # Build install URL
    install_url = f"https://{shop}/admin/oauth/authorize?client_id={api_key}&scope={scopes}&redirect_uri={redirect_uri}"
    return redirect(install_url)


def shopify_callback(request):
    """Exchange code for access token and persist it."""
    code = request.GET.get("code")
    shop = request.GET.get("shop")
    if not code or not shop:
        return JsonResponse(
            {"success": False, "error": "missing code/shop"}, status=400
        )

    token_url = f"https://{shop}/admin/oauth/access_token"
    data = {
        "client_id": settings.SHOPIFY_API_KEY,
        "client_secret": settings.SHOPIFY_API_SECRET,
        "code": code,
    }
    r = requests.post(token_url, json=data, timeout=20)
    if r.status_code != 200:
        return JsonResponse(
            {
                "success": False,
                "error": f"token exchange failed: {r.status_code} - {r.text}",
            },
            status=500,
        )

    payload = r.json()
    access_token = payload.get("access_token")
    scopes = payload.get("scope") or ""
    if not access_token:
        return JsonResponse(
            {"success": False, "error": "no access_token returned"}, status=500
        )

    obj, created = ShopifyApp.objects.update_or_create(
        shop_domain=shop,
        defaults={
            "access_token": access_token,
            "scopes": scopes,
            "installed_at": timezone.now(),
        },
    )

    # Redirect to dashboard with a simple success message
    return redirect(reverse("sync_dashboard") + "?installed=1")


@csrf_exempt
def delete_product(request, pk):
    """Delete a product that hasnt been pushed yet."""
    # For consistency with other API endpoints, require POST and be CSRF-exempt
    from django.views.decorators.csrf import csrf_exempt

    @csrf_exempt
    def _inner(req, pk):
        if req.method != "POST":
            return JsonResponse(
                {"success": False, "error": "POST required"}, status=405
            )
        product = get_object_or_404(StyliaProduct, pk=pk)
        if product.shopify_id:
            return JsonResponse(
                {"success": False, "error": "Cannot delete a product already pushed"},
                status=400,
            )
        product.delete()
        return JsonResponse({"success": True})

    return _inner(request, pk)


@csrf_exempt
def api_delete_all_pushed(request):
    """Delete all products that were pushed to Shopify (best-effort).

    This will attempt to DELETE the product via Shopify Admin API, remove any
    Variant mappings, and mark the StyliaProduct as deleted.
    """
    if request.method != "POST":
        return JsonResponse({"success": False, "error": "POST required"}, status=405)

    from .models import Variant

    # Materialize product list to avoid cursor invalidation during deletes
    products = list(StyliaProduct.objects.values("id", "shopify_id", "model_code"))
    attempted_shopify = 0
    shopify_deleted = 0
    db_deleted = 0
    errors = []
    for row in products:
        pk = row.get("id")
        shopify_id = row.get("shopify_id")
        model_code = row.get("model_code")
        if shopify_id:
            attempted_shopify += 1
            try:
                url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/products/{shopify_id}.json"
                r = services.shopify_request("DELETE", url, timeout=30)
                if r.status_code in (200, 202, 204) or r.status_code == 404:
                    shopify_deleted += 1
                else:
                    errors.append(
                        {
                            "model_code": model_code,
                            "status": r.status_code,
                            "body": r.text[:1000],
                        }
                    )
            except Exception as e:
                logger.exception("Failed to delete product %s from Shopify", model_code)
                errors.append({"model_code": model_code, "error": str(e)})
        # Regardless of Shopify result or missing shopify_id, remove Variant mappings and delete DB row
        try:
            Variant.objects.filter(stylia_product_id=pk).delete()
        except Exception:
            logger.exception("Failed deleting Variant rows for %s", model_code)
        try:
            StyliaProduct.objects.filter(pk=pk).delete()
            db_deleted += 1
        except Exception:
            logger.exception("Failed deleting StyliaProduct %s from DB", model_code)
            errors.append({"model_code": model_code, "error": "db_delete_failed"})

    return JsonResponse(
        {
            "success": True,
            "attempted_shopify_deletes": attempted_shopify,
            "shopify_deleted": shopify_deleted,
            "db_deleted": db_deleted,
            "errors": errors,
        }
    )
