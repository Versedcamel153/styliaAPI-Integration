from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.paginator import Paginator
from .models import StyliaProduct
from .tasks import run_full_sync
import json
from django.conf import settings


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
            task = run_full_sync.delay()
            return JsonResponse(
                {"success": True, "task_id": task.id, "pushed_immediately": True}
            )
        except Exception as e:
            return JsonResponse({"success": False, "error": str(e)}, status=500)
    return JsonResponse({"success": True, "pushed_immediately": False})
