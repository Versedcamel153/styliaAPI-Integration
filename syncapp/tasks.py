from celery import shared_task
from django.utils import timezone
from .services import reconcile_shopify_state, ingest_stylia, push_to_shopify
from .services import push_single_product
from .models import SyncLog


@shared_task
def run_full_sync():
    """Run Step 0 (reconcile), Step 1 (ingest), Step 2 (push)"""
    # simple guard: skip if a recent sync started and hasn't completed
    active = (
        SyncLog.objects.filter(completed_at__isnull=True).order_by("started_at").first()
    )
    if active:
        return {"success": False, "error": "sync_already_running"}

    log = SyncLog.objects.create(started_at=timezone.now())
    t0 = reconcile_shopify_state()
    t1 = ingest_stylia()
    t2 = push_to_shopify()
    log.products_created = t2["summary"]["created"]
    log.products_updated = t2["summary"]["updated"]
    log.products_errors = t2["summary"]["errors"]
    log.stylia_products_count = t1["summary"]["unique"]
    log.mark_completed(success=True)

    return {
        "timestamp": timezone.now().isoformat(),
        "reconcile": t0,
        "ingest": t1,
        "push": t2,
    }


@shared_task
def push_product(product_id):
    return push_single_product(product_id)
