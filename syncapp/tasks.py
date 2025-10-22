from celery import shared_task
from django.utils import timezone
from django.conf import settings
import logging

from .services import reconcile_shopify_state, ingest_stylia, push_to_shopify
from .services import push_single_product
from .models import SyncLog

logger = logging.getLogger(__name__)


@shared_task
def run_full_sync():
    """Run Step 0 (reconcile), Step 1 (ingest), Step 2 (push)"""
    # simple guard: skip if a recent sync started and hasn't completed
    active = (
        SyncLog.objects.filter(completed_at__isnull=True).order_by("started_at").first()
    )
    if active:
        # allow auto-expire of stale locks (seconds)
        ttl = getattr(settings, "SYNCLOG_TTL_SECONDS", 60 * 60)
        age = timezone.now() - active.started_at
        if age.total_seconds() > ttl:
            # mark the stale run completed with an error and continue
            logger.warning(
                "Expiring stale SyncLog started_at=%s (age=%.0fs) and continuing",
                active.started_at,
                age.total_seconds(),
            )
            try:
                active.mark_completed(success=False, error_message="stale_lock_expired")
            except Exception:
                logger.exception("Failed to expire stale SyncLog %s", active.pk)
        else:
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
