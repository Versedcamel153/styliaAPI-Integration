from celery import shared_task
from django.utils import timezone
from django.conf import settings
import logging

from .services import (
    reconcile_shopify_state,
    ingest_stylia,
    push_to_shopify,
    process_creations,
    process_updates,
    retry_pending_products,
    ensure_location_assignments,
)
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
    # Retry any stuck pending products first (products that failed in previous syncs)
    retry_res = retry_pending_products()
    # Process creations first, then updates (separate queues)
    c_res = process_creations()
    u_res = process_updates()
    # Ensure inventory is connected and quantities set at the configured location each run
    loc_res = ensure_location_assignments()
    log.products_created = c_res["summary"]["created"] + retry_res["summary"]["created"]
    log.products_updated = u_res["summary"]["updated"] + retry_res["summary"]["updated"]
    # Persist count of products that already existed in Shopify and were only mapped/linked
    log.products_existing = (
        c_res["summary"].get("existing", 0)
        + u_res["summary"].get("existing", 0)
        + retry_res["summary"].get("existing", 0)
    )
    log.products_errors = (
        c_res["summary"]["errors"]
        + u_res["summary"]["errors"]
        + retry_res["summary"]["errors"]
    )
    log.stylia_products_count = t1["summary"]["unique"]
    log.mark_completed(success=True)

    return {
        "timestamp": timezone.now().isoformat(),
        "reconcile": t0,
        "ingest": t1,
        "retry": retry_res,
        "creations": c_res,
        "updates": u_res,
        "location": loc_res,
    }


@shared_task
def push_product(product_id):
    return push_single_product(product_id)


@shared_task
def run_process_creations():
    return process_creations()


@shared_task
def run_process_updates():
    return process_updates()


@shared_task
def run_retry_pending():
    """Manually retry products stuck in pending status."""
    return retry_pending_products()
