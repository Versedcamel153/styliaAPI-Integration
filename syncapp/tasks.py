from celery import shared_task
from django.utils import timezone
from .services import reconcile_shopify_state, ingest_stylia, push_to_shopify


@shared_task
def run_full_sync():
    """Run Step 0 (reconcile), Step 1 (ingest), Step 2 (push)"""
    t0 = reconcile_shopify_state()
    t1 = ingest_stylia()
    t2 = push_to_shopify()
    return {
        "timestamp": timezone.now().isoformat(),
        "reconcile": t0,
        "ingest": t1,
        "push": t2,
    }
