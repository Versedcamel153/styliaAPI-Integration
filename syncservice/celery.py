import os
from celery import Celery
from celery.schedules import crontab

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "syncservice.settings")

app = Celery("syncservice")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    # Run sync every 30 minutes
    from syncapp.tasks import run_full_sync

    sender.add_periodic_task(
        crontab(minute="*/30"),
        run_full_sync.s(),
        name="stylia_shopify_sync_every_30_minutes",
    )
