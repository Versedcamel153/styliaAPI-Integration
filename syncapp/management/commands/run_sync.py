from django.core.management.base import BaseCommand
from syncapp.tasks import run_full_sync


class Command(BaseCommand):
    help = "Run Stylia → DB → Shopify sync now"

    def handle(self, *args, **options):
        result = run_full_sync()
        self.stdout.write(self.style.SUCCESS("✅ Sync completed"))
        self.stdout.write(str(result))
