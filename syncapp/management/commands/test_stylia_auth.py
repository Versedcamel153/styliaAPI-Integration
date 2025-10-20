from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import requests


class Command(BaseCommand):
    help = "Test Stylia API authentication by calling /sync/getItemsList and printing status + body."

    def add_arguments(self, parser):
        parser.add_argument(
            "--url",
            required=False,
            help="Stylia base url (overrides STYLIA_BASE_URL in settings)",
        )

    def handle(self, *args, **options):
        base = options.get("url") or getattr(settings, "STYLIA_BASE_URL", None)
        if not base:
            raise CommandError(
                "STYLIA_BASE_URL not set in settings or --url not provided"
            )
        url = f"{base.rstrip('/')}/sync/getItemsList"
        username = getattr(settings, "STYLIA_USERNAME", None)
        password = getattr(settings, "STYLIA_PASSWORD", None)
        self.stdout.write(f"Calling {url} with username={username!r} ...")
        try:
            r = requests.get(url, auth=(username, password), timeout=30)
        except Exception as e:
            raise CommandError(f"Request failed: {e}")
        self.stdout.write(f"Status: {r.status_code}\n")
        try:
            self.stdout.write(r.text[:5000])
        except Exception:
            self.stdout.write("<unreadable response body>")
