from django.core.management.base import BaseCommand
from django.conf import settings
from syncapp.services import shopify_get_locations, resolve_location_id


class Command(BaseCommand):
    help = "Show the Shopify Location configured/resolved for inventory assignment"

    def handle(self, *args, **options):
        explicit_id = getattr(settings, "SHOPIFY_LOCATION_ID", "")
        name_pref = getattr(settings, "SHOPIFY_LOCATION_NAME", "")
        resolved_id = resolve_location_id()

        self.stdout.write(self.style.MIGRATE_HEADING("Shopify Location Configuration"))
        self.stdout.write(f"SHOPIFY_LOCATION_ID (env): {explicit_id or '(not set)'}")
        self.stdout.write(f"SHOPIFY_LOCATION_NAME (env): {name_pref or '(not set)'}")
        self.stdout.write(f"Resolved location_id: {resolved_id or '(none)'}")

        try:
            locations = shopify_get_locations()
        except Exception as e:
            self.stderr.write(
                self.style.ERROR(f"Failed to fetch locations from Shopify: {e}")
            )
            return

        by_id = {str(l.get("id")): l for l in locations}
        match = by_id.get(str(resolved_id)) if resolved_id else None

        if match:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Resolved location → id={match.get('id')}, name='{match.get('name')}', active={match.get('active')}"
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING("Resolved id not found among fetched locations.")
            )
            self.stdout.write("Available locations:")
            for l in locations:
                self.stdout.write(
                    f"- id={l.get('id')}, name='{l.get('name')}', active={l.get('active')}"
                )
