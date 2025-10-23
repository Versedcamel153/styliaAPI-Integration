from django.core.management.base import BaseCommand
from django.db.models import Q
from syncapp.models import StyliaProduct, Variant
from syncapp import services
import time
from django.conf import settings


class Command(BaseCommand):
    help = (
        "Scan StyliaProduct rows with a shopify_id, fetch live Shopify product, and "
        "persist Variant mappings (sku→shopify_variant_id, inventory_item_id, price, product_id)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--shopify-id",
            dest="shopify_id",
            help="Repair only this Shopify product id (or comma-separated list)",
        )
        parser.add_argument(
            "--model-code",
            dest="model_code",
            help="Repair only this model_code (or comma-separated list)",
        )
        parser.add_argument(
            "--limit", dest="limit", type=int, default=None, help="Max rows to process"
        )
        parser.add_argument(
            "--dry-run",
            dest="dry_run",
            action="store_true",
            default=False,
            help="Do not write to DB; just print what would be changed",
        )
        parser.add_argument(
            "--sleep",
            dest="sleep",
            type=float,
            default=None,
            help="Seconds to sleep between Shopify calls (defaults to SHOPIFY_MIN_SLEEP)",
        )

    def handle(self, *args, **options):
        shopify_id_opt = options.get("shopify_id")
        model_code_opt = options.get("model_code")
        limit = options.get("limit")
        dry_run = options.get("dry_run")
        sleep_seconds = (
            options.get("sleep")
            if options.get("sleep") is not None
            else float(getattr(settings, "SHOPIFY_MIN_SLEEP", 0.8))
        )

        qs = StyliaProduct.objects.exclude(shopify_id__isnull=True).exclude(
            shopify_id=""
        )
        if shopify_id_opt:
            ids = [s.strip() for s in shopify_id_opt.split(",") if s.strip()]
            qs = qs.filter(shopify_id__in=ids)
        if model_code_opt:
            mcs = [s.strip() for s in model_code_opt.split(",") if s.strip()]
            qs = qs.filter(model_code__in=mcs)
        qs = qs.order_by("updated_at")
        if limit:
            qs = qs[:limit]

        total = qs.count()
        updated = 0
        skipped = 0
        errors = 0

        self.stdout.write(self.style.NOTICE(f"Scanning {total} products..."))

        for sp in qs.iterator():
            try:
                r = services.shopify_get_product(sp.shopify_id)
                if r.status_code != 200:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Shopify GET product {sp.shopify_id} returned {r.status_code}"
                        )
                    )
                    errors += 1
                    continue
                prod = r.json().get("product") or {}
                # update handle if missing
                if not sp.shopify_handle and prod.get("handle"):
                    if dry_run:
                        self.stdout.write(
                            self.style.WARNING(
                                f"Would set shopify_handle for {sp.model_code} -> {prod.get('handle')}"
                            )
                        )
                    else:
                        sp.shopify_handle = prod.get("handle")
                        sp.save(update_fields=["shopify_handle"])

                # persist variant mappings by SKU
                variants = prod.get("variants") or []
                for v in variants:
                    sku = v.get("sku")
                    if not sku:
                        continue
                    defaults = {
                        "shopify_variant_id": v.get("id"),
                        "inventory_item_id": v.get("inventory_item_id"),
                        "shopify_product_id": prod.get("id"),
                        "price": v.get("price"),
                    }
                    if dry_run:
                        self.stdout.write(
                            self.style.WARNING(
                                f"Would map SKU {sku} -> variant_id={defaults['shopify_variant_id']} inv_item={defaults['inventory_item_id']}"
                            )
                        )
                    else:
                        Variant.objects.update_or_create(
                            stylia_product=sp, sku=sku, defaults=defaults
                        )
                updated += 1
                # polite pacing between calls
                if sleep_seconds:
                    time.sleep(sleep_seconds)
            except Exception as e:
                errors += 1
                self.stdout.write(
                    self.style.ERROR(
                        f"Error repairing {sp.model_code} (shopify_id={sp.shopify_id}): {e}"
                    )
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. total={total} updated={updated} skipped={skipped} errors={errors}"
            )
        )
