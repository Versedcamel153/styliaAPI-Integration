from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import requests


class Command(BaseCommand):
    help = "Exchange a Shopify OAuth code for an access token and persist it to ShopifyApp."

    def add_arguments(self, parser):
        parser.add_argument(
            "--shop", required=True, help="Shop domain, e.g. example.myshopify.com"
        )
        parser.add_argument(
            "--code", required=True, help="OAuth code returned by Shopify"
        )
        parser.add_argument(
            "--api-key",
            required=False,
            help="Shopify API Key (client_id). Falls back to settings.SHOPIFY_API_KEY",
        )
        parser.add_argument(
            "--api-secret",
            required=False,
            help="Shopify API Secret. Falls back to settings.SHOPIFY_API_SECRET",
        )

    def handle(self, *args, **options):
        shop = options.get("shop")
        code = options.get("code")
        api_key = options.get("api_key") or getattr(settings, "SHOPIFY_API_KEY", None)
        api_secret = options.get("api_secret") or getattr(
            settings, "SHOPIFY_API_SECRET", None
        )

        if not api_key or not api_secret:
            raise CommandError(
                "API key and secret must be provided via args or settings.SHOPIFY_API_KEY/SHOPIFY_API_SECRET"
            )

        token_url = f"https://{shop}/admin/oauth/access_token"
        payload = {"client_id": api_key, "client_secret": api_secret, "code": code}
        self.stdout.write(f"Exchanging code for access token at {token_url} ...")
        r = requests.post(token_url, json=payload, timeout=20)
        if r.status_code != 200:
            raise CommandError(f"Token exchange failed: {r.status_code} - {r.text}")

        data = r.json()
        access_token = data.get("access_token")
        scope = data.get("scope") or ""
        if not access_token:
            raise CommandError(f"No access_token returned: {data}")

        # Persist to ShopifyApp
        from syncapp.models import ShopifyApp

        obj, created = ShopifyApp.objects.update_or_create(
            shop_domain=shop,
            defaults={"access_token": access_token, "scopes": scope},
        )
        if created:
            self.stdout.write(self.style.SUCCESS(f"Created ShopifyApp for {shop}"))
        else:
            self.stdout.write(self.style.SUCCESS(f"Updated ShopifyApp for {shop}"))
        self.stdout.write(
            self.style.SUCCESS(f"Access token saved (length={len(access_token)})")
        )
