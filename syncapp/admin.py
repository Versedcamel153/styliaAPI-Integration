from django.contrib import admin
from .models import StyliaProduct, SyncLog, ShopifyApp


@admin.register(StyliaProduct)
class StyliaProductAdmin(admin.ModelAdmin):
    list_display = (
        "model_code",
        "brand",
        "sync_status",
        "shopify_id",
        "last_synced_at",
    )
    search_fields = ("model_code", "brand", "title", "shopify_id")


@admin.register(SyncLog)
class SyncLogAdmin(admin.ModelAdmin):
    list_display = (
        "started_at",
        "completed_at",
        "success",
        "products_created",
        "products_updated",
        "products_existing",
        "products_errors",
    )


@admin.register(ShopifyApp)
class ShopifyAppAdmin(admin.ModelAdmin):
    list_display = ("shop_domain", "installed_at")
    readonly_fields = ("installed_at",)
