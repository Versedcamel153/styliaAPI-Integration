from django.db import models
from django.utils import timezone


class StyliaProduct(models.Model):
    model_code = models.CharField(max_length=100, unique=True, db_index=True)
    brand = models.CharField(max_length=100)
    shopify_id = models.CharField(
        max_length=50, unique=True, null=True, blank=True, db_index=True
    )
    shopify_handle = models.CharField(max_length=255, blank=True, db_index=True)
    title = models.CharField(max_length=255)
    price = models.DecimalField(max_digits=10, decimal_places=2)
    total_stock = models.IntegerField(default=0)
    variant_count = models.IntegerField(default=0)
    product_type = models.CharField(max_length=100, blank=True)
    is_active_in_stylia = models.BooleanField(default=True)
    sync_status = models.CharField(
        max_length=20,
        choices=[
            ("pending", "Pending"),
            ("active", "Active"),
            ("updated", "Updated"),
            ("failed", "Failed"),
            ("deleted", "Deleted"),
        ],
        default="pending",
    )
    data_hash = models.CharField(max_length=64)
    shopify_data = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_synced_at = models.DateTimeField(default=timezone.now)
    # Inventory location tracking
    location_assigned = models.BooleanField(default=False)
    location_last_error = models.TextField(blank=True)
    # Push error tracking
    push_last_error = models.TextField(blank=True)
    push_error_count = models.IntegerField(default=0)

    def mark_as_synced(self):
        self.sync_status = "active"
        self.last_synced_at = timezone.now()
        # clear push error info on success
        self.push_last_error = ""
        self.push_error_count = 0
        self.save()

    def mark_as_failed(self, message=None):
        self.sync_status = "failed"
        if message:
            self.push_last_error = str(message)[:2000]
            self.push_error_count = (self.push_error_count or 0) + 1
        else:
            self.push_error_count = (self.push_error_count or 0) + 1
        self.save()

    def mark_as_deleted(self):
        self.sync_status = "deleted"
        self.save()


class SyncLog(models.Model):
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    success = models.BooleanField(default=False)
    error_message = models.TextField(blank=True)
    products_created = models.IntegerField(default=0)
    products_updated = models.IntegerField(default=0)
    products_deleted = models.IntegerField(default=0)
    products_errors = models.IntegerField(default=0)
    stylia_products_count = models.IntegerField(default=0)

    def mark_completed(self, success=True, error_message=None):
        self.completed_at = timezone.now()
        self.success = success
        if error_message:
            self.error_message = error_message
        self.save()


class ShopifyApp(models.Model):
    """Persist installed shop and admin access token for API calls."""

    shop_domain = models.CharField(
        max_length=255,
        unique=True,
        help_text="Shopify shop domain, e.g. example-store.myshopify.com",
    )
    access_token = models.CharField(max_length=255, help_text="Admin API access token")
    scopes = models.CharField(max_length=512, blank=True)
    installed_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.shop_domain}"
