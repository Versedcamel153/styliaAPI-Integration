from django.urls import path
from . import views

urlpatterns = [
    path("", views.dashboard, name="sync_dashboard"),
    path("api/trigger-sync/", views.api_trigger_sync, name="api_trigger_sync"),
    path(
        "api/products/<int:pk>/reenable/",
        views.api_reenable_product,
        name="api_reenable_product",
    ),
    path("shopify/install/", views.shopify_install, name="shopify_install"),
    path("shopify/callback/", views.shopify_callback, name="shopify_callback"),
]
