import json
import hashlib
import requests
from django.conf import settings
import logging
import traceback
import time
import random

logger = logging.getLogger(__name__)


def get_stylia_sync_list():
    base = getattr(settings, "STYLIA_BASE_URL", None)
    if not base:
        raise RuntimeError(
            "STYLIA_BASE_URL is not set in settings. Set the env var STYLIA_BASE_URL to the Stylia API base URL."
        )
    if not base.startswith("http://") and not base.startswith("https://"):
        raise RuntimeError(f"STYLIA_BASE_URL does not look like a URL: {base}")

    # ensure credentials are present
    if not getattr(settings, "STYLIA_USERNAME", None) or not getattr(
        settings, "STYLIA_PASSWORD", None
    ):
        raise RuntimeError(
            "STYLIA_USERNAME or STYLIA_PASSWORD is not set. Check your .env or environment variables."
        )

    url = f"{base.rstrip('/')}/sync/getItemsList"
    try:
        r = requests.get(
            url,
            auth=(settings.STYLIA_USERNAME, settings.STYLIA_PASSWORD),
            timeout=30,
        )
        # If the endpoint returns an error, capture body for diagnosis
        if r.status_code >= 400:
            body = None
            try:
                body = r.text
            except Exception:
                body = "<unreadable response body>"
            logger.warning(
                "Stylia API %s returned %s: %s", url, r.status_code, body[:2000]
            )
            hint = ""
            if r.status_code in (401, 403):
                hint = (
                    "\nHINT: authentication failed when calling Stylia. Check that STYLIA_USERNAME/STYLIA_PASSWORD are correct.\n"
                    'If you store the password in a .env file and it contains spaces or special characters, quote it: STYLIA_PASSWORD="your password".\n'
                    "Also verify whether Stylia expects a token in headers rather than HTTP Basic auth, or whether the API requires your IP to be allowed."
                )
            raise RuntimeError(f"Stylia API returned {r.status_code}: {body}{hint}")

        data = r.json()
    except requests.exceptions.RequestException as e:
        logger.exception("Stylia API request failed: %s", e)
        raise RuntimeError(f"Failed to fetch Stylia data: {e}")

    if not data.get("success"):
        raise RuntimeError(f"Stylia API returned success=false: {data}")
    return data.get("result", [])


def generate_hash(product):
    # include per-item identifying fields (sku/ean/price/stock) to detect variant-level changes
    items = product.get("items", []) or []
    item_signatures = []
    for it in items:
        p = it.get("sellingPrice") or it.get("price") or 0
        sku = it.get("sku") or it.get("ean") or ""
        stk = it.get("stock") or 0
        item_signatures.append(f"{sku}:{p}:{stk}")

    key = {
        "brand": product.get("brand", ""),
        "modelCode": product.get("modelCode", ""),
        "stock": product.get("stock", 0),
        "items": sorted(item_signatures),
        "items_count": len(items),
    }
    return hashlib.md5(json.dumps(key, sort_keys=True).encode()).hexdigest()


def map_to_shopify(stylia_group):
    first_item = (stylia_group.get("items") or [{}])[0]
    title = f"{stylia_group['brand']} {stylia_group['modelCode']}"
    if first_item.get("colorName"):
        title += f" - {first_item['colorName']}"

    variants = []
    for item in stylia_group.get("items", []):
        # prefer item-level sellingPrice if present, then item.price, then group-level sellingPrice/price
        price_val = (
            item.get("sellingPrice")
            or item.get("price")
            or stylia_group.get("sellingPrice")
            or stylia_group.get("price")
            or 0
        )
        v = {
            "price": str(price_val),
            "compare_at_price": str(stylia_group.get("retailPrice", 0)),
            "inventory_quantity": item.get("stock", 0),
            "sku": item.get("sku", ""),
            "barcode": item.get("ean", ""),
            "inventory_management": "shopify",
            "inventory_policy": "deny",
            "fulfillment_service": "manual",
            "requires_shipping": True,
            "taxable": True,
            "weight": 0,
            "weight_unit": "kg",
        }
        if item.get("size"):
            v["option1"] = item["size"]
        if item.get("colorName"):
            v["option2"] = item["colorName"]
        variants.append(v)

    if not variants:
        variants = [
            {
                "price": str(
                    stylia_group.get("sellingPrice", stylia_group.get("price", 0))
                ),
                "compare_at_price": str(stylia_group.get("retailPrice", 0)),
                "inventory_quantity": stylia_group.get("stock", 0),
                "inventory_management": "shopify",
                "inventory_policy": "deny",
                "fulfillment_service": "manual",
                "requires_shipping": True,
                "taxable": True,
                "weight": 0,
                "weight_unit": "kg",
            }
        ]

    images = [{"src": u} for u in (first_item.get("images") or [])]

    tags = ", ".join(
        [
            t
            for t in [
                "stylia",
                f"model-{stylia_group['modelCode']}",
                (stylia_group.get("category") or "").lower().replace(" ", "-"),
            ]
            if t
        ]
    )

    return {
        "product": {
            "title": title,
            "body_html": stylia_group.get("description", {}).get("en")
            or f"{stylia_group['brand']} {stylia_group['modelCode']}",
            "vendor": stylia_group["brand"],
            "product_type": "Fashion",
            "tags": tags,
            "variants": variants,
            "images": images,
            "status": "active" if stylia_group.get("stock", 0) > 0 else "draft",
        }
    }


def shopify_headers():
    token = settings.SHOPIFY_ACCESS_TOKEN
    # prefer a saved token from the DB if present
    try:
        from .models import ShopifyApp

        shop_domain = settings.SHOPIFY_STORE_URL
        if shop_domain:
            app = ShopifyApp.objects.filter(shop_domain=shop_domain).first()
            if app and app.access_token:
                token = app.access_token
    except Exception:
        # don't fail hard if DB isn't available during migration/setup
        pass

    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}


def shopify_request(method: str, path: str, json_payload=None, params=None, retries=5):
    """Perform a Shopify API request with retries and basic rate-limit handling.

    path may be a full URL or a path under the store (starting with /admin)
    """
    headers = shopify_headers()
    store = settings.SHOPIFY_STORE_URL
    if path.startswith("http"):
        url = path
    else:
        url = f"https://{store}{path}"

    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.request(
                method,
                url,
                headers=headers,
                json=json_payload,
                params=params,
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            if attempt >= retries:
                raise
            backoff = (2**attempt) + random.uniform(0, 1)
            logger.warning(
                "Shopify request exception, retrying in %.1fs (%s)", backoff, e
            )
            time.sleep(backoff)
            continue

        # If Shopify indicates we're rate-limited, handle Retry-After or backoff
        if r.status_code in (429, 503):
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    wait = float(retry_after)
                except Exception:
                    wait = (2**attempt) + random.uniform(0, 1)
            else:
                wait = (2**attempt) + random.uniform(0, 1)
            if attempt >= retries:
                r.raise_for_status()
            logger.warning(
                "Shopify rate limit or service unavailable (%s). Waiting %.1fs before retrying...",
                r.status_code,
                wait,
            )
            time.sleep(wait)
            continue

        # Inspect call-limit header and throttle if we're near capacity
        call_limit = r.headers.get("X-Shopify-Shop-Api-Call-Limit")
        try:
            if call_limit:
                used, total = call_limit.split("/")
                used = int(used)
                total = int(total)
                if total and used / total > 0.8:
                    sleep_for = max(0.5, (used / total) * 2)
                    logger.info(
                        "Shopify call limit high (%s). Sleeping %.2fs to avoid hitting cap.",
                        call_limit,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
        except Exception:
            pass

        return r


def shopify_get_product(product_id):
    path = f"/admin/api/2025-07/products/{product_id}.json"
    return shopify_request("GET", path)


def shopify_create(product_payload):
    path = "/admin/api/2025-07/products.json"
    r = shopify_request("POST", path, json_payload=product_payload)
    if r.status_code not in (200, 201):
        hint = ""
        try:
            text = r.text or ""
            if r.status_code == 403 and (
                "write_products" in text or "requires merchant approval" in text
            ):
                hint = (
                    "\nHINT: Shopify returned 403 - the API token lacks required Admin API scopes.\n"
                    "Ensure your app access token includes 'write_products' (and 'write_inventory' if you need inventory updates),\n"
                    "then reinstall or regenerate the token and set SHOPIFY_ACCESS_TOKEN accordingly."
                )
        except Exception:
            hint = ""
        raise RuntimeError(f"Shopify create failed: {r.status_code} - {r.text}{hint}")
    return r.json()


def shopify_update(product_id, product_payload):
    path = f"/admin/api/2025-07/products/{product_id}.json"
    r = shopify_request("PUT", path, json_payload=product_payload)
    if r.status_code != 200:
        hint = ""
        try:
            text = r.text or ""
            if r.status_code == 403 and (
                "write_products" in text or "requires merchant approval" in text
            ):
                hint = (
                    "\nHINT: Shopify returned 403 - the API token lacks required Admin API scopes.\n"
                    "Ensure your app access token includes 'write_products' (and 'write_inventory' if you need inventory updates),\n"
                    "then reinstall or regenerate the token and set SHOPIFY_ACCESS_TOKEN accordingly."
                )
        except Exception:
            hint = ""
        raise RuntimeError(f"Shopify update failed: {r.status_code} - {r.text}{hint}")
    return r.json()


def shopify_get_locations():
    path = "/admin/api/2025-07/locations.json"
    r = shopify_request("GET", path)
    r.raise_for_status()
    return r.json().get("locations", [])


def resolve_location_id():
    # Prefer explicit ID if provided
    if getattr(settings, "SHOPIFY_LOCATION_ID", ""):
        return settings.SHOPIFY_LOCATION_ID
    # Otherwise try to resolve by name
    name = getattr(settings, "SHOPIFY_LOCATION_NAME", "Stylia Warehouse")
    try:
        for loc in shopify_get_locations():
            if loc.get("name") == name:
                return str(loc.get("id"))
    except Exception as e:
        logger.warning("Failed to resolve Shopify location by name: %s", e)
    return ""


def inventory_connect(inventory_item_id: int, location_id: str):
    path = "/admin/api/2025-07/inventory_levels/connect.json"
    payload = {
        "inventory_item_id": int(inventory_item_id),
        "location_id": int(location_id),
    }
    r = shopify_request("POST", path, json_payload=payload)
    # 200 OK on success, 422 if already connected (we treat as ok)
    if r.status_code not in (200, 201, 202, 204):
        # many 422s are benign; log and continue
        logger.info("inventory_connect returned %s: %s", r.status_code, r.text[:500])
    return r


def inventory_set(inventory_item_id: int, location_id: str, available: int):
    path = "/admin/api/2025-07/inventory_levels/set.json"
    payload = {
        "inventory_item_id": int(inventory_item_id),
        "location_id": int(location_id),
        "available": int(max(0, available or 0)),
    }
    r = shopify_request("POST", path, json_payload=payload)
    if r.status_code not in (200, 201):
        logger.warning("inventory_set failed %s: %s", r.status_code, r.text[:500])
    return r


def reconcile_shopify_state():
    from .models import StyliaProduct

    checked = deleted_marked = errors = 0
    qs = StyliaProduct.objects.exclude(shopify_id__isnull=True).exclude(shopify_id="")
    for p in qs.iterator():
        checked += 1
        try:
            r = shopify_get_product(p.shopify_id)
            if r.status_code == 404:
                p.mark_as_deleted()
                deleted_marked += 1
            elif r.status_code not in (200, 201):
                errors += 1
        except Exception:
            errors += 1

    return {
        "success": True,
        "summary": {
            "checked": checked,
            "deleted_marked": deleted_marked,
            "errors": errors,
        },
    }


def ingest_stylia():
    from .models import StyliaProduct

    data = get_stylia_sync_list()
    with_stock = [p for p in data if p.get("stock", 0) > 0]

    # group by modelCode
    groups = {}
    for p in with_stock:
        groups.setdefault(p["modelCode"], []).append(p)

    created = updated = skipped = 0
    for model_code, plist in groups.items():
        primary = plist[0]
        # combine items
        items = []
        total_stock = 0
        for g in plist:
            items.extend(g.get("items", []))
            total_stock += g.get("stock", 0)
        combined = primary.copy()
        combined["items"] = items
        combined["stock"] = total_stock

        data_hash = generate_hash(combined)
        mapped = map_to_shopify(combined)
        variants = mapped["product"]["variants"]
        price = float(variants[0].get("price", 0)) if variants else 0

        try:
            db = StyliaProduct.objects.get(model_code=model_code)
            db.is_active_in_stylia = True
            if db.sync_status == "deleted":
                skipped += 1
                db.save()
                continue
            if db.data_hash != data_hash:
                db.title = mapped["product"]["title"]
                db.price = price
                db.total_stock = sum(v.get("inventory_quantity", 0) for v in variants)
                db.variant_count = len(variants)
                db.product_type = mapped["product"].get("product_type", "Fashion")
                db.data_hash = data_hash
                db.shopify_data = mapped
                db.sync_status = "pending"
                db.save()
                updated += 1
            else:
                db.save()
                skipped += 1
        except StyliaProduct.DoesNotExist:
            StyliaProduct.objects.create(
                model_code=model_code,
                brand=combined["brand"],
                title=mapped["product"]["title"],
                price=price,
                total_stock=sum(v.get("inventory_quantity", 0) for v in variants),
                variant_count=len(variants),
                product_type=mapped["product"].get("product_type", "Fashion"),
                data_hash=data_hash,
                shopify_data=mapped,
                is_active_in_stylia=True,
                sync_status="pending",
            )
            created += 1

    return {
        "success": True,
        "summary": {
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "unique": len(groups),
            "with_stock": len(with_stock),
        },
    }


def push_to_shopify():
    from .models import StyliaProduct

    qs = StyliaProduct.objects.filter(sync_status="pending")
    created = updated = errors = 0

    location_id_cache = resolve_location_id()
    for p in qs.iterator():
        try:
            if not p.shopify_id:
                created_payload = shopify_create(p.shopify_data)
                p.shopify_id = created_payload["product"]["id"]
                p.shopify_handle = created_payload["product"]["handle"]
                # After create, try to set inventory at location
                _assign_inventory_to_location(
                    created_payload["product"], location_id_cache, p
                )
                p.mark_as_synced()
                created += 1
            else:
                updated_payload = shopify_update(p.shopify_id, p.shopify_data)
                _assign_inventory_to_location(
                    updated_payload.get("product"), location_id_cache, p
                )
                p.mark_as_synced()
                updated += 1
        except Exception as exc:
            # capture full traceback and a short message to persist on the model
            tb = traceback.format_exc()
            logger.exception(
                "Failed pushing product %s to Shopify: %s", p.model_code, tb
            )
            # persist a truncated message to avoid extremely large DB fields
            short = tb[:2000]
            try:
                p.mark_as_failed(message=short)
            except Exception:
                # ensure we don't raise while handling an error; log and continue
                logger.exception("Failed to mark product %s as failed", p.model_code)
            errors += 1

    return {
        "success": True,
        "summary": {"created": created, "updated": updated, "errors": errors},
    }


def _assign_inventory_to_location(product_json, location_id, db_obj):
    """Best-effort inventory assignment to a location per variant.
    Does not raise; logs and updates model flags.
    """
    from .models import StyliaProduct

    if not product_json:
        return
    if not location_id:
        logger.info(
            "No Shopify location configured; skipping inventory levels assignment"
        )
        return
    try:
        variants = product_json.get("variants", [])
        # If no variants, nothing to set
        for v in variants:
            inv_item_id = v.get("inventory_item_id")
            qty = v.get("inventory_quantity", 0)
            # ensure inventory is Shopify-managed
            # v["inventory_management"] is set in payload; Shopify should reflect it
            inventory_connect(inv_item_id, location_id)
            inventory_set(inv_item_id, location_id, qty)
        db_obj.location_assigned = True
        db_obj.location_last_error = ""
        db_obj.save(update_fields=["location_assigned", "location_last_error"])
    except Exception as e:
        logger.exception(
            "Failed assigning inventory to location for product %s: %s",
            db_obj.model_code,
            e,
        )
        db_obj.location_assigned = False
        db_obj.location_last_error = str(e)[:1000]
        db_obj.save(update_fields=["location_assigned", "location_last_error"])
