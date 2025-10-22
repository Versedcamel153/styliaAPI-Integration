import json
import hashlib
import requests
from django.conf import settings
import logging
import traceback
import random
import time

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


def shopify_request(method, url, max_retries=5, backoff_base=0.5, **kwargs):
    """Make a resilient request to Shopify with retries and backoff.

    Respects Retry-After header and X-Shopify-Shop-Api-Call-Limit to avoid bursting.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.request(
                method,
                url,
                headers=shopify_headers(),
                timeout=kwargs.pop("timeout", 30),
                **kwargs,
            )
        except requests.exceptions.RequestException as e:
            if attempt >= max_retries:
                logger.exception(
                    "Request to Shopify failed after %s attempts: %s %s",
                    attempt,
                    method,
                    url,
                )
                raise
            sleep_for = backoff_base * (2 ** (attempt - 1)) + random.random() * 0.1
            logger.info(
                "Request exception, retrying in %.2fs (%s/%s)",
                sleep_for,
                attempt,
                max_retries,
            )
            time.sleep(sleep_for)
            continue

        # If successful, possibly throttle based on call limit header
        call_limit = r.headers.get("X-Shopify-Shop-Api-Call-Limit")
        if call_limit:
            try:
                used, total = call_limit.split("/")
                used = int(used)
                total = int(total)
                # if we're above 80% of the bucket, pause briefly
                if total and used / total >= 0.8:
                    sleep_seconds = max(0.5, (used / total) * 0.5)
                    logger.info(
                        "Approaching Shopify call limit (%s). Sleeping %.2fs to avoid bursts.",
                        call_limit,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)
            except Exception:
                pass

        # Handle 429 and 5xx with backoff and Retry-After
        if r.status_code == 429 or 500 <= r.status_code < 600:
            if attempt >= max_retries:
                logger.error(
                    "Shopify request failed with %s after %s attempts: %s %s",
                    r.status_code,
                    attempt,
                    method,
                    url,
                )
                r.raise_for_status()
            # honor Retry-After header if present
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_for = float(retry_after)
                except Exception:
                    sleep_for = backoff_base * (2 ** (attempt - 1))
            else:
                sleep_for = backoff_base * (2 ** (attempt - 1)) + random.random() * 0.5
            logger.info(
                "Shopify returned %s. Backing off %.2fs (attempt %s/%s)",
                r.status_code,
                sleep_for,
                attempt,
                max_retries,
            )
            time.sleep(sleep_for)
            continue

        return r


def shopify_get_product(product_id):
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/products/{product_id}.json"
    return shopify_request("GET", url, timeout=20)


def shopify_graphql_request(query, variables=None, timeout=30):
    """Simple GraphQL request helper for Shopify Admin API."""
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/graphql.json"
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    r = shopify_request("POST", url, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def shopify_find_product_by_sku(sku):
    """Try GraphQL product search for a SKU first, fall back to REST products.json if necessary.
    Returns product dict (GraphQL shape converted) or None.
    """
    if not sku:
        return None
    # GraphQL search by variant SKU
    try:
        query = """
        query productBySKU($query: String!) {
          products(first: 1, query: $query) {
            edges { node { id handle variants(first: 10) { edges { node { id sku inventoryItem { id } } } } } }
          }
        }
        """
        # Shopify product search query syntax: "variants:sku:SKUVALUE"
        gql_query = f"variants:sku:{sku}"
        result = shopify_graphql_request(
            query, variables={"query": gql_query}, timeout=20
        )
        # traverse response
        prod_edges = result.get("data", {}).get("products", {}).get("edges", [])
        if prod_edges:
            node = prod_edges[0].get("node")
            # convert node to a light REST-like dict for compatibility
            product = {
                "id": node.get("id").split("/")[-1] if node.get("id") else None,
                "handle": node.get("handle"),
                "variants": [],
            }
            for ve in node.get("variants", {}).get("edges", []):
                v = ve.get("node")
                inv = v.get("inventoryItem") or {}
                product["variants"].append(
                    {
                        "id": v.get("id").split("/")[-1] if v.get("id") else None,
                        "sku": v.get("sku"),
                        "inventory_item_id": (
                            inv.get("id").split("/")[-1] if inv.get("id") else None
                        ),
                    }
                )
            return product
    except Exception as e:
        logger.info("GraphQL SKU search failed, falling back to REST: %s", e)

    # Fallback to REST products.json endpoint (existing behavior)
    try:
        url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/products.json?limit=250&fields=id,handle,variants&sku={sku}"
        r = shopify_request("GET", url, timeout=20)
        if r.status_code == 200:
            data = r.json()
            products = data.get("products") or []
            if products:
                return products[0]
    except Exception:
        logger.exception("REST SKU search failed for %s", sku)
    return None


def shopify_find_product_by_skus(skus):
    """Try to find a product for any SKU in the iterable `skus`. Returns (product, sku) or (None, None)."""
    # First try DB mapped variants for a fast resolution
    try:
        from .models import Variant

        skus_list = [s for s in (skus or []) if s]
        if skus_list:
            v = (
                Variant.objects.filter(sku__in=skus_list)
                .exclude(shopify_product_id__isnull=True)
                .exclude(shopify_product_id="")
                .first()
            )
            if v:
                # Return a light product dict built from DB mapping
                product = {
                    "id": v.shopify_product_id,
                    "handle": v.stylia_product.shopify_handle or "",
                    "variants": [
                        {
                            "id": v.shopify_variant_id,
                            "sku": v.sku,
                            "inventory_item_id": v.inventory_item_id,
                        }
                    ],
                }
                return product, v.sku
    except Exception:
        logger.exception("Error checking Variant DB for SKUs")

    # Fall back to live lookups (GraphQL then REST)
    for s in skus or []:
        if not s:
            continue
        found = shopify_find_product_by_sku(s)
        if found:
            return found, s
    return None, None


def shopify_create(product_payload):
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/products.json"
    r = shopify_request("POST", url, json=product_payload, timeout=30)
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
    data = r.json()
    # persist variant/shopify ids if possible
    try:
        from .models import Variant, StyliaProduct

        prod = data.get("product") or {}
        shopify_product_id = prod.get("id")
        # Try to find the corresponding StyliaProduct via a tag or handle not always possible
        # We leave the caller to set StyliaProduct.shopify_id; here we persist variants if we can
        # Map by SKU
        variants = prod.get("variants") or []
        for v in variants:
            sku = v.get("sku")
            if not sku:
                continue
            # find whichever StyliaProduct has this SKU in its shopify_data
            sp = StyliaProduct.objects.filter(
                shopify_data__product__variants__contains=[{"sku": sku}]
            ).first()
            if not sp:
                # fallback: try by product handle
                sp = StyliaProduct.objects.filter(
                    shopify_handle=prod.get("handle")
                ).first()
            if sp:
                Variant.objects.update_or_create(
                    stylia_product=sp,
                    sku=sku,
                    defaults={
                        "shopify_variant_id": v.get("id"),
                        "inventory_item_id": v.get("inventory_item_id"),
                        "shopify_product_id": shopify_product_id,
                        "price": v.get("price") or None,
                    },
                )
    except Exception:
        logger.exception("Failed to persist variant mappings after create")
    return data


def shopify_update(product_id, product_payload):
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/products/{product_id}.json"
    r = shopify_request("PUT", url, json=product_payload, timeout=30)
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
    data = r.json()
    try:
        from .models import Variant, StyliaProduct

        prod = data.get("product") or {}
        shopify_product_id = prod.get("id")
        variants = prod.get("variants") or []
        for v in variants:
            sku = v.get("sku")
            if not sku:
                continue
            sp = StyliaProduct.objects.filter(
                shopify_id=str(shopify_product_id)
            ).first()
            if not sp:
                sp = StyliaProduct.objects.filter(
                    shopify_handle=prod.get("handle")
                ).first()
            if sp:
                Variant.objects.update_or_create(
                    stylia_product=sp,
                    sku=sku,
                    defaults={
                        "shopify_variant_id": v.get("id"),
                        "inventory_item_id": v.get("inventory_item_id"),
                        "shopify_product_id": shopify_product_id,
                        "price": v.get("price") or None,
                    },
                )
    except Exception:
        logger.exception("Failed to persist variant mappings after update")
    return data


def shopify_get_locations():
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/locations.json"
    r = shopify_request("GET", url, timeout=20)
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
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/inventory_levels/connect.json"
    payload = {
        "inventory_item_id": int(inventory_item_id),
        "location_id": int(location_id),
    }
    r = shopify_request("POST", url, json=payload, timeout=20)
    # 200 OK on success, 422 if already connected (we treat as ok)
    if r.status_code not in (200, 201, 202, 204):
        logger.info("inventory_connect returned %s: %s", r.status_code, r.text[:500])
    return r


def inventory_set(inventory_item_id: int, location_id: str, available: int):
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/inventory_levels/set.json"
    payload = {
        "inventory_item_id": int(inventory_item_id),
        "location_id": int(location_id),
        "available": int(max(0, available or 0)),
    }
    r = shopify_request("POST", url, json=payload, timeout=20)
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

    batch_size = getattr(settings, "SHOPIFY_BATCH_SIZE", 20)
    qs = StyliaProduct.objects.filter(sync_status="pending")
    created = updated = errors = 0
    location_id_cache = resolve_location_id()
    # pacing controls
    per_call_sleep = getattr(settings, "SHOPIFY_MIN_SLEEP", 0.8)
    error_backoff = 0
    max_backoff = getattr(settings, "SHOPIFY_MAX_BACKOFF", 10)

    # process oldest pending products first (FIFO)
    qs = (
        StyliaProduct.objects.filter(sync_status="pending")
        .order_by("updated_at")
        .all()[:batch_size]
    )
    for p in qs.iterator():
        try:
            if not p.shopify_id:
                try:
                    created_payload = shopify_create(p.shopify_data)
                except RuntimeError as e:
                    text = str(e)
                    # Handle duplicate variant / SKU errors by finding existing product and updating
                    if "variant" in text.lower() and "already exists" in text.lower():
                        # attempt to find by any SKU from our shopify_data (all variants)
                        skus = [
                            v.get("sku")
                            for v in p.shopify_data.get("product", {}).get("variants")
                            or []
                        ]
                        found, found_sku = shopify_find_product_by_skus(skus)
                        if found:
                            logger.info(
                                "Found existing product for SKU %s (id=%s) — switching to update.",
                                found_sku,
                                found.get("id"),
                            )
                            # persist mapping to DB
                            try:
                                p.shopify_id = found.get("id")
                                p.shopify_handle = (
                                    found.get("handle") or p.shopify_handle
                                )
                                p.save(update_fields=["shopify_id", "shopify_handle"])
                                # persist variant mappings from found product
                                from .models import Variant

                                for v in found.get("variants") or []:
                                    sku_val = v.get("sku")
                                    if not sku_val:
                                        continue
                                    Variant.objects.update_or_create(
                                        stylia_product=p,
                                        sku=sku_val,
                                        defaults={
                                            "shopify_variant_id": v.get("id"),
                                            "inventory_item_id": v.get(
                                                "inventory_item_id"
                                            ),
                                            "shopify_product_id": found.get("id"),
                                        },
                                    )
                            except Exception:
                                logger.exception(
                                    "Failed to persist variant mapping when switching to update"
                                )

                            # Now try update flow using the found product id
                            updated_payload = shopify_update(
                                p.shopify_id, p.shopify_data
                            )
                            _assign_inventory_to_location(
                                updated_payload.get("product"), location_id_cache, p
                            )
                            p.mark_as_synced()
                            updated += 1
                            continue
                    # if not handled by fallback, re-raise
                    raise
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
            # on success, gently reduce backoff and sleep to avoid bursts
            error_backoff = max(0, error_backoff - 0.3)
            sleep_for = per_call_sleep + error_backoff
            if sleep_for:
                time.sleep(sleep_for)
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
                logger.exception("Failed to mark product %s as failed", p.model_code)
            errors += 1
            # increase backoff on errors so subsequent calls are slowed
            error_backoff = min(max_backoff, error_backoff + 1.0)

    return {
        "success": True,
        "summary": {"created": created, "updated": updated, "errors": errors},
    }


def push_single_product(product_id):
    """Push a single product by id (create or update) and return result dict.
    This is intended to be called from a task to avoid scheduling a full sync.
    """
    from .models import StyliaProduct

    try:
        p = StyliaProduct.objects.get(pk=product_id)
    except StyliaProduct.DoesNotExist:
        return {"success": False, "error": "not_found"}

    location_id_cache = resolve_location_id()
    created = updated = errors = 0
    try:
        if not p.shopify_id:
            try:
                created_payload = shopify_create(p.shopify_data)
                p.shopify_id = created_payload["product"]["id"]
                p.shopify_handle = created_payload["product"].get("handle")
                _assign_inventory_to_location(
                    created_payload["product"], location_id_cache, p
                )
                p.mark_as_synced()
                created = 1
            except RuntimeError as e:
                text = str(e)
                if "variant" in text.lower() and "already exists" in text.lower():
                    skus = [
                        v.get("sku")
                        for v in p.shopify_data.get("product", {}).get("variants") or []
                    ]
                    found, found_sku = shopify_find_product_by_skus(skus)
                    if found:
                        # Found existing product, switch to update
                        try:
                            p.shopify_id = found.get("id")
                            p.shopify_handle = found.get("handle") or p.shopify_handle
                            p.save(update_fields=["shopify_id", "shopify_handle"])
                            from .models import Variant

                            for v in found.get("variants") or []:
                                sku_val = v.get("sku")
                                if not sku_val:
                                    continue
                                Variant.objects.update_or_create(
                                    stylia_product=p,
                                    sku=sku_val,
                                    defaults={
                                        "shopify_variant_id": v.get("id"),
                                        "inventory_item_id": v.get("inventory_item_id"),
                                        "shopify_product_id": found.get("id"),
                                    },
                                )
                        except Exception:
                            logger.exception(
                                "Failed to persist variant mapping when switching to update (single push)"
                            )

                        updated_payload = shopify_update(p.shopify_id, p.shopify_data)
                        _assign_inventory_to_location(
                            updated_payload["product"], location_id_cache, p
                        )
                        p.mark_as_synced()
                        updated = 1
                    else:
                        raise
                else:
                    raise
        else:
            updated_payload = shopify_update(p.shopify_id, p.shopify_data)
            _assign_inventory_to_location(
                updated_payload.get("product"), location_id_cache, p
            )
            p.mark_as_synced()
            updated = 1
    except Exception:
        tb = traceback.format_exc()
        logger.exception("Failed pushing single product %s: %s", p.model_code, tb)
        try:
            p.mark_as_failed(message=tb[:2000])
        except Exception:
            logger.exception("Failed to mark single product %s as failed", p.model_code)
        errors = 1

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
