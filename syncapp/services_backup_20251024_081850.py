import json
import hashlib
import requests
from django.conf import settings
import logging
import traceback
import random
import time

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger(__name__)

_redis_client = None


def _get_redis_client():
    global _redis_client
    url = getattr(settings, "SHOPIFY_RATE_LIMIT_REDIS_URL", "")
    if not url or not redis:
        return None
    if _redis_client is None:
        try:
            _redis_client = redis.from_url(url)
        except Exception:
            _redis_client = None
    return _redis_client


def _acquire_shopify_rate_token():
    """Simple cross-process rate limit: allow N tokens per second with capacity C using Redis.

    If Redis or config isn't available, no-op (fall back to per-call sleep and backoff).
    """
    client = _get_redis_client()
    if not client:
        return
    key = "shopify:rate:bucket"
    now_ms = int(time.time() * 1000)
    window_ms = 1000
    tokens_per_sec = float(getattr(settings, "SHOPIFY_RATE_LIMIT_TOKENS_PER_SEC", 2.0))
    capacity = int(getattr(settings, "SHOPIFY_RATE_LIMIT_CAPACITY", 4))

    # Use a simple counter with 1s TTL. If we exceed capacity, sleep until next second.
    try:
        pipe = client.pipeline(True)
        pipe.incr(key)
        pipe.expire(key, 1)
        count, _ = pipe.execute()
        if int(count) > capacity:
            # sleep until end of window; cap at small delay
            time_to_sleep = max(0.0, (window_ms - (now_ms % window_ms)) / 1000.0)
            time.sleep(min(time_to_sleep, 1.0))
    except Exception:
        # ignore limiter failures
        pass


def _build_shopify_urls(product_id: str | int | None, handle: str | None):
    """Return dict with admin and storefront URLs for a Shopify product."""
    domain = getattr(settings, "SHOPIFY_STORE_URL", "").strip()
    admin_url = None
    public_url = None
    if domain and product_id:
        admin_url = f"https://{domain}/admin/products/{product_id}"
    if domain and handle:
        public_url = f"https://{domain}/products/{handle}"
    return {"admin_url": admin_url, "storefront_url": public_url}


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
    seen_skus = set()
    for idx, item in enumerate(stylia_group.get("items", [])):
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
        # normalize SKU: strip, uppercase and fallback to modelCode-index when missing
        sku_raw = (v.get("sku") or v.get("barcode") or "").strip()
        sku = sku_raw.upper() if sku_raw else ""
        if not sku:
            sku = f"{stylia_group.get('modelCode')}-{idx}"
        # ensure uniqueness within this payload
        if sku in seen_skus:
            suffix = 1
            new_sku = f"{sku}-{suffix}"
            while new_sku in seen_skus:
                suffix += 1
                new_sku = f"{sku}-{suffix}"
            sku = new_sku
        seen_skus.add(sku)
        v["sku"] = sku
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
            # Cross-process rate limit token (no-op if not configured)
            _acquire_shopify_rate_token()
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


def _normalize_shopify_payload(payload):
    """Ensure variant SKUs are unique and payload is deterministic before sending to Shopify.

    This prevents accidental 'variant already exists' errors caused by duplicate SKUs
    in the payload or empty SKUs. We only adjust SKUs when necessary and log a warning.
    """
    prod = payload.get("product") or {}
    variants = prod.get("variants") or []
    seen = set()
    changed = False
    for i, v in enumerate(variants):
        sku = (v.get("sku") or "").strip()
        if not sku:
            # generate a fallback SKU to avoid empty SKUs
            sku = f"NO-SKU-{i}"
            v["sku"] = sku
            changed = True
        if sku in seen:
            # make SKU unique by appending an index suffix
            suffix = 1
            new_sku = f"{sku}-{suffix}"
            while new_sku in seen:
                suffix += 1
                new_sku = f"{sku}-{suffix}"
            v["sku"] = new_sku
            changed = True
            seen.add(new_sku)
        else:
            seen.add(sku)
    if changed:
        logger.info("Normalized shopify payload variants to ensure unique SKUs")
    # sort variants by sku for determinism
    prod["variants"] = sorted(
        prod.get("variants", []), key=lambda x: x.get("sku") or ""
    )
    payload["product"] = prod
    return payload


def _option_tuple(v):
    o1 = (v.get("option1") or "").strip().lower()
    o2 = (v.get("option2") or "").strip().lower()
    o3 = (v.get("option3") or "").strip().lower()
    return (o1, o2, o3)


def _dedupe_variants_by_options(product_dict):
    """Remove duplicate variants by identical option tuples in the payload.
    Keeps the first occurrence (preferring the one with an 'id')."""
    variants = product_dict.get("variants") or []
    seen = {}
    result = []
    for v in variants:
        key = _option_tuple(v)
        if key not in seen:
            seen[key] = v
            result.append(v)
        else:
            # if the existing one has no id but the new one has, replace
            existing = seen[key]
            if not existing.get("id") and v.get("id"):
                idx = result.index(existing)
                result[idx] = v
                seen[key] = v
            # else drop duplicate
    if len(result) != len(variants):
        logger.info(
            "Removed %d duplicate option variants in payload",
            len(variants) - len(result),
        )
    product_dict["variants"] = result
    return product_dict


def _enrich_variant_ids(db_sp, product_dict):
    """Attach variant 'id' to payload variants using DB Variant mapping or live Shopify lookup."""
    try:
        from .models import Variant
    except Exception:
        return product_dict

    # Map by SKU from DB
    sku_map = {}
    try:
        for var in Variant.objects.filter(stylia_product=db_sp):
            if var.sku and var.shopify_variant_id:
                sku_map[var.sku] = str(var.shopify_variant_id)
    except Exception:
        logger.exception("Error fetching Variant mappings for %s", db_sp.model_code)

    for v in product_dict.get("variants", []) or []:
        if not v.get("id") and v.get("sku") in sku_map:
            v["id"] = sku_map[v.get("sku")]

    # If still missing and product exists in Shopify, fetch and match by sku or options
    if (
        any(not v.get("id") for v in product_dict.get("variants", []) or [])
        and db_sp.shopify_id
    ):
        try:
            r = shopify_get_product(db_sp.shopify_id)
            if r.status_code == 200:
                prod = r.json().get("product") or {}
                live = prod.get("variants") or []
                by_sku = {lv.get("sku"): lv for lv in live if lv.get("sku")}
                by_opts = {_option_tuple(lv): lv for lv in live}
                for v in product_dict.get("variants", []) or []:
                    if not v.get("id"):
                        lv = by_sku.get(v.get("sku")) or by_opts.get(_option_tuple(v))
                        if lv and lv.get("id"):
                            v["id"] = lv.get("id")
                            # persist mapping to DB so next time we don't fetch live for this SKU
                            try:
                                Variant.objects.update_or_create(
                                    stylia_product=db_sp,
                                    sku=v.get("sku") or "",
                                    defaults={
                                        "shopify_variant_id": lv.get("id"),
                                        "inventory_item_id": lv.get(
                                            "inventory_item_id"
                                        ),
                                        "shopify_product_id": prod.get("id"),
                                        "price": lv.get("price"),
                                    },
                                )
                            except Exception:
                                logger.exception(
                                    "Failed to persist Variant mapping during enrichment for %s",
                                    db_sp.model_code,
                                )
        except Exception:
            logger.exception(
                "Failed to enrich variant ids from Shopify for %s", db_sp.model_code
            )
    return product_dict


def _acquire_product_lock(model_code: str, ttl_seconds: int = 60):
    client = _get_redis_client()
    if not client:
        return True
    try:
        return bool(
            client.set(f"shopify:pushlock:{model_code}", "1", nx=True, ex=ttl_seconds)
        )
    except Exception:
        return True


def _release_product_lock(model_code: str):
    client = _get_redis_client()
    if not client:
        return
    try:
        client.delete(f"shopify:pushlock:{model_code}")
    except Exception:
        pass


def _sanitize_update_payload(payload: dict) -> dict:
    """Sanitize REST update payload: coerce variant.id to int and remove read-only fields."""
    try:
        prod = payload.get("product") or {}
        vs = prod.get("variants") or []
        cleaned = []
        for v in vs:
            v2 = dict(v)
            # read-only on REST update
            v2.pop("inventory_item_id", None)
            vid = v2.get("id")
            if isinstance(vid, str) and vid.isdigit():
                try:
                    v2["id"] = int(vid)
                except Exception:
                    pass
            cleaned.append(v2)
        prod["variants"] = cleaned
        payload["product"] = prod
        return payload
    except Exception:
        return payload


def _strip_mismatched_variant_options_for_update(
    product_id: str | int, payload: dict
) -> dict:
    """Fetch Shopify product to detect supported options and strip option1/2/3
    from variant updates when they don't match the product's option count.

    This prevents 422 errors like "Option values provided for X unknown option(s)" when
    we're only updating price/sku/inventory and not changing options.
    """
    try:
        r = shopify_get_product(product_id)
        if r.status_code != 200:
            return payload
        prod = (r.json() or {}).get("product") or {}
        options = prod.get("options") or []
        option_count = len(options)
        p = payload.get("product") or {}
        vs = p.get("variants") or []
        for v in vs:
            # Remove option fields beyond what Shopify product actually supports
            if option_count < 3:
                v.pop("option3", None)
            if option_count < 2:
                v.pop("option2", None)
            if option_count < 1:
                v.pop("option1", None)
        p["variants"] = vs
        payload["product"] = p
        return payload
    except Exception:
        # On any failure, return original payload unchanged
        return payload


def _merge_into_existing_product(
    existing_sp, source_sp, found_product_json, location_id_cache=None
):
    """Merge source_sp shopify_data into existing_sp and update Shopify.

    - Adds any variants from source_sp that are not present in existing_sp (by SKU).
    - Updates counts and shopify_data on existing_sp.
    - Calls shopify_update for the combined product and assigns inventory.
    - Persists Variant mappings from the updated Shopify response.
    - Deletes the source_sp record after merge.
    """
    try:
        existing_data = existing_sp.shopify_data or {"product": {"variants": []}}
        source_data = source_sp.shopify_data or {"product": {"variants": []}}
        existing_variants = existing_data["product"].get("variants") or []
        existing_skus = {v.get("sku") for v in existing_variants if v.get("sku")}

        # Add missing variants from source
        added = 0
        for v in source_data["product"].get("variants") or []:
            sku = v.get("sku")
            if not sku or sku in existing_skus:
                continue
            existing_variants.append(v)
            existing_skus.add(sku)
            added += 1

        # Update metadata
        existing_data["product"]["variants"] = existing_variants
        existing_sp.shopify_data = existing_data
        existing_sp.variant_count = len(existing_variants)
        existing_sp.total_stock = sum(
            int(v.get("inventory_quantity", 0)) for v in existing_variants
        )
        existing_sp.save()

        # Enrich ids, dedupe options, then normalize just before update
        enriched_product = _enrich_variant_ids(existing_sp, existing_data["product"])
        deduped_product = _dedupe_variants_by_options(enriched_product)
        payload = _normalize_shopify_payload({"product": deduped_product})
        # Strip mismatched option fields before update to avoid 422 unknown option(s)
        payload = _strip_mismatched_variant_options_for_update(
            existing_sp.shopify_id, payload
        )
        updated_payload = shopify_update(
            existing_sp.shopify_id, _sanitize_update_payload(payload)
        )
        # assign inventory for updated product
        _assign_inventory_to_location(
            updated_payload.get("product"), location_id_cache, existing_sp
        )

        # persist variant mappings
        try:
            from .models import Variant

            prod = updated_payload.get("product") or {}
            for v in prod.get("variants") or []:
                sku_val = v.get("sku")
                if not sku_val:
                    continue
                Variant.objects.update_or_create(
                    stylia_product=existing_sp,
                    sku=sku_val,
                    defaults={
                        "shopify_variant_id": v.get("id"),
                        "inventory_item_id": v.get("inventory_item_id"),
                        "shopify_product_id": prod.get("id"),
                        "price": v.get("price"),
                    },
                )
        except Exception:
            logger.exception(
                "Failed to persist variant mappings during merge for %s",
                existing_sp.model_code,
            )

        # delete the source record now that we've merged it
        try:
            source_sp.delete()
        except Exception:
            logger.exception(
                "Failed to delete source StyliaProduct after merge: %s",
                source_sp.model_code,
            )

        return True
    except Exception:
        logger.exception(
            "Failed merging product %s into %s",
            source_sp.model_code,
            existing_sp.model_code,
        )
        return False


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


def shopify_find_product_by_model(
    model_code: str, brand: str, title: str, handle: str | None = None
):
    """Search Shopify products by model tag, vendor, title, and optional handle using GraphQL.

    Returns a REST-like product dict or None.
    """
    try:
        parts = []
        if model_code:
            parts.append(f"tag:model-{model_code}")
        if brand:
            parts.append(f"vendor:{brand}")
        if title:
            # quote the title for exact match bias
            parts.append(f"title:'{title}'")
        if handle:
            parts.append(f"handle:{handle}")
        if not parts:
            return None
        q = " ".join(parts)
        query = """
        query productByMeta($query: String!) {
          products(first: 1, query: $query) {
            edges { node { id handle variants(first: 50) { edges { node { id sku inventoryItem { id } price } } } } }
          }
        }
        """
        result = shopify_graphql_request(query, variables={"query": q}, timeout=20)
        prod_edges = result.get("data", {}).get("products", {}).get("edges", [])
        if not prod_edges:
            return None
        node = prod_edges[0].get("node") or {}
        product = {
            "id": node.get("id").split("/")[-1] if node.get("id") else None,
            "handle": node.get("handle"),
            "variants": [],
        }
        for ve in node.get("variants", {}).get("edges", []):
            v = ve.get("node") or {}
            inv = v.get("inventoryItem") or {}
            product["variants"].append(
                {
                    "id": v.get("id").split("/")[-1] if v.get("id") else None,
                    "sku": v.get("sku"),
                    "inventory_item_id": (
                        inv.get("id").split("/")[-1] if inv.get("id") else None
                    ),
                    "price": v.get("price"),
                }
            )
        return product
    except Exception as e:
        logger.info("Model search failed: %s", e)
        return None


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


def _parse_link_next(link_header: str | None) -> str | None:
    if not link_header:
        return None
    # Shopify style: <https://host/...&page_info=XYZ>; rel="next"
    try:
        parts = [p.strip() for p in link_header.split(",")]
        for p in parts:
            if 'rel="next"' in p:
                start = p.find("<")
                end = p.find(">", start + 1)
                if start != -1 and end != -1:
                    return p[start + 1 : end]
    except Exception:
        return None
    return None


def shopify_list_inventory_item_ids_for_location(
    location_id: str, max_pages: int | None = None
):
    """Iterate inventory_levels for a location and collect inventory_item_ids.

    Returns a set of inventory_item_id strings. Respects Shopify pagination via Link header.
    """
    collected = set()
    base = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/inventory_levels.json?location_ids={location_id}&limit=250"
    url = base
    pages = 0
    while url:
        r = shopify_request("GET", url, timeout=30)
        if r.status_code != 200:
            logger.warning(
                "inventory_levels call failed %s: %s", r.status_code, r.text[:500]
            )
            break
        data = r.json() or {}
        for lvl in data.get("inventory_levels", []) or []:
            inv_id = lvl.get("inventory_item_id")
            if inv_id:
                collected.add(str(inv_id))
        # pagination
        link = r.headers.get("Link")
        url = _parse_link_next(link)
        pages += 1
        if max_pages is not None and pages >= max_pages:
            break
    return collected


def shopify_get_variants_by_inventory_item_ids(item_ids: list[str]):
    """Fetch variants for given inventory_item_ids in batches. Returns list of variant dicts."""
    all_variants = []
    if not item_ids:
        return all_variants
    batch_size = 50  # Shopify allows quite many IDs; keep conservative
    for i in range(0, len(item_ids), batch_size):
        batch = item_ids[i : i + batch_size]
        qs = ",".join(batch)
        url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/variants.json?limit=250&inventory_item_ids={qs}"
        r = shopify_request("GET", url, timeout=30)
        if r.status_code != 200:
            logger.warning(
                "variants by inventory_item_ids failed %s: %s",
                r.status_code,
                r.text[:500],
            )
            continue
        data = r.json() or {}
        all_variants.extend(data.get("variants", []) or [])
    return all_variants


def shopify_find_product_ids_by_location(location_id: str):
    """Return a set of product IDs that have any inventory item connected to the given location."""
    inv_ids = list(shopify_list_inventory_item_ids_for_location(location_id))
    if not inv_ids:
        return set()
    variants = shopify_get_variants_by_inventory_item_ids(inv_ids)
    product_ids = set()
    for v in variants:
        pid = v.get("product_id")
        if pid:
            product_ids.add(str(pid))
    return product_ids


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

    checked = deleted_marked = recreate_staged = repaired = inconsistencies_fixed = (
        errors
    ) = 0
    qs = StyliaProduct.objects.exclude(shopify_id__isnull=True).exclude(shopify_id="")
    for p in qs:
        checked += 1
        try:
            r = shopify_get_product(p.shopify_id)
            if r.status_code == 404:
                # Product missing on Shopify: either stage for recreation or mark deleted
                recreate = getattr(settings, "RECREATE_ON_MISSING_SHOPIFY", True)
                if recreate:
                    sdata = p.shopify_data or {"product": {}}
                    sdata.pop("existing_info", None)
                    sdata.pop("existing_product", None)
                    p.shopify_id = ""
                    p.shopify_handle = ""
                    p.location_assigned = False
                    p.location_last_error = ""
                    p.sync_status = "pending"
                    p.shopify_data = sdata
                    p.save(
                        update_fields=[
                            "shopify_id",
                            "shopify_handle",
                            "location_assigned",
                            "location_last_error",
                            "sync_status",
                            "shopify_data",
                        ]
                    )
                    recreate_staged += 1
                else:
                    p.mark_as_deleted()
                    deleted_marked += 1
            elif r.status_code in (200, 201):
                # Repair handle/links if missing
                try:
                    prod = r.json().get("product", {})
                except Exception:
                    prod = {}
                need_save = False
                new_handle = prod.get("handle") or ""
                if new_handle and new_handle != (p.shopify_handle or ""):
                    p.shopify_handle = new_handle
                    need_save = True
                urls = _build_shopify_urls(p.shopify_id, p.shopify_handle)
                sdata = p.shopify_data or {"product": {}}
                ex = sdata.get("existing_info") or {}
                if not ex.get("admin_url") or not ex.get("storefront_url"):
                    sdata["existing_info"] = {
                        "id": p.shopify_id,
                        "handle": p.shopify_handle,
                        **urls,
                    }
                    p.shopify_data = sdata
                    need_save = True
                if need_save:
                    p.save(update_fields=["shopify_handle", "shopify_data"])
                    repaired += 1
            else:
                errors += 1
        except Exception:
            errors += 1

    # Fix inconsistent rows: 'active' without a shopify_id should be restaged
    try:
        from django.db.models import Q

        bad = StyliaProduct.objects.filter(sync_status="active").filter(
            Q(shopify_id__isnull=True) | Q(shopify_id="")
        )
        for p in bad:
            p.sync_status = "pending"
            p.save(update_fields=["sync_status"])
            inconsistencies_fixed += 1
    except Exception:
        errors += 1

    return {
        "success": True,
        "summary": {
            "checked": checked,
            "deleted_marked": deleted_marked,
            "recreate_staged": recreate_staged,
            "repaired": repaired,
            "inconsistencies_fixed": inconsistencies_fixed,
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
                # Keep deleted records untouched, skip until reconciliation re-adds
                skipped += 1
                db.save()
                continue
            # If incoming data changed, stage for create or update based on whether it's already on Shopify
            if db.data_hash != data_hash:
                db.title = mapped["product"]["title"]
                db.price = price
                db.total_stock = sum(v.get("inventory_quantity", 0) for v in variants)
                db.variant_count = len(variants)
                db.product_type = mapped["product"].get("product_type", "Fashion")
                db.data_hash = data_hash
                db.shopify_data = mapped
                # Decide queue: if shopify_id exists, it's an update; else it's a create
                db.sync_status = "updated" if db.shopify_id else "pending"
                db.save()
                updated += 1
            else:
                # If previously failed, allow retry: push or update depending on Shopify presence
                if db.sync_status == "failed":
                    db.sync_status = "updated" if db.shopify_id else "pending"
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
    created = updated = errors = existing = 0
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
    for p in qs:
        # prevent concurrent updates for the same model
        if not _acquire_product_lock(p.model_code):
            logger.info("Skip %s due to active push lock", p.model_code)
            continue
        try:
            if not p.shopify_id:
                # Pre-check: try to locate an existing Shopify product by SKUs or model tag/title to avoid duplicate creates
                try:
                    skus = [
                        v.get("sku")
                        for v in p.shopify_data.get("product", {}).get("variants") or []
                    ]
                    found, found_sku = shopify_find_product_by_skus(skus)
                except Exception:
                    found, found_sku = None, None
                if not found:
                    try:
                        found = shopify_find_product_by_model(
                            model_code=p.model_code,
                            brand=p.brand,
                            title=p.shopify_data.get("product", {}).get("title") or "",
                            handle=p.shopify_handle,
                        )
                    except Exception:
                        found = None
                if found:
                    try:
                        from .models import StyliaProduct

                        existing_sp = StyliaProduct.objects.filter(
                            shopify_id=str(found.get("id"))
                        ).first()
                        if existing_sp and existing_sp.pk != p.pk:
                            _merge_into_existing_product(
                                existing_sp, p, found, location_id_cache
                            )
                            updated += 1
                            continue
                        else:
                            p.shopify_id = found.get("id")
                            p.shopify_handle = found.get("handle") or p.shopify_handle
                            # persist existing info (urls + snapshot)
                            sdata = p.shopify_data or {"product": {}}
                            sdata["existing_product"] = found
                            urls = _build_shopify_urls(
                                found.get("id"), found.get("handle")
                            )
                            sdata["existing_info"] = {
                                "id": found.get("id"),
                                "handle": found.get("handle"),
                                **urls,
                            }
                            p.shopify_data = sdata
                            p.save(
                                update_fields=[
                                    "shopify_id",
                                    "shopify_handle",
                                    "shopify_data",
                                ]
                            )
                    except Exception:
                        logger.exception(
                            "Failed to persist mapping from pre-check find"
                        )
                    # proceed with update path instead of create
                    try:
                        upd_prod = p.shopify_data.get("product", {})
                        upd_prod = _enrich_variant_ids(p, upd_prod)
                        upd_prod = _dedupe_variants_by_options(upd_prod)
                        prep_payload = _normalize_shopify_payload({"product": upd_prod})
                        prep_payload = _strip_mismatched_variant_options_for_update(
                            p.shopify_id, prep_payload
                        )
                        updated_payload = shopify_update(
                            p.shopify_id,
                            _sanitize_update_payload(prep_payload),
                        )
                        _assign_inventory_to_location(
                            updated_payload.get("product"), location_id_cache, p
                        )
                        p.mark_as_synced()
                        updated += 1
                        existing += 1
                        # reduce backoff and sleep
                        error_backoff = max(0, error_backoff - 0.3)
                        sleep_for = per_call_sleep + error_backoff
                        if sleep_for:
                            time.sleep(sleep_for)
                        continue
                    except Exception:
                        logger.exception(
                            "Pre-check update path failed; will attempt create"
                        )
                try:
                    # normalize payload variants before sending
                    normalized_payload = _normalize_shopify_payload(p.shopify_data)
                    created_payload = shopify_create(normalized_payload)
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
                                "Found existing product for SKU %s (id=%s) — mapping locally and assigning inventory (no update).",
                                found_sku,
                                found.get("id"),
                            )
                            # persist mapping to DB and optionally merge duplicates
                            try:
                                from .models import StyliaProduct, Variant

                                existing_sp = StyliaProduct.objects.filter(
                                    shopify_id=str(found.get("id"))
                                ).first()
                                if existing_sp and existing_sp.pk != p.pk:
                                    logger.info(
                                        "Merging %s into %s for shopify_id=%s",
                                        p.model_code,
                                        existing_sp.model_code,
                                        found.get("id"),
                                    )
                                    _merge_into_existing_product(
                                        existing_sp, p, found, location_id_cache
                                    )
                                    updated += 1
                                    continue
                                p.shopify_id = found.get("id")
                                p.shopify_handle = (
                                    found.get("handle") or p.shopify_handle
                                )
                                # persist existing info (urls + snapshot)
                                sdata = p.shopify_data or {"product": {}}
                                sdata["existing_product"] = found
                                urls = _build_shopify_urls(
                                    found.get("id"), found.get("handle")
                                )
                                sdata["existing_info"] = {
                                    "id": found.get("id"),
                                    "handle": found.get("handle"),
                                    **urls,
                                }
                                p.shopify_data = sdata
                                p.save(
                                    update_fields=[
                                        "shopify_id",
                                        "shopify_handle",
                                        "shopify_data",
                                    ]
                                )
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
                                    "Failed to persist mapping for found product"
                                )

                            # assign inventory at location based on local quantities
                            try:
                                sku_qty = {}
                                for v in (
                                    p.shopify_data.get("product", {}).get("variants")
                                    or []
                                ):
                                    s = v.get("sku")
                                    if s:
                                        sku_qty[s] = int(v.get("inventory_quantity", 0))
                                if location_id_cache:
                                    for fv in found.get("variants") or []:
                                        s = fv.get("sku")
                                        inv_id = fv.get("inventory_item_id")
                                        if s and inv_id and s in sku_qty:
                                            try:
                                                inventory_connect(
                                                    inv_id, location_id_cache
                                                )
                                                inventory_set(
                                                    inv_id,
                                                    location_id_cache,
                                                    sku_qty[s],
                                                )
                                            except Exception:
                                                logger.exception(
                                                    "Failed inventory set for sku=%s (inventory_item_id=%s)",
                                                    s,
                                                    inv_id,
                                                )
                            except Exception:
                                logger.exception(
                                    "Inventory assignment after found product failed"
                                )
                            # Update location flags after manual assignment
                            try:
                                p.location_assigned = True
                                p.location_last_error = ""
                                p.save(
                                    update_fields=[
                                        "location_assigned",
                                        "location_last_error",
                                    ]
                                )
                            except Exception:
                                logger.exception(
                                    "Failed to set location flags after mapping existing product"
                                )
                            p.mark_as_synced()
                            updated += 1
                            existing += 1
                            continue
                    # if not handled by fallback, re-raise
                    raise
                # created successfully — ensure the returned shopify_id isn't already linked to another record
                created_shopify_id = created_payload["product"]["id"]
                from .models import StyliaProduct

                existing_sp = (
                    StyliaProduct.objects.filter(shopify_id=str(created_shopify_id))
                    .exclude(pk=p.pk)
                    .first()
                )
                if existing_sp:
                    logger.info(
                        "Created product id %s is already linked to StyliaProduct %s — merging new data into it.",
                        created_shopify_id,
                        existing_sp.model_code,
                    )
                    # Merge p into existing_sp and then continue
                    _merge_into_existing_product(
                        existing_sp, p, created_payload, location_id_cache
                    )
                    updated += 1
                    continue

                p.shopify_id = created_shopify_id
                p.shopify_handle = created_payload["product"].get("handle")
                # After create, try to set inventory at location
                _assign_inventory_to_location(
                    created_payload["product"], location_id_cache, p
                )
                p.mark_as_synced()
                created += 1
            else:
                upd_prod3 = p.shopify_data.get("product", {})
                upd_prod3 = _enrich_variant_ids(p, upd_prod3)
                upd_prod3 = _dedupe_variants_by_options(upd_prod3)
                try:
                    prep_payload2 = _normalize_shopify_payload({"product": upd_prod3})
                    prep_payload2 = _strip_mismatched_variant_options_for_update(
                        p.shopify_id, prep_payload2
                    )
                    updated_payload = shopify_update(
                        p.shopify_id,
                        _sanitize_update_payload(prep_payload2),
                    )
                    _assign_inventory_to_location(
                        updated_payload.get("product"), location_id_cache, p
                    )
                    p.mark_as_synced()
                    updated += 1
                except RuntimeError as e:
                    text = str(e)
                    # If Shopify says the variant already exists on update, treat as 'already existing'
                    if "variant" in text.lower() and "already exists" in text.lower():
                        try:
                            # Fetch the current Shopify product and persist mapping + existing_info
                            r = shopify_get_product(p.shopify_id)
                            prod_json = {}
                            try:
                                prod_json = (
                                    r.json().get("product", {})
                                    if r.status_code == 200
                                    else {}
                                )
                            except Exception:
                                prod_json = {}
                            sdata = p.shopify_data or {"product": {}}
                            sdata["existing_product"] = (
                                prod_json or sdata.get("existing_product") or {}
                            )
                            urls = _build_shopify_urls(p.shopify_id, p.shopify_handle)
                            sdata["existing_info"] = {
                                "id": p.shopify_id,
                                "handle": p.shopify_handle,
                                **urls,
                            }
                            p.shopify_data = sdata
                            p.save(update_fields=["shopify_data"])

                            # Persist Variant mappings from Shopify snapshot if available
                            try:
                                from .models import Variant

                                for v in prod_json.get("variants") or []:
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
                                            "shopify_product_id": p.shopify_id,
                                        },
                                    )
                            except Exception:
                                logger.exception(
                                    "Failed to persist variant mapping after 422 on update"
                                )

                            # Assign inventory at configured location using local quantities
                            try:
                                sku_qty = {}
                                for v in (
                                    p.shopify_data.get("product", {}).get("variants")
                                    or []
                                ):
                                    s = v.get("sku")
                                    if s:
                                        sku_qty[s] = int(v.get("inventory_quantity", 0))
                                if location_id_cache and prod_json:
                                    for fv in prod_json.get("variants") or []:
                                        s = fv.get("sku")
                                        inv_id = fv.get("inventory_item_id")
                                        if s and inv_id and s in sku_qty:
                                            try:
                                                inventory_connect(
                                                    inv_id, location_id_cache
                                                )
                                                inventory_set(
                                                    inv_id,
                                                    location_id_cache,
                                                    sku_qty[s],
                                                )
                                            except Exception:
                                                logger.exception(
                                                    "Failed inventory set for sku=%s (inventory_item_id=%s)",
                                                    s,
                                                    inv_id,
                                                )
                            except Exception:
                                logger.exception(
                                    "Inventory assignment after 422 on update failed"
                                )
                            # Update location flags after manual assignment
                            try:
                                p.location_assigned = True
                                p.location_last_error = ""
                                p.save(
                                    update_fields=[
                                        "location_assigned",
                                        "location_last_error",
                                    ]
                                )
                            except Exception:
                                logger.exception(
                                    "Failed to set location flags after 422 on update"
                                )
                            p.mark_as_synced()
                            updated += 1
                            existing += 1
                            # continue to next product
                            pass
                        except Exception:
                            # fall back to raising the original error if our handling fails
                            raise
                    else:
                        # re-raise non-duplicate errors
                        raise
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
        finally:
            _release_product_lock(p.model_code)

    return {
        "success": True,
        "summary": {
            "created": created,
            "updated": updated,
            "existing": existing,
            "errors": errors,
        },
    }


def process_creations():
    """Process only creations: records staged with sync_status='pending' and no shopify_id.

    Uses push_single_product for each record to reuse duplicate-prevention, enrichment,
    and inventory assignment logic.
    """
    from .models import StyliaProduct

    batch_size = getattr(settings, "SHOPIFY_BATCH_SIZE", 20)
    qs = (
        StyliaProduct.objects.filter(sync_status="pending")
        .filter(shopify_id__isnull=True)
        .order_by("updated_at")[:batch_size]
    )
    created = updated = existing = errors = 0
    for p in qs:
        res = push_single_product(p.pk)
        created += res["summary"]["created"]
        updated += res["summary"]["updated"]
        existing += res["summary"].get("existing", 0)
        errors += res["summary"]["errors"]
    return {
        "success": True,
        "summary": {
            "created": created,
            "updated": updated,
            "existing": existing,
            "errors": errors,
        },
    }


def process_updates():
    """Process only updates: records staged with sync_status='updated' and an existing shopify_id."""
    from .models import StyliaProduct

    batch_size = getattr(settings, "SHOPIFY_BATCH_SIZE", 20)
    qs = (
        StyliaProduct.objects.filter(sync_status="updated")
        .exclude(shopify_id__isnull=True)
        .exclude(shopify_id="")
        .order_by("updated_at")[:batch_size]
    )
    created = updated = existing = errors = 0
    for p in qs.iterator():
        res = push_single_product(p.pk)
        created += res["summary"]["created"]
        updated += res["summary"]["updated"]
        existing += res["summary"].get("existing", 0)
        errors += res["summary"]["errors"]
    return {
        "success": True,
        "summary": {
            "created": created,
            "updated": updated,
            "existing": existing,
            "errors": errors,
        },
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
    created = updated = existing = errors = 0
    # prevent concurrent update for same model code
    if not _acquire_product_lock(p.model_code):
        return {"success": False, "error": "locked"}
    try:
        if not p.shopify_id:
            try:
                # normalize payload before create
                created_payload = shopify_create(
                    _normalize_shopify_payload(p.shopify_data)
                )
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
                            from .models import StyliaProduct

                            existing_sp = StyliaProduct.objects.filter(
                                shopify_id=str(found.get("id"))
                            ).first()
                            if existing_sp and existing_sp.pk != p.pk:
                                # Merge p into existing_sp (source p will be deleted inside merge)
                                _merge_into_existing_product(
                                    existing_sp, p, found, location_id_cache
                                )
                                updated = 1
                                existing = 1
                            else:
                                p.shopify_id = found.get("id")
                                p.shopify_handle = (
                                    found.get("handle") or p.shopify_handle
                                )
                                # persist existing info (urls + snapshot)
                                sdata = p.shopify_data or {"product": {}}
                                sdata["existing_product"] = found
                                urls = _build_shopify_urls(
                                    found.get("id"), found.get("handle")
                                )
                                sdata["existing_info"] = {
                                    "id": found.get("id"),
                                    "handle": found.get("handle"),
                                    **urls,
                                }
                                p.shopify_data = sdata
                                p.save(
                                    update_fields=[
                                        "shopify_id",
                                        "shopify_handle",
                                        "shopify_data",
                                    ]
                                )
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
                                "Failed to persist variant mapping when switching to update (single push)"
                            )

                        # Don't update the product to avoid duplicate errors; assign inventory only
                        try:
                            sku_qty = {}
                            for v in (
                                p.shopify_data.get("product", {}).get("variants") or []
                            ):
                                s = v.get("sku")
                                if s:
                                    sku_qty[s] = int(v.get("inventory_quantity", 0))
                            if location_id_cache:
                                for fv in found.get("variants") or []:
                                    s = fv.get("sku")
                                    inv_id = fv.get("inventory_item_id")
                                    if s and inv_id and s in sku_qty:
                                        try:
                                            inventory_connect(inv_id, location_id_cache)
                                            inventory_set(
                                                inv_id, location_id_cache, sku_qty[s]
                                            )
                                        except Exception:
                                            logger.exception(
                                                "Failed inventory set for sku=%s (inventory_item_id=%s)",
                                                s,
                                                inv_id,
                                            )
                        except Exception:
                            logger.exception(
                                "Inventory assignment after found product failed (single push)"
                            )
                        # Update location flags after manual assignment
                        try:
                            p.location_assigned = True
                            p.location_last_error = ""
                            p.save(
                                update_fields=[
                                    "location_assigned",
                                    "location_last_error",
                                ]
                            )
                        except Exception:
                            logger.exception(
                                "Failed to set location flags after mapping existing product (single push)"
                            )
                        p.mark_as_synced()
                        updated = 1
                        existing = 1
                    else:
                        raise
                else:
                    raise
        else:
            sp_prod2 = p.shopify_data.get("product", {})
            sp_prod2 = _enrich_variant_ids(p, sp_prod2)
            sp_prod2 = _dedupe_variants_by_options(sp_prod2)
            try:
                updated_payload = shopify_update(
                    p.shopify_id,
                    _sanitize_update_payload(
                        _normalize_shopify_payload({"product": sp_prod2})
                    ),
                )
                _assign_inventory_to_location(
                    updated_payload.get("product"), location_id_cache, p
                )
                p.mark_as_synced()
                updated = 1
                existing = 1
            except RuntimeError as e:
                text = str(e)
                if "variant" in text.lower() and "already exists" in text.lower():
                    # Treat as 'already existing' — no update, just map and assign inventory; persist existing_info
                    try:
                        r = shopify_get_product(p.shopify_id)
                        prod_json = {}
                        try:
                            prod_json = (
                                r.json().get("product", {})
                                if r.status_code == 200
                                else {}
                            )
                        except Exception:
                            prod_json = {}
                        sdata = p.shopify_data or {"product": {}}
                        sdata["existing_product"] = (
                            prod_json or sdata.get("existing_product") or {}
                        )
                        urls = _build_shopify_urls(p.shopify_id, p.shopify_handle)
                        sdata["existing_info"] = {
                            "id": p.shopify_id,
                            "handle": p.shopify_handle,
                            **urls,
                        }
                        p.shopify_data = sdata
                        p.save(update_fields=["shopify_data"])

                        try:
                            from .models import Variant

                            for v in prod_json.get("variants") or []:
                                sku_val = v.get("sku")
                                if not sku_val:
                                    continue
                                Variant.objects.update_or_create(
                                    stylia_product=p,
                                    sku=sku_val,
                                    defaults={
                                        "shopify_variant_id": v.get("id"),
                                        "inventory_item_id": v.get("inventory_item_id"),
                                        "shopify_product_id": p.shopify_id,
                                    },
                                )
                        except Exception:
                            logger.exception(
                                "Failed to persist variant mapping after 422 on single update"
                            )

                        try:
                            sku_qty = {}
                            for v in (
                                p.shopify_data.get("product", {}).get("variants") or []
                            ):
                                s = v.get("sku")
                                if s:
                                    sku_qty[s] = int(v.get("inventory_quantity", 0))
                            if location_id_cache and prod_json:
                                for fv in prod_json.get("variants") or []:
                                    s = fv.get("sku")
                                    inv_id = fv.get("inventory_item_id")
                                    if s and inv_id and s in sku_qty:
                                        try:
                                            inventory_connect(inv_id, location_id_cache)
                                            inventory_set(
                                                inv_id, location_id_cache, sku_qty[s]
                                            )
                                        except Exception:
                                            logger.exception(
                                                "Failed inventory set for sku=%s (inventory_item_id=%s)",
                                                s,
                                                inv_id,
                                            )
                        except Exception:
                            logger.exception(
                                "Inventory assignment after 422 on single update failed"
                            )
                        # Update location flags after manual assignment
                        try:
                            p.location_assigned = True
                            p.location_last_error = ""
                            p.save(
                                update_fields=[
                                    "location_assigned",
                                    "location_last_error",
                                ]
                            )
                        except Exception:
                            logger.exception(
                                "Failed to set location flags after 422 on single update"
                            )
                        p.mark_as_synced()
                        updated = 1
                        existing = 1
                    except Exception:
                        raise
                else:
                    raise
    except Exception:
        tb = traceback.format_exc()
        logger.exception("Failed pushing single product %s: %s", p.model_code, tb)
        try:
            p.mark_as_failed(message=tb[:2000])
        except Exception:
            logger.exception("Failed to mark single product %s as failed", p.model_code)
        errors = 1
    finally:
        _release_product_lock(p.model_code)

    return {
        "success": True,
        "summary": {
            "created": created,
            "updated": updated,
            "existing": existing,
            "errors": errors,
        },
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


def ensure_location_assignments():
    """Best-effort: during every sync, re-check and ensure inventory is connected and set at the configured location.

    - Iterates a batch of products that have a shopify_id
    - Fetches the current Shopify product to get inventory_item_ids
    - Uses local shopify_data to determine per-SKU quantities
    - Connects inventory and sets level for matching SKUs

    Returns a summary dict useful for logging/monitoring.
    """
    from .models import StyliaProduct

    location_id = resolve_location_id()
    if not location_id:
        return {
            "success": True,
            "summary": {
                "checked": 0,
                "assigned": 0,
                "skipped": 0,
                "errors": 0,
                "note": "no_location_configured",
            },
        }

    batch_size = getattr(settings, "SHOPIFY_BATCH_SIZE", 20)
    qs = (
        StyliaProduct.objects.exclude(shopify_id__isnull=True)
        .exclude(shopify_id="")
        .filter(location_assigned=False)
        .order_by("updated_at")[:batch_size]
    )
    checked = assigned = skipped = errors = 0
    for p in qs:
        checked += 1
        try:
            r = shopify_get_product(p.shopify_id)
            if r.status_code != 200:
                skipped += 1
                continue
            prod = r.json().get("product") or {}
            # build desired quantities per SKU from local payload
            sku_qty = {}
            for v in p.shopify_data.get("product", {}).get("variants") or []:
                s = v.get("sku")
                if s:
                    try:
                        sku_qty[s] = int(v.get("inventory_quantity", 0))
                    except Exception:
                        sku_qty[s] = 0
            # assign inventory for matching SKUs
            had_any = False
            for fv in prod.get("variants") or []:
                s = fv.get("sku")
                inv_id = fv.get("inventory_item_id")
                if s and inv_id and s in sku_qty:
                    had_any = True
                    try:
                        inventory_connect(inv_id, location_id)
                        inventory_set(inv_id, location_id, sku_qty[s])
                    except Exception:
                        logger.exception(
                            "ensure_location_assignments: failed for %s sku=%s inv_id=%s",
                            p.model_code,
                            s,
                            inv_id,
                        )
            if had_any:
                p.location_assigned = True
                p.location_last_error = ""
                p.save(update_fields=["location_assigned", "location_last_error"])
                assigned += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            p.location_assigned = False
            p.location_last_error = str(e)[:1000]
            try:
                p.save(update_fields=["location_assigned", "location_last_error"])
            except Exception:
                logger.exception("Failed saving location flags for %s", p.model_code)

    return {
        "success": True,
        "summary": {
            "checked": checked,
            "assigned": assigned,
            "skipped": skipped,
            "errors": errors,
        },
    }
