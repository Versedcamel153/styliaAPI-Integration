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
    """Enforce strict rate limiting to prevent 429 errors.

    Uses a token bucket algorithm with MANDATORY sleep to ensure we never
    exceed Shopify's rate limits, even with concurrent workers.

    - Shopify allows 2 requests/second (Standard) or 4 requests/second (Plus)
    - We use a conservative approach: sleep BEFORE every request
    - Minimum 0.6 seconds between requests (guarantees < 2 req/sec)
    """
    # ALWAYS sleep before making a request - this is the key fix
    min_request_interval = getattr(settings, "SHOPIFY_MIN_REQUEST_INTERVAL", 0.6)

    client = _get_redis_client()
    if client:
        # Use Redis to coordinate across multiple workers
        key = "shopify:last_request_time"
        try:
            # Get last request time
            last_request = client.get(key)
            now = time.time()

            if last_request:
                last_time = float(last_request)
                elapsed = now - last_time

                # If not enough time has passed, sleep the difference
                if elapsed < min_request_interval:
                    sleep_time = min_request_interval - elapsed
                    logger.debug(f"Rate limiter: sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    now = time.time()  # Update time after sleep

            # Record this request time
            client.set(key, str(now), ex=2)  # Expire after 2 seconds
        except Exception as e:
            logger.warning(
                f"Redis rate limiter error: {e}, falling back to local sleep"
            )
            time.sleep(min_request_interval)
    else:
        # No Redis - just sleep locally (won't coordinate across workers but still helps)
        time.sleep(min_request_interval)


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


def shopify_request(method, url, max_retries=10, backoff_base=1.0, **kwargs):
    """Make a resilient request to Shopify with retries and backoff.

    Respects Retry-After header and X-Shopify-Shop-Api-Call-Limit to avoid bursting.
    Increased max_retries to 10 and backoff_base to 1.0 to be more patient with 429 errors.
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
                # For 429 specifically, use longer backoff
                if r.status_code == 429:
                    sleep_for = backoff_base * (2**attempt) + random.random() * 1.0
                else:
                    sleep_for = (
                        backoff_base * (2 ** (attempt - 1)) + random.random() * 0.5
                    )

            # Cap maximum sleep at 60 seconds to avoid hanging forever
            sleep_for = min(sleep_for, 60.0)

            logger.warning(
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


def _prepare_variant_payload_for_update(existing_product_json, local_variants):
    """
    Prepare variant payload for update by:
    1. Matching existing variants by SKU to get their IDs
    2. Stripping option fields that don't match the product's option schema

    This prevents:
    - "variant already exists" errors (by including existing IDs)
    - "Option values provided for X unknown option(s)" errors (by stripping mismatched options)
    """
    existing_variants = existing_product_json.get("variants", [])
    options = existing_product_json.get("options", [])
    option_count = len(options)

    # Build SKU to variant ID map
    sku_to_id = {}
    for v in existing_variants:
        if v.get("sku"):
            sku_to_id[v["sku"]] = v["id"]

    prepared_variants = []
    for local_var in local_variants:
        sku = local_var.get("sku")
        variant = dict(local_var)

        # Match to existing variant by SKU to get the ID
        if sku and sku in sku_to_id:
            variant["id"] = sku_to_id[sku]

        # Strip option fields beyond what product supports
        if option_count < 3:
            variant.pop("option3", None)
        if option_count < 2:
            variant.pop("option2", None)
        if option_count < 1:
            variant.pop("option1", None)

        # Remove read-only fields
        variant.pop("inventory_item_id", None)

        prepared_variants.append(variant)

    return prepared_variants


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
    from .models import StyliaProduct, Variant

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
                # OPTIMIZATION: Skip expensive GraphQL searches, just try to create
                # If product exists, we'll catch the 422 error and handle it
                # This reduces API calls from 2-3 down to 1 per new product

                # Create new product
                normalized_payload = _normalize_shopify_payload(p.shopify_data)
                try:
                    created_payload = shopify_create(normalized_payload)

                    p.shopify_id = str(created_payload["product"]["id"])
                    p.shopify_handle = created_payload["product"].get("handle")
                    p.save(update_fields=["shopify_id", "shopify_handle"])

                    # Assign inventory at location
                    _assign_inventory_to_location(
                        created_payload["product"], location_id_cache, p
                    )
                    p.mark_as_synced()
                    created += 1
                except RuntimeError as e:
                    # Check if this is a duplicate product error (422)
                    if "422" in str(e) and (
                        "already exists" in str(e).lower()
                        or "duplicate" in str(e).lower()
                    ):
                        logger.info(
                            "Product %s already exists in Shopify (422 duplicate), searching to link...",
                            p.model_code,
                        )
                        # Search for existing product by SKU or model code
                        try:
                            found = shopify_find_product_by_sku(p.model_code)
                            if found:
                                shopify_id_to_link = str(found.get("id"))

                                # Check if another StyliaProduct already has this shopify_id
                                existing_product = (
                                    StyliaProduct.objects.filter(
                                        shopify_id=shopify_id_to_link
                                    )
                                    .exclude(id=p.id)
                                    .first()
                                )

                                if existing_product:
                                    logger.warning(
                                        "Product %s: Shopify product %s is already linked to %s (id=%s). Skipping duplicate.",
                                        p.model_code,
                                        shopify_id_to_link,
                                        existing_product.model_code,
                                        existing_product.id,
                                    )
                                    # Mark as error or skip
                                    p.sync_status = "error"
                                    p.save(update_fields=["sync_status"])
                                    errors += 1
                                    continue

                                p.shopify_id = shopify_id_to_link
                                p.shopify_handle = found.get("handle")
                                p.sync_status = "active"  # Set to active, not synced
                                p.save(
                                    update_fields=[
                                        "shopify_id",
                                        "shopify_handle",
                                        "sync_status",
                                    ]
                                )

                                # Persist variant mappings
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
                                logger.info(
                                    "Successfully linked duplicate product %s to existing Shopify product %s",
                                    p.model_code,
                                    p.shopify_id,
                                )
                                existing += 1
                                continue
                        except Exception as search_err:
                            logger.exception(
                                "Failed to find/link duplicate product %s: %s",
                                p.model_code,
                                search_err,
                            )
                    # Re-raise if not a duplicate error or if linking failed
                    raise
            else:
                # Update existing product
                # Fetch current product to prepare payload with option stripping
                r = shopify_get_product(p.shopify_id)
                if r.status_code == 404:
                    logger.warning(
                        "Product %s not found in Shopify, marking for recreation",
                        p.model_code,
                    )
                    p.shopify_id = ""
                    p.shopify_handle = ""
                    p.sync_status = "pending"
                    p.save(
                        update_fields=["shopify_id", "shopify_handle", "sync_status"]
                    )
                    continue

                if r.status_code != 200:
                    raise RuntimeError(
                        f"Failed to fetch product for update: {r.status_code} - {r.text}"
                    )

                existing_product = r.json().get("product", {})
                local_variants = p.shopify_data.get("product", {}).get("variants", [])

                # Prepare variants with option stripping and ID matching
                prepared_variants = _prepare_variant_payload_for_update(
                    existing_product, local_variants
                )

                update_payload = {
                    "product": {
                        "id": p.shopify_id,
                        "variants": prepared_variants,
                    }
                }

                updated_payload = shopify_update(p.shopify_id, update_payload)
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
    import time

    batch_size = getattr(settings, "SHOPIFY_BATCH_SIZE", 20)
    product_delay = getattr(settings, "SHOPIFY_PRODUCT_DELAY", 1.5)

    qs = (
        StyliaProduct.objects.filter(sync_status="pending")
        .filter(shopify_id__isnull=True)
        .order_by("updated_at")[:batch_size]
    )
    created = updated = existing = errors = 0
    processed_count = 0

    for p in qs:
        res = push_single_product(p.pk)
        created += res["summary"]["created"]
        updated += res["summary"]["updated"]
        existing += res["summary"].get("existing", 0)
        errors += res["summary"]["errors"]

        processed_count += 1
        # Add delay between products to avoid rate limits (except after last product)
        if processed_count < len(qs) and product_delay > 0:
            logger.info(
                "Sleeping %.2fs before next product (processed %d/%d)",
                product_delay,
                processed_count,
                len(qs),
            )
            time.sleep(product_delay)

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
    import time

    batch_size = getattr(settings, "SHOPIFY_BATCH_SIZE", 20)
    product_delay = getattr(settings, "SHOPIFY_PRODUCT_DELAY", 1.5)

    qs = (
        StyliaProduct.objects.filter(sync_status="updated")
        .exclude(shopify_id__isnull=True)
        .exclude(shopify_id="")
        .order_by("updated_at")[:batch_size]
    )
    created = updated = existing = errors = 0
    processed_count = 0
    total_count = qs.count()

    for p in qs.iterator():
        res = push_single_product(p.pk)
        created += res["summary"]["created"]
        updated += res["summary"]["updated"]
        existing += res["summary"].get("existing", 0)
        errors += res["summary"]["errors"]

        processed_count += 1
        # Add delay between products to avoid rate limits (except after last product)
        if processed_count < total_count and product_delay > 0:
            logger.info(
                "Sleeping %.2fs before next product (processed %d/%d)",
                product_delay,
                processed_count,
                total_count,
            )
            time.sleep(product_delay)

    return {
        "success": True,
        "summary": {
            "created": created,
            "updated": updated,
            "existing": existing,
            "errors": errors,
        },
    }


def retry_pending_products():
    """Retry products stuck in 'pending' status that haven't been processed.

    This is useful for products that failed during sync due to temporary issues
    like network errors, rate limits, or transient Shopify API problems.
    """
    from .models import StyliaProduct
    from django.utils import timezone
    from datetime import timedelta
    import time

    # Retry products that have been pending for more than 5 minutes
    retry_threshold = timezone.now() - timedelta(minutes=5)
    batch_size = getattr(settings, "SHOPIFY_BATCH_SIZE", 20)
    product_delay = getattr(settings, "SHOPIFY_PRODUCT_DELAY", 1.5)

    qs = (
        StyliaProduct.objects.filter(sync_status="pending")
        .filter(updated_at__lt=retry_threshold)
        .order_by("updated_at")[:batch_size]
    )

    created = updated = existing = errors = 0
    retried_count = 0
    total_count = qs.count()

    for p in qs:
        logger.info(
            "Retrying stuck pending product: %s (last updated: %s)",
            p.model_code,
            p.updated_at,
        )
        res = push_single_product(p.pk)
        retried_count += 1
        created += res.get("summary", {}).get("created", 0)
        updated += res.get("summary", {}).get("updated", 0)
        existing += res.get("summary", {}).get("existing", 0)
        errors += res.get("summary", {}).get("errors", 0)

        # Add delay between products to avoid rate limits (except after last product)
        if retried_count < total_count and product_delay > 0:
            logger.info(
                "Sleeping %.2fs before next product (processed %d/%d)",
                product_delay,
                retried_count,
                total_count,
            )
            time.sleep(product_delay)

    logger.info("Retry pending products: processed %d products", retried_count)

    return {
        "success": True,
        "retried": retried_count,
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
    from .models import StyliaProduct, Variant

    try:
        p = StyliaProduct.objects.get(pk=product_id)
    except StyliaProduct.DoesNotExist:
        return {
            "success": False,
            "error": "not_found",
            "summary": {"created": 0, "updated": 0, "existing": 0, "errors": 1},
        }

    location_id_cache = resolve_location_id()
    created = updated = existing = errors = 0
    # prevent concurrent update for same model code
    if not _acquire_product_lock(p.model_code):
        return {
            "success": False,
            "error": "locked",
            "summary": {"created": 0, "updated": 0, "existing": 0, "errors": 0},
        }
    try:
        if not p.shopify_id:
            # Create new product
            try:
                created_payload = shopify_create(
                    _normalize_shopify_payload(p.shopify_data)
                )
                p.shopify_id = str(created_payload["product"]["id"])
                p.shopify_handle = created_payload["product"].get("handle")
                _assign_inventory_to_location(
                    created_payload["product"], location_id_cache, p
                )
                p.mark_as_synced()
                created = 1
            except RuntimeError as e:
                # Check if this is a duplicate product error (422)
                if "422" in str(e) and (
                    "already exists" in str(e).lower() or "duplicate" in str(e).lower()
                ):
                    logger.info(
                        "Product %s already exists in Shopify (422 duplicate), searching to link...",
                        p.model_code,
                    )
                    # Search for existing product by SKU or model code
                    try:
                        found = shopify_find_product_by_sku(p.model_code)
                        if found:
                            shopify_id_to_link = str(found.get("id"))

                            # Check if another StyliaProduct already has this shopify_id
                            existing_product = (
                                StyliaProduct.objects.filter(
                                    shopify_id=shopify_id_to_link
                                )
                                .exclude(id=p.id)
                                .first()
                            )

                            if existing_product:
                                logger.warning(
                                    "Product %s: Shopify product %s is already linked to %s (id=%s). Marking as skipped.",
                                    p.model_code,
                                    shopify_id_to_link,
                                    existing_product.model_code,
                                    existing_product.id,
                                )
                                # Mark as skipped so it's not stuck in pending
                                p.sync_status = "skipped"
                                p.push_last_error = f"Duplicate of {existing_product.model_code} (already linked to Shopify product {shopify_id_to_link})"
                                p.save(update_fields=["sync_status", "push_last_error"])
                                _release_product_lock(p.model_code)
                                return {
                                    "success": False,
                                    "error": "duplicate_already_linked",
                                    "summary": {
                                        "created": 0,
                                        "updated": 0,
                                        "existing": 0,
                                        "errors": 1,
                                    },
                                }

                            p.shopify_id = shopify_id_to_link
                            p.shopify_handle = found.get("handle")
                            p.sync_status = "active"  # Set to active, not synced
                            p.save(
                                update_fields=[
                                    "shopify_id",
                                    "shopify_handle",
                                    "sync_status",
                                ]
                            )

                            # Persist variant mappings
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
                            logger.info(
                                "Successfully linked duplicate product %s to existing Shopify product %s",
                                p.model_code,
                                p.shopify_id,
                            )
                            existing = 1
                        else:
                            errors = 1
                            _release_product_lock(p.model_code)
                            return {
                                "success": False,
                                "error": "duplicate_not_found",
                                "summary": {
                                    "created": 0,
                                    "updated": 0,
                                    "existing": 0,
                                    "errors": 1,
                                },
                            }
                    except Exception as search_err:
                        logger.exception(
                            "Failed to find/link duplicate product %s: %s",
                            p.model_code,
                            search_err,
                        )
                        _release_product_lock(p.model_code)
                        return {
                            "success": False,
                            "error": str(search_err),
                            "summary": {
                                "created": 0,
                                "updated": 0,
                                "existing": 0,
                                "errors": 1,
                            },
                        }
                else:
                    # Re-raise if not a duplicate error
                    raise
        else:
            # Update existing product
            r = shopify_get_product(p.shopify_id)
            if r.status_code == 404:
                p.shopify_id = ""
                p.sync_status = "pending"
                p.save(update_fields=["shopify_id", "sync_status"])
                _release_product_lock(p.model_code)
                return {
                    "success": False,
                    "error": "not_found_in_shopify",
                    "summary": {"created": 0, "updated": 0, "existing": 0, "errors": 1},
                }

            if r.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch product: {r.status_code} - {r.text}"
                )

            existing_product = r.json().get("product", {})
            local_variants = p.shopify_data.get("product", {}).get("variants", [])
            prepared_variants = _prepare_variant_payload_for_update(
                existing_product, local_variants
            )

            update_payload = {
                "product": {"id": p.shopify_id, "variants": prepared_variants}
            }
            try:
                updated_payload = shopify_update(p.shopify_id, update_payload)
                _assign_inventory_to_location(
                    updated_payload.get("product"), location_id_cache, p
                )
                p.mark_as_synced()
                updated = 1
            except RuntimeError as update_err:
                # Handle specific 422 variant errors gracefully
                if (
                    "422" in str(update_err)
                    and "already exists" in str(update_err).lower()
                ):
                    logger.warning(
                        "Product %s: Variant already exists in Shopify. Marking as active without update.",
                        p.model_code,
                    )
                    # Mark as active since the product exists in Shopify even if we can't update it
                    p.sync_status = "active"
                    p.push_last_error = f"Variant conflict: {str(update_err)[:500]}"
                    p.save(update_fields=["sync_status", "push_last_error"])
                    existing = 1
                else:
                    # Re-raise other errors
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
