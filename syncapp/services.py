import json
import hashlib
import requests
from django.conf import settings


def get_stylia_sync_list():
    url = f"{settings.STYLIA_BASE_URL}/sync/getItemsList"
    r = requests.get(
        url, auth=(settings.STYLIA_USERNAME, settings.STYLIA_PASSWORD), timeout=30
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise RuntimeError(f"Stylia API failed: {data}")
    return data.get("result", [])


def generate_hash(product):
    key = {
        "brand": product.get("brand", ""),
        "modelCode": product.get("modelCode", ""),
        "stock": product.get("stock", 0),
        "price": product.get("sellingPrice", 0),
        "items_count": len(product.get("items", [])),
    }
    return hashlib.md5(json.dumps(key, sort_keys=True).encode()).hexdigest()


def map_to_shopify(stylia_group):
    first_item = (stylia_group.get("items") or [{}])[0]
    title = f"{stylia_group['brand']} {stylia_group['modelCode']}"
    if first_item.get("colorName"):
        title += f" - {first_item['colorName']}"

    variants = []
    for item in stylia_group.get("items", []):
        v = {
            "price": str(stylia_group.get("sellingPrice", item.get("price", 0))),
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
    return {
        "X-Shopify-Access-Token": settings.SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }


def shopify_get_product(product_id):
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/products/{product_id}.json"
    return requests.get(url, headers=shopify_headers(), timeout=20)


def shopify_create(product_payload):
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/products.json"
    r = requests.post(url, headers=shopify_headers(), json=product_payload, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Shopify create failed: {r.status_code} - {r.text}")
    return r.json()


def shopify_update(product_id, product_payload):
    url = f"https://{settings.SHOPIFY_STORE_URL}/admin/api/2025-07/products/{product_id}.json"
    r = requests.put(url, headers=shopify_headers(), json=product_payload, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Shopify update failed: {r.status_code} - {r.text}")
    return r.json()


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

    for p in qs.iterator():
        try:
            if not p.shopify_id:
                created_payload = shopify_create(p.shopify_data)
                p.shopify_id = created_payload["product"]["id"]
                p.shopify_handle = created_payload["product"]["handle"]
                p.mark_as_synced()
                created += 1
            else:
                shopify_update(p.shopify_id, p.shopify_data)
                p.mark_as_synced()
                updated += 1
        except Exception:
            p.mark_as_failed()
            errors += 1

    return {
        "success": True,
        "summary": {"created": created, "updated": updated, "errors": errors},
    }
