# Stylia ↔ Shopify Sync Service

A standalone Django + Celery service to synchronize products between Stylia (partner) and a Shopify store every 30 minutes.

## Features
- Database-first architecture: Stylia → DB → Shopify
- Reconciliation step: respect merchant deletions in Shopify
- Create/Update push to Shopify
- 30-minute Celery beat schedule

## Setup
1. Install dependencies (use your existing environment).
2. Migrate DB:
   - python manage.py migrate
3. Run a manual sync:
   - python manage.py run_sync
4. Start Celery worker and (optionally) beat for scheduling.

Credentials are sourced from `syncservice/settings.py` using the same base URL and Shopify token from your existing project.
