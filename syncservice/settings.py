import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root; fallback to templates/.env if present (some setups put it there)
load_dotenv(dotenv_path=".env")
if not (Path(".env")).exists() and (Path("templates/.env")).exists():
    load_dotenv(dotenv_path="templates/.env")

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "dev-secret-key")
DEBUG = os.environ.get("DJANGO_DEBUG", "True") == "True"
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "syncapp",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "syncservice.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "syncservice.wsgi.application"
ASGI_APPLICATION = "syncservice.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Celery
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")
CELERY_RESULT_BACKEND = CELERY_BROKER_URL

# Partner and Store credentials (from existing project constants)
STYLIA_USERNAME = os.environ.get("STYLIA_USERNAME")
STYLIA_PASSWORD = os.environ.get("STYLIA_PASSWORD", "bexit 28%")
STYLIA_BASE_URL = os.environ.get("STYLIA_BASE_URL", "https://api.dropshippingsf.com")

SHOPIFY_STORE_URL = os.environ.get("SHOPIFY_STORE_URL")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")

# Shopify Location configuration (inventory levels)
# If SHOPIFY_LOCATION_ID is not provided, code may resolve by name at runtime.
SHOPIFY_LOCATION_ID = os.environ.get("SHOPIFY_LOCATION_ID", 112990716244)
SHOPIFY_LOCATION_NAME = os.environ.get("SHOPIFY_LOCATION_NAME", "Stylia Warehouse")
print(SHOPIFY_ACCESS_TOKEN)

# OAuth / App settings (for automatic token fetch)
SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY")
SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET")
# comma-separated scopes to request during install
SHOPIFY_OAUTH_SCOPES = os.environ.get(
    "SHOPIFY_OAUTH_SCOPES",
    "write_products,write_inventory,read_products,read_locations",
)
# Base URL for the app; used to build the OAuth redirect URI
APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://localhost:8000")
print("Password", STYLIA_PASSWORD)
print("Username", STYLIA_USERNAME)
