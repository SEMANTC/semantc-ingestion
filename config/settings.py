import os
from google.cloud import secret_manager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_secret(secret_id: str) -> str:
    """Retrieve secret from Google Cloud Secret Manager"""
    client = secret_manager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Project settings
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

# Shopify settings
SHOP_URL = get_secret('shopify-shop-url') if ENVIRONMENT == 'production' else os.getenv('SHOPIFY_SHOP_URL')
ACCESS_TOKEN = get_secret('shopify-access-token') if ENVIRONMENT == 'production' else os.getenv('SHOPIFY_ACCESS_TOKEN')
ADMIN_API_VERSION = "2024-01"
ADMIN_API_URL = f"https://{SHOP_URL}/admin/api/{ADMIN_API_VERSION}/graphql.json"

# GCP settings
GCS_BUCKET = get_secret('shopify-gcs-bucket') if ENVIRONMENT == 'production' else os.getenv('GCS_BUCKET')

# Application settings
BULK_OPERATION_POLL_INTERVAL = int(os.getenv('BULK_OPERATION_POLL_INTERVAL', '5'))  # seconds
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '1'))  # seconds