import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Shopify settings
SHOP_URL = os.getenv('SHOPIFY_SHOP_URL')
ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
ADMIN_API_VERSION = "2024-01"
ADMIN_API_URL = f"https://{SHOP_URL}/admin/api/{ADMIN_API_VERSION}/graphql.json"

# Application settings
BULK_OPERATION_POLL_INTERVAL = int(os.getenv('BULK_OPERATION_POLL_INTERVAL', '5'))  # seconds
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '1'))  # seconds

# Optional GCP settings
GCS_BUCKET = os.getenv('GCS_BUCKET')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')