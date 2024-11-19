# test_connection.py
from src.client.shopify_client import ShopifyClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection():
    try:
        client = ShopifyClient()
        query = """
        {
          shop {
            name
            email
            myshopifyDomain
          }
        }
        """
        result = client.execute_query(query)
        logger.info(f"Successfully connected to: {result['data']['shop']['name']}")
        return True
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection()