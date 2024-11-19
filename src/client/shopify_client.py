from typing import Dict, Any
import requests
from config.settings import ADMIN_API_URL, ACCESS_TOKEN, MAX_RETRIES, RETRY_DELAY
import time
import logging

logger = logging.getLogger(__name__)

class ShopifyClient:
    def __init__(self):
        self.url = ADMIN_API_URL
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': ACCESS_TOKEN
        }

    def execute_query(self, query: str) -> Dict[str, Any]:
        """Execute GraphQL query with retry mechanism"""
        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.post(
                    self.url,
                    json={'query': query},
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries == MAX_RETRIES:
                    logger.error(f"Failed to execute query after {MAX_RETRIES} attempts: {str(e)}")
                    raise
                
                logger.warning(f"Request failed, retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)

    def health_check(self) -> bool:
        """Check if the Shopify API is accessible"""
        try:
            query = """
            {
              shop {
                name
              }
            }
            """
            response = self.execute_query(query)
            return 'data' in response and 'shop' in response['data']
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False