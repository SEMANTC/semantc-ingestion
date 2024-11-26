# src/client/shopify_client.py

import os
import requests
import logging
from typing import Dict, Any, Optional

class ShopifyClient:
    def __init__(self):
        self.store_url = os.getenv('SHOPIFY_STORE_URL')
        self.access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
        self.api_version = '2024-01'
        self.logger = logging.getLogger(__name__)

        if not self.store_url or not self.access_token:
            raise ValueError("SHOPIFY_STORE_URL and SHOPIFY_ACCESS_TOKEN must be set")

        self.endpoint = f'https://{self.store_url}/admin/api/{self.api_version}/graphql.json'
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': self.access_token
        }

    def execute(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a GraphQL query against Shopify's API"""
        try:
            payload = {'query': query, 'variables': variables or {}}
            
            response = requests.post(
                self.endpoint,
                json=payload,
                headers=self.headers
            )
            response.raise_for_status()
            
            result = response.json()
            if 'errors' in result:
                raise Exception(f"GraphQL errors: {result['errors']}")
                
            return result['data']
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise

    def get_shop_info(self) -> Dict[str, Any]:
        """Get shop information using regular GraphQL query"""
        query = """
        {
            shop {
                id
                name
                email
                primaryDomain {
                    url
                }
                currencyCode
                timezoneAbbreviation
                billingAddress {
                    city 
                    country
                    zip
                }
            }
        }
        """
        result = self.execute(query)
        return result['shop']