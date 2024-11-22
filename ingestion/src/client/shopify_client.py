# src/client/shopify_client.py

import requests
import os

class ShopifyClient:
    def __init__(self):
        self.store_url = os.getenv('SHOPIFY_STORE_URL')
        self.access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
        self.api_version = '2024-01'  # update to the latest api version as needed

        # check if environment variables are set
        if not self.store_url or not self.access_token:
            raise ValueError("SHOPIFY_STORE_URL and SHOPIFY_ACCESS_TOKEN must be set")

        self.endpoint = f'https://{self.store_url}/admin/api/{self.api_version}/graphql.json'
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': self.access_token
        }

    def execute(self, query, variables=None):
        payload = {'query': query, 'variables': variables or {}}
        response = requests.post(self.endpoint, json=payload, headers=self.headers)
        response.raise_for_status()
        result = response.json()
        if 'errors' in result:
            raise Exception(f"GraphQL errors: {result['errors']}")
        return result['data']