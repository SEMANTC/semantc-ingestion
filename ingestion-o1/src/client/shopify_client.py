# src/client/shopify_client.py

import requests
import os
from dotenv import load_dotenv

# load environment variables from the .env file
load_dotenv()

class ShopifyClient:
    def __init__(self):
        # get shopify store url and access token from environment variables
        self.store_url = os.getenv('SHOPIFY_STORE_URL')
        self.access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
        self.api_version = '2024-10'  # update to the latest api version as needed

        # construct the api endpoint url
        self.endpoint = f'https://{self.store_url}/admin/api/{self.api_version}/graphql.json'

        # set up headers for the api request
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': self.access_token
        }

    def execute(self, query, variables=None):
        # prepare the payload with the query and variables
        payload = {'query': query, 'variables': variables or {}}

        # make a post request to the shopify graphql endpoint
        response = requests.post(self.endpoint, json=payload, headers=self.headers)

        # raise an error if the request was unsuccessful
        response.raise_for_status()

        # parse the json response
        result = response.json()

        # check for graphql errors in the response
        if 'errors' in result:
            raise Exception(f"GraphQL errors: {result['errors']}")

        # return the data from the response
        return result['data']