# src/extractors/bulk_operations.py

import time
import requests
from client.shopify_client import ShopifyClient
from extractors.base import BaseExtractor

class BulkOperationsExtractor(BaseExtractor):
    def __init__(self):
        self.client = ShopifyClient()

    def start_bulk_operation(self, query):
        mutation = '''
        mutation bulkOperationRunQuery($query: String!) {
          bulkOperationRunQuery(query: $query) {
            bulkOperation {
              id
              status
            }
            userErrors {
              field
              message
            }
          }
        }
        '''
        variables = {'query': query}
        response = self.client.execute(mutation, variables)
        user_errors = response['bulkOperationRunQuery'].get('userErrors', [])
        if user_errors:
            raise Exception(f"User errors: {user_errors}")
        bulk_op = response['bulkOperationRunQuery']['bulkOperation']
        return bulk_op

    def check_bulk_operation_status(self):
        query = '''
        {
          currentBulkOperation {
            status
            errorCode
            createdAt
            completedAt
            objectCount
            fileSize
            url
            partialDataUrl
          }
        }
        '''
        while True:
            response = self.client.execute(query)
            bulk_op = response.get('currentBulkOperation')
            status = bulk_op['status']
            if status in ['COMPLETED', 'FAILED', 'CANCELED']:
                return bulk_op
            print(f"bulk operation status: {status}. Waiting...")
            time.sleep(5)

    def download_bulk_operation_data(self, url, file_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"data downloaded to {file_path}")

    def extract(self, query, file_path):
        # start the bulk operation
        bulk_op = self.start_bulk_operation(query)
        print(f"started bulk operation with ID: {bulk_op['id']}")

        # wait for the bulk operation to complete
        bulk_op_status = self.check_bulk_operation_status()
        if bulk_op_status['status'] == 'COMPLETED':
            print("bulk operation completed successfully.")
            # download the data
            data_url = bulk_op_status['url']
            self.download_bulk_operation_data(data_url, file_path)
        else:
            raise Exception(f"bulk operation failed with status: {bulk_op_status['status']}")