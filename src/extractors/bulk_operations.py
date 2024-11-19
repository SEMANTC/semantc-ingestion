import time
from typing import Optional, Dict, Any
import logging
from src.client.shopify_client import ShopifyClient
from src.extractors.base import BaseExtractor
from config.settings import BULK_OPERATION_POLL_INTERVAL

logger = logging.getLogger(__name__)

class BulkOperationExtractor(BaseExtractor):
    def __init__(self):
        self.client = ShopifyClient()

    def start_bulk_operation(self, query: str) -> str:
        """Start a bulk operation with the provided query"""
        mutation = f"""
        mutation {{
          bulkOperationRunQuery(
            query: {query}
          ) {{
            bulkOperation {{
              id
              status
            }}
            userErrors {{
              field
              message
            }}
          }}
        }}
        """
        result = self.client.execute_query(mutation)
        
        if 'userErrors' in result.get('data', {}).get('bulkOperationRunQuery', {}) and \
           result['data']['bulkOperationRunQuery']['userErrors']:
            errors = result['data']['bulkOperationRunQuery']['userErrors']
            raise Exception(f"Bulk operation failed to start: {errors}")
            
        return result['data']['bulkOperationRunQuery']['bulkOperation']['id']

    def check_bulk_operation_status(self, operation_id: str) -> Dict[str, Any]:
        """Check the status of a bulk operation"""
        query = f"""
        query {{
          node(id: "{operation_id}") {{
            ... on BulkOperation {{
              id
              status
              errorCode
              createdAt
              completedAt
              objectCount
              fileSize
              url
              partialDataUrl
            }}
          }}
        }}
        """
        result = self.client.execute_query(query)
        return result['data']['node']

    def wait_for_bulk_operation(self, operation_id: str) -> Optional[str]:
        """Wait for bulk operation to complete and return the result URL"""
        start_time = time.time()
        
        while True:
            status = self.check_bulk_operation_status(operation_id)
            
            if status['status'] == 'COMPLETED':
                duration = time.time() - start_time
                logger.info(f"Bulk operation completed in {duration:.2f} seconds")
                return status['url']
                
            elif status['status'] in ['FAILED', 'CANCELED']:
                error_msg = f"Bulk operation failed: {status['errorCode']}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
            logger.debug(f"Bulk operation status: {status['status']}")
            time.sleep(BULK_OPERATION_POLL_INTERVAL)

    def extract(self, query: str) -> Optional[str]:
        """Main extraction method"""
        try:
            operation_id = self.start_bulk_operation(query)
            return self.wait_for_bulk_operation(operation_id)
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise