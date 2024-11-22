# src/extractors/bulk_operations.py

import time
import json
import os
import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional
from client.shopify_client import ShopifyClient
from extractors.base import BaseExtractor

class BulkOperationsExtractor(BaseExtractor):
    def __init__(self):
        self.client = ShopifyClient()
        self.logger = logging.getLogger(__name__)
        self.MAX_RETRIES = 3
        self.POLL_INTERVAL = 5  # seconds
        self.MAX_WAIT_TIME = 3600  # 1 hour

    def extract(self, query: str, file_path: str, incremental_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Main extraction method"""
        for attempt in range(self.MAX_RETRIES):
            try:
                # Add incremental filter if date provided
                if incremental_date:
                    query = self._add_date_filter(query, incremental_date)

                # Start bulk operation
                bulk_op = self._start_bulk_operation(query)
                operation_id = bulk_op['id']
                self.logger.info(f"Started bulk operation {operation_id}")

                # Monitor progress
                status = self._monitor_operation(operation_id)
                
                if status['status'] == 'COMPLETED':
                    # Download and verify data
                    self._download_and_verify(status['url'], file_path)
                    return {
                        'success': True,
                        'operation_id': operation_id,
                        'records_count': status.get('objectCount', 0),
                        'file_size': status.get('fileSize', 0)
                    }
                else:
                    raise Exception(f"Operation failed: {status.get('errorCode', 'Unknown error')}")

            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def _start_bulk_operation(self, query: str) -> Dict[str, Any]:
        """Start a bulk operation"""
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
            
        return response['bulkOperationRunQuery']['bulkOperation']

    def _monitor_operation(self, operation_id: str) -> Dict[str, Any]:
        """Monitor bulk operation status"""
        query = '''
        {
            currentBulkOperation {
                id
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
        
        start_time = time.time()
        while time.time() - start_time < self.MAX_WAIT_TIME:
            response = self.client.execute(query)
            current_op = response.get('currentBulkOperation')
            
            if not current_op:
                raise Exception("No active bulk operation found")
                
            status = current_op['status']
            if status in ['COMPLETED', 'FAILED', 'CANCELED']:
                return current_op
                
            self.logger.info(f"Operation status: {status}, Objects processed: {current_op.get('objectCount', 0)}")
            time.sleep(self.POLL_INTERVAL)
            
        raise TimeoutError(f"Operation {operation_id} timed out")

    def _download_and_verify(self, url: str, file_path: str) -> None:
        """Download and verify the bulk operation result"""
        temp_path = f"{file_path}.tmp"
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Download file
            response = requests.get(url)
            response.raise_for_status()
            
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            # Verify file integrity
            if self._verify_file(temp_path):
                os.rename(temp_path, file_path)
                self.logger.info(f"Data downloaded to {file_path}")
            else:
                raise Exception("File verification failed")
                
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _verify_file(self, file_path: str) -> bool:
        """Verify the downloaded file integrity"""
        try:
            with open(file_path, 'r') as f:
                # Check first and last line can be parsed
                first_line = next(f)
                json.loads(first_line)
                
                # Seek to last line
                f.seek(max(0, os.path.getsize(file_path) - 4096))
                last_line = list(f)[-1]
                json.loads(last_line)
                
            return True
        except Exception as e:
            self.logger.error(f"File verification failed: {str(e)}")
            return False

    @staticmethod
    def _add_date_filter(query: str, date: datetime) -> str:
        """Add incremental date filter to query"""
        date_str = date.strftime("%Y-%m-%d")
        return query.replace(
            "{INCREMENTAL_FILTER}", 
            f"updated_at:>={date_str}"
        )