# src/extractors/bulk_operations.py

import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from extractors.base import BaseExtractor
from client.shopify_client import ShopifyClient

class BulkOperationsExtractor(BaseExtractor):
    MAX_RETRIES = 3
    POLL_INTERVAL = 5
    MAX_WAIT_TIME = 3600  # 1 hour

    def __init__(self):
        self.client = ShopifyClient()
        self.logger = logging.getLogger(__name__)

    def extract(self, query: str, file_path: str, incremental_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Execute bulk extraction with retries and monitoring
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                if incremental_date:
                    query = self._add_date_filter(query, incremental_date)
                
                bulk_op = self.start_bulk_operation(query)
                operation_id = bulk_op['id']
                self.logger.info(f"Started bulk operation {operation_id}")
                
                start_time = time.time()
                status = self._monitor_operation(operation_id, start_time)
                
                if status['status'] == 'COMPLETED':
                    self._download_and_verify(status['url'], file_path)
                    return {
                        'success': True,
                        'operation_id': operation_id,
                        'records_count': status['objectCount'],
                        'file_size': status['fileSize']
                    }
                else:
                    raise Exception(f"Operation failed: {status['errorCode']}")
                    
            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def _monitor_operation(self, operation_id: str, start_time: float) -> Dict[str, Any]:
        """
        Monitor bulk operation with timeout and detailed status
        """
        while time.time() - start_time < self.MAX_WAIT_TIME:
            status = self.check_bulk_operation_status()
            
            if status['status'] in ['COMPLETED', 'FAILED', 'CANCELED']:
                return status
                
            self._log_progress(status)
            time.sleep(self.POLL_INTERVAL)
            
        raise TimeoutError(f"Operation {operation_id} timed out")
    
    def _log_progress(self, status: Dict[str, Any]) -> None:
        """
        Log detailed progress information
        """
        if status.get('objectCount'):
            self.logger.info(
                f"Progress: {status['status']}, "
                f"Objects: {status['objectCount']}, "
                f"Size: {status.get('fileSize', 0) / 1024/1024:.2f}MB"
            )
    
    def _download_and_verify(self, url: str, file_path: str) -> None:
        """
        Download and verify the integrity of the bulk operation result
        """
        temp_path = f"{file_path}.tmp"
        try:
            self.download_bulk_operation_data(url, temp_path)
            
            # Verify file integrity
            if not self._verify_file(temp_path):
                raise Exception("File verification failed")
                
            os.rename(temp_path, file_path)
            
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
    
    def _verify_file(self, file_path: str) -> bool:
        """
        Verify the downloaded file integrity
        """
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
        """
        Add incremental date filter to query
        """
        date_str = date.strftime("%Y-%m-%d")
        return query.replace(
            "{INCREMENTAL_FILTER}", 
            f"updated_at:>={date_str}"
        )