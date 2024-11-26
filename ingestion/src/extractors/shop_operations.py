# src/extractors/shop_operations.py

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from client.shopify_client import ShopifyClient
from processors.sync_state import SyncStateTracker
from extractors.base import BaseExtractor

class ShopOperationsExtractor(BaseExtractor):
    def __init__(self):
        self.client = ShopifyClient()
        self.logger = logging.getLogger(__name__)

    def extract(self, output_dir: str = '/app/data', 
                state_tracker: Optional[SyncStateTracker] = None) -> Dict[str, Any]:
        """Extract shop information"""
        try:
            # Prepare output directory
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            processed_dir = os.path.join(output_dir, 'processed', 'shop_info')
            os.makedirs(processed_dir, exist_ok=True)
            
            # Get shop info
            shop_data = self.client.get_shop_info()
            
            # Save to file
            processed_path = os.path.join(processed_dir, f"{timestamp}.json")
            with open(processed_path, 'w') as f:
                json.dump(shop_data, f, indent=2)
            
            result = {
                'success': True,
                'records_count': 1,
                'file_size': os.path.getsize(processed_path),
                'processed_path': processed_path
            }
            
            # Update sync state if tracker provided
            if state_tracker:
                state_tracker.update_sync_state('shop_info', result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to get shop info: {str(e)}")
            error_result = {
                'success': False,
                'error': str(e)
            }
            
            # Update sync state if tracker provided
            if state_tracker:
                state_tracker.update_sync_state('shop_info', error_result)
                
            return error_result