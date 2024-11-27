# src/main.py

import logging
import os
import json
from datetime import datetime
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from extractors.bulk_operations import BulkOperationsExtractor
from extractors.shop_operations import ShopOperationsExtractor
from processors.data_processor import DataProcessor
from queries.bulk_queries import *

class SyncManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.extractor = BulkOperationsExtractor()
        self.processor = DataProcessor()
        self.shop_extractor = ShopOperationsExtractor()
        
        self.entities = {
            'orders': GET_ORDERS_QUERY,
            'products': GET_PRODUCTS_QUERY,
            'customers': GET_CUSTOMERS_QUERY,
            'collections': GET_COLLECTIONS_QUERY,
            'product_metafields': GET_PRODUCT_METAFIELDS_QUERY
        }

    def sync_entity(self, entity: str, query: str) -> Dict[str, Any]:
        """Sync a single entity"""
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        raw_file_path = f"data/raw/{entity}/{timestamp}.jsonl"
        processed_file_path = f"data/processed/{entity}/{timestamp}.json"

        try:
            result = self.extractor.extract(query, raw_file_path)
            if result['success']:
                self.processor.process_jsonl_file(raw_file_path, processed_file_path, entity)
                return {
                    'entity': entity,
                    'status': 'success',
                    'stats': {
                        'last_attempt': datetime.utcnow().isoformat(),
                        'last_success': datetime.utcnow().isoformat(),
                        'records_count': result.get('records_count', 0),
                        'file_size': result.get('file_size', 0),
                        'error': None,
                        'operation_id': result.get('operation_id')
                    }
                }
            return {
                'entity': entity,
                'status': 'failed',
                'stats': {
                    'last_attempt': datetime.utcnow().isoformat(),
                    'last_success': None,
                    'error': 'Extraction failed without error',
                    'operation_id': result.get('operation_id')
                }
            }
        except Exception as e:
            self.logger.error(f"Sync failed for {entity}", exc_info=True)
            return {
                'entity': entity,
                'status': 'failed',
                'stats': {
                    'last_attempt': datetime.utcnow().isoformat(),
                    'last_success': None,
                    'error': str(e),
                    'operation_id': None
                }
            }

    def sync_all(self) -> Dict[str, Any]:
        """Synchronize all entities"""
        sync_stats = {}
        
        # Sequential processing for bulk operations
        for entity, query in self.entities.items():
            self.logger.info(f"Starting sync for {entity}")
            result = self.sync_entity(entity, query)
            sync_stats[entity] = result['stats']
            
        # Process shop info
        self.logger.info("Starting sync for shop_info")
        try:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            result = self.shop_extractor.extract(output_dir='data')
            sync_stats['shop_info'] = {
                'last_attempt': datetime.utcnow().isoformat(),
                'last_success': datetime.utcnow().isoformat(),
                'records_count': 1,
                'file_size': result.get('file_size', 0),
                'error': None,
                'operation_id': None
            }
        except Exception as e:
            sync_stats['shop_info'] = {
                'last_attempt': datetime.utcnow().isoformat(),
                'last_success': None,
                'error': str(e),
                'operation_id': None
            }

        # Save sync results
        self._save_sync_stats(sync_stats)
        self._log_summary(sync_stats)
        
        return sync_stats

    def _save_sync_stats(self, stats: Dict[str, Any]) -> None:
        """Save sync stats to file"""
        stats_file = 'data/state/sync_stats.json'
        os.makedirs(os.path.dirname(stats_file), exist_ok=True)
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)

    def _log_summary(self, stats: Dict[str, Any]) -> None:
        """Log sync summary"""
        self.logger.info("Sync completed:")
        for entity, entity_stats in stats.items():
            if entity_stats['error']:
                self.logger.error(f"{entity}: Failed - {entity_stats['error']}")
            else:
                self.logger.info(f"{entity}: Success - {entity_stats.get('records_count', 0)} records")

def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    sync_manager = SyncManager()
    try:
        sync_manager.sync_all()
    except Exception as e:
        logging.error("Sync process failed", exc_info=True)
        raise

if __name__ == "__main__":
    main()