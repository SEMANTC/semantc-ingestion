# src/main.py
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any

from client.shopify_client import ShopifyClient
from extractors.bulk_operations import BulkOperationsExtractor
from processors.data_processor import DataProcessor
from processors.sync_state import SyncStateTracker
from queries.bulk_queries import (
    GET_ORDERS_QUERY,
    GET_PRODUCTS_QUERY,
    GET_CUSTOMERS_QUERY,
    GET_INVENTORY_QUERY,
    GET_COLLECTIONS_QUERY,
    GET_PRODUCT_METAFIELDS_QUERY,
    GET_SHOP_INFO_QUERY
)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def run_sync(entity: str, query: str, processor: DataProcessor, 
             extractor: BulkOperationsExtractor, state_tracker: SyncStateTracker) -> Dict[str, Any]:
    """Run synchronization for a single entity"""
    try:
        # Get last sync time
        last_sync = state_tracker.get_last_sync(entity)
        
        # Prepare paths with absolute paths
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_dir = os.path.join('/app/data/raw', entity)
        processed_dir = os.path.join('/app/data/processed', entity)
        
        # Create directories
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        raw_path = os.path.join(raw_dir, f"{timestamp}.jsonl")
        processed_path = os.path.join(processed_dir, f"{timestamp}.json")
        
        # Extract data
        result = extractor.extract(query, raw_path, incremental_date=last_sync)
        
        # Process data if extraction successful
        if result['success']:
            try:
                processed_data = processor.process_jsonl_file(raw_path, entity)
                with open(processed_path, 'w') as f:
                    json.dump(processed_data, f, indent=2)
                logging.info(f"Processed data written to {processed_path}")
            except Exception as e:
                logging.error(f"Error processing data: {str(e)}")
                raise
            
            # Update sync state
            state_tracker.update_sync_state(entity, {
                'success': True,
                **result
            })
            
            return {
                'success': True,
                'raw_path': raw_path,
                'processed_path': processed_path,
                **result
            }
            
    except Exception as e:
        logging.error(f"Sync failed for {entity}: {str(e)}")
        state_tracker.update_sync_state(entity, {
            'success': False,
            'error': str(e)
        })
        return {
            'success': False,
            'error': str(e)
        }

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        extractor = BulkOperationsExtractor()
        processor = DataProcessor()
        state_tracker = SyncStateTracker()
        
        # Define sync tasks
        tasks = [
            ('orders', GET_ORDERS_QUERY),
            ('products', GET_PRODUCTS_QUERY),
            ('customers', GET_CUSTOMERS_QUERY),
            ('inventory', GET_INVENTORY_QUERY),
            ('collections', GET_COLLECTIONS_QUERY),
            ('product_metafields', GET_PRODUCT_METAFIELDS_QUERY),
            ('shop_info', GET_SHOP_INFO_QUERY)
        ]
        
        results = {}
        for entity, query in tasks:
            logger.info(f"Starting sync for {entity}")
            results[entity] = run_sync(entity, query, processor, extractor, state_tracker)
            
        # Log final status
        logger.info("Sync completed:")
        for entity, result in results.items():
            if result['success']:
                logger.info(f"{entity}: Success - {result.get('records_count', 0)} records")
            else:
                logger.error(f"{entity}: Failed - {result.get('error')}")
        
        # Get overall stats
        stats = state_tracker.get_sync_stats()
        logger.info(f"Overall sync stats: {json.dumps(stats, indent=2)}")

    except Exception as e:
        logger.error(f"Critical error in main process: {str(e)}")
        raise

if __name__ == '__main__':
    main()