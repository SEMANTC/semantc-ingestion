# src/main.py

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from extractors.bulk_operations import BulkOperationsExtractor
from processors.data_processor import DataProcessor
from processors.sync_state import SyncStateTracker
from queries.bulk_queries import (
    GET_ORDERS_QUERY,
    GET_PRODUCTS_QUERY,
    GET_CUSTOMERS_QUERY,
    GET_INVENTORY_LEVELS_QUERY,
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
    """run synchronization for a single entity"""
    try:
        # get last sync time
        last_sync = state_tracker.get_last_sync(entity)
        
        # prepare paths
        raw_path = f'data/raw/{entity}/{datetime.now().strftime("%Y%m%d_%H%M%S")}.jsonl'
        processed_path = f'data/processed/{entity}/{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        # create directories if they don't exist
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        os.makedirs(os.path.dirname(processed_path), exist_ok=True)
        
        # extract data
        result = extractor.extract(query, raw_path, incremental_date=last_sync)
        
        # process data if extraction successful
        if result['success']:
            processed_data = processor.process_jsonl_file(raw_path, entity)
            with open(processed_path, 'w') as f:
                json.dump(processed_data, f)
            
            # update sync state
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
        logging.error(f"sync failed for {entity}: {str(e)}")
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
        
        # define sync tasks
        tasks = [
            ('orders', GET_ORDERS_QUERY),
            ('products', GET_PRODUCTS_QUERY),
            ('customers', GET_CUSTOMERS_QUERY),
            ('inventory_levels', GET_INVENTORY_LEVELS_QUERY),
            ('collections', GET_COLLECTIONS_QUERY),
            ('product_metafields', GET_PRODUCT_METAFIELDS_QUERY),
            ('shop_info', GET_SHOP_INFO_QUERY)
        ]
        
        results = {}
        for entity, query in tasks:
            logger.info(f"starting sync for {entity}")
            results[entity] = run_sync(entity, query, processor, extractor, state_tracker)
            
        # log final status
        logger.info("sync completed:")
        for entity, result in results.items():
            if result['success']:
                logger.info(f"{entity}: success - {result.get('records_count', 0)} records")
            else:
                logger.error(f"{entity}: failed - {result.get('error')}")
        
        # get overall stats
        stats = state_tracker.get_sync_stats()
        logger.info(f"overall sync stats: {json.dumps(stats, indent=2)}")

    except Exception as e:
        logger.error(f"critical error in main process: {str(e)}")
        raise

if __name__ == '__main__':
    main()