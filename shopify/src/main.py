# src/main.py

import logging
import os
import json
from datetime import datetime
from extractors.bulk_operations import BulkOperationsExtractor
from extractors.shop_operations import ShopOperationsExtractor
from processors.data_processor import DataProcessor
from queries.bulk_queries import (
    GET_ORDERS_QUERY,
    GET_PRODUCTS_QUERY,
    GET_CUSTOMERS_QUERY,
    GET_COLLECTIONS_QUERY,
    GET_PRODUCT_METAFIELDS_QUERY
)

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Exclude 'shop_info' from bulk entities
    bulk_entities = {
        'orders': GET_ORDERS_QUERY,
        'products': GET_PRODUCTS_QUERY,
        'customers': GET_CUSTOMERS_QUERY,
        'collections': GET_COLLECTIONS_QUERY,
        'product_metafields': GET_PRODUCT_METAFIELDS_QUERY
    }

    bulk_extractor = BulkOperationsExtractor()
    shop_extractor = ShopOperationsExtractor()
    processor = DataProcessor()

    sync_stats = {}

    # Process bulk entities
    for entity, query in bulk_entities.items():
        logger.info(f"Starting sync for {entity}")

        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        raw_file_path = f"data/raw/{entity}/{timestamp}.jsonl"
        processed_file_path = f"data/processed/{entity}/{timestamp}.json"

        try:
            result = bulk_extractor.extract(query, raw_file_path)
            processor.process_jsonl_file(raw_file_path, processed_file_path, entity)

            sync_stats[entity] = {
                'last_attempt': datetime.utcnow().isoformat(),
                'last_success': datetime.utcnow().isoformat(),
                'records_count': result.get('records_count', 0),
                'file_size': result.get('file_size', 0),
                'error': None,
                'operation_id': result.get('operation_id')
            }

            logger.info(f"{entity.capitalize()} data extraction and processing completed successfully.")

        except Exception as e:
            logger.error(f"Sync failed for {entity}: {str(e)}")
            sync_stats[entity] = {
                'last_attempt': datetime.utcnow().isoformat(),
                'last_success': None,
                'records_count': 0,
                'file_size': 0,
                'error': str(e),
                'operation_id': None
            }

    # Process shop_info separately
    logger.info("Starting sync for shop_info")
    try:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        processed_file_path = f"data/processed/shop_info/{timestamp}.json"
        result = shop_extractor.extract(output_dir='data')
        # No processing needed for shop_info as it's already JSON
        sync_stats['shop_info'] = {
            'last_attempt': datetime.utcnow().isoformat(),
            'last_success': datetime.utcnow().isoformat(),
            'records_count': 1,
            'file_size': result.get('file_size', 0),
            'error': None,
            'operation_id': None
        }
        logger.info("Shop_info data extraction completed successfully.")
    except Exception as e:
        logger.error(f"Sync failed for shop_info: {str(e)}")
        sync_stats['shop_info'] = {
            'last_attempt': datetime.utcnow().isoformat(),
            'last_success': None,
            'records_count': 0,
            'file_size': 0,
            'error': str(e),
            'operation_id': None
        }

    # Log sync summary
    logger.info("Sync completed:")
    for entity, stats in sync_stats.items():
        if stats['error']:
            logger.error(f"{entity}: Failed - {stats['error']}")
        else:
            logger.info(f"{entity}: Success - {stats['records_count']} records")

    # Save sync stats
    stats_file = 'data/state/sync_stats.json'
    os.makedirs(os.path.dirname(stats_file), exist_ok=True)
    with open(stats_file, 'w') as f:
        json.dump(sync_stats, f, indent=2)

if __name__ == "__main__":
    main()