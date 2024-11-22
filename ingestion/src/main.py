# src/main.py

import os
import json
from extractors.bulk_operations import BulkOperationsExtractor
from queries.bulk_queries import (
    GET_ORDERS_QUERY,
    GET_PRODUCTS_QUERY,
    GET_CUSTOMERS_QUERY,
    GET_INVENTORY_LEVELS_QUERY,
    GET_COLLECTIONS_QUERY,
    GET_PRODUCT_METAFIELDS_QUERY,
    GET_SHOP_INFO_QUERY,
    # add other queries as needed
)
from processors.data_processor import DataProcessor
# from loaders.gcs_loader import GCSLoader  # commented out as per instructions

def main():
    extractor = BulkOperationsExtractor()
    processor = DataProcessor()
    # loader = GCSLoader()  # commented out as per instructions

    # ensure 'data' directory exists
    os.makedirs('data', exist_ok=True)

    # list of tuples containing query and file names
    extraction_tasks = [
        (GET_ORDERS_QUERY, 'orders'),
        (GET_PRODUCTS_QUERY, 'products'),
        (GET_CUSTOMERS_QUERY, 'customers'),
        (GET_INVENTORY_LEVELS_QUERY, 'inventory_levels'),
        (GET_COLLECTIONS_QUERY, 'collections'),
        (GET_PRODUCT_METAFIELDS_QUERY, 'product_metafields'),
        (GET_SHOP_INFO_QUERY, 'shop_info'),
        # add other entities as needed
    ]

    for query, name in extraction_tasks:
        raw_data_file = os.path.join('data', f'{name}_data.jsonl')
        processed_data_file = os.path.join('data', f'processed_{name}_data.json')

        # extract data
        try:
            print(f"Starting extraction for {name}...")
            extractor.extract(query, raw_data_file)
            print(f"Data extraction for {name} completed.")
        except Exception as e:
            print(f"Data extraction for {name} failed: {e}")
            continue  # skip to the next entity

        # process data
        try:
            processed_data = processor.process_jsonl_file(raw_data_file, entity=name)
            with open(processed_data_file, 'w') as f:
                json.dump(processed_data, f)
            print(f"Data processing for {name} completed.")
        except Exception as e:
            print(f"Data processing for {name} failed: {e}")
            continue

        # commented out the gcs loading functionality
        # try:
        #     loader.upload_file(processed_data_file, os.path.basename(processed_data_file))
        #     print("Data uploaded to Google Cloud Storage.")
        # except Exception as e:
        #     print(f"Data upload failed: {e}")
        #     continue

if __name__ == '__main__':
    main()