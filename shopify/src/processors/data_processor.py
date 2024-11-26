# src/processors/data_processor.py

import json
import logging
import os

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_jsonl_file(self, raw_file_path: str, processed_file_path: str, entity: str):
        """Process JSONL files based on entity type."""
        process_method = getattr(self, f'process_{entity}', None)
        if callable(process_method):
            process_method(raw_file_path, processed_file_path)
        else:
            self.logger.error(f"No processing method for entity: {entity}")

    def process_orders(self, raw_file_path: str, processed_file_path: str):
        """Process orders data, including nested lineItems."""
        processed_data = []
        orders_map = {}
        with open(raw_file_path, 'r') as raw_file:
            for line in raw_file:
                record = json.loads(line)
                if '__parentId' not in record:
                    # This is an order
                    order_id = record['id']
                    record['lineItems'] = []
                    orders_map[order_id] = record
                    processed_data.append(record)
                else:
                    # This is a lineItem
                    parent_id = record['__parentId']
                    if parent_id in orders_map:
                        orders_map[parent_id]['lineItems'].append(record)
        # Ensure directory exists
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
        with open(processed_file_path, 'w') as processed_file:
            json.dump(processed_data, processed_file, indent=2)
        self.logger.info(f"Successfully processed {len(processed_data)} orders with line items")

    def process_products(self, raw_file_path: str, processed_file_path: str):
        """Process products data, including nested variants."""
        processed_data = []
        products_map = {}
        with open(raw_file_path, 'r') as raw_file:
            for line in raw_file:
                record = json.loads(line)
                if '__parentId' not in record:
                    # This is a product
                    product_id = record['id']
                    record['variants'] = []
                    products_map[product_id] = record
                    processed_data.append(record)
                else:
                    # This is a variant
                    parent_id = record['__parentId']
                    if parent_id in products_map:
                        products_map[parent_id]['variants'].append(record)
        # Ensure directory exists
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
        with open(processed_file_path, 'w') as processed_file:
            json.dump(processed_data, processed_file, indent=2)
        self.logger.info(f"Successfully processed {len(processed_data)} products with variants")

    def process_customers(self, raw_file_path: str, processed_file_path: str):
        """Process customers data."""
        processed_data = []
        with open(raw_file_path, 'r') as raw_file:
            for line in raw_file:
                record = json.loads(line)
                processed_data.append(record)
        # Ensure directory exists
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
        with open(processed_file_path, 'w') as processed_file:
            json.dump(processed_data, processed_file, indent=2)
        self.logger.info(f"Successfully processed {len(processed_data)} customers")

    def process_collections(self, raw_file_path: str, processed_file_path: str):
        """Process collections data."""
        processed_data = []
        with open(raw_file_path, 'r') as raw_file:
            for line in raw_file:
                record = json.loads(line)
                processed_data.append(record)
        # Ensure directory exists
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
        with open(processed_file_path, 'w') as processed_file:
            json.dump(processed_data, processed_file, indent=2)
        self.logger.info(f"Successfully processed {len(processed_data)} collections")

    def process_product_metafields(self, raw_file_path: str, processed_file_path: str):
        """Process product metafields data."""
        processed_data = []
        products_map = {}
        with open(raw_file_path, 'r') as raw_file:
            for line in raw_file:
                record = json.loads(line)
                if '__parentId' not in record:
                    # This is a product
                    product_id = record['id']
                    record['metafields'] = []
                    products_map[product_id] = record
                    processed_data.append(record)
                else:
                    # This is a metafield
                    parent_id = record['__parentId']
                    if parent_id in products_map:
                        products_map[parent_id]['metafields'].append(record)
        # Ensure directory exists
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)
        with open(processed_file_path, 'w') as processed_file:
            json.dump(processed_data, processed_file, indent=2)
        self.logger.info(f"Successfully processed {len(processed_data)} products with metafields")