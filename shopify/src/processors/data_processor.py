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
        """Process orders data, including nested lineItems and refunds."""
        processed_data = []
        orders_map = {}
        line_items_map = {}
        refunds_map = {}
        refund_line_items_map = {}
        transactions_map = {}

        with open(raw_file_path, 'r') as raw_file:
            for line in raw_file:
                record = json.loads(line)

                if '__parentId' not in record:
                    # This is an order
                    order_id = record['id']
                    record['lineItems'] = []
                    record['refunds'] = []
                    orders_map[order_id] = record
                    processed_data.append(record)
                else:
                    parent_id = record['__parentId']
                    if parent_id in orders_map:
                        # Could be a lineItem or a refund
                        if 'variant' in record:
                            # It's a lineItem
                            orders_map[parent_id]['lineItems'].append(record)
                            line_items_map[record['id']] = record
                        elif 'refundLineItems' in record or 'transactions' in record:
                            # It's a refund
                            record['refundLineItems'] = []
                            record['transactions'] = []
                            orders_map[parent_id]['refunds'].append(record)
                            refunds_map[record['id']] = record
                        else:
                            self.logger.warning(f"Unrecognized record under order {parent_id}: {record}")
                    elif parent_id in refunds_map:
                        # Could be a refundLineItem or a transaction
                        if 'lineItem' in record:
                            # It's a refundLineItem
                            refunds_map[parent_id]['refundLineItems'].append(record)
                            refund_line_items_map[record['id']] = record
                        elif 'amountSet' in record:
                            # It's a transaction
                            refunds_map[parent_id]['transactions'].append(record)
                            transactions_map[record['id']] = record
                        else:
                            self.logger.warning(f"Unrecognized record under refund {parent_id}: {record}")
                    else:
                        self.logger.warning(f"Unrecognized parent ID: {parent_id}")

        # Ensure directory exists
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)

        with open(processed_file_path, 'w') as processed_file:
            json.dump(processed_data, processed_file, indent=2)

        self.logger.info(f"Successfully processed {len(processed_data)} orders with line items and refunds")

    def process_products(self, raw_file_path: str, processed_file_path: str):
        """Process products data, including nested variants and inventory levels."""
        processed_data = []
        products_map = {}
        variants_map = {}

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
                    parent_id = record['__parentId']
                    if parent_id in products_map:
                        # This is a variant
                        variant_id = record['id']
                        record['inventoryLevels'] = []
                        products_map[parent_id]['variants'].append(record)
                        variants_map[variant_id] = record
                    elif parent_id in variants_map:
                        # This is an inventoryLevel for a variant
                        variants_map[parent_id]['inventoryLevels'].append(record)
                    else:
                        self.logger.warning(f"Unrecognized parent ID: {parent_id}")

        # Ensure directory exists
        os.makedirs(os.path.dirname(processed_file_path), exist_ok=True)

        with open(processed_file_path, 'w') as processed_file:
            json.dump(processed_data, processed_file, indent=2)

        self.logger.info(f"Successfully processed {len(processed_data)} products with variants and inventory levels")

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