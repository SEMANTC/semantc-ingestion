# src/processors/data_processor.py

import json
import logging
from typing import List, Dict, Any, Union

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_jsonl_file(self, input_file_path: str, entity: str) -> List[Dict[str, Any]]:
        """Process a JSONL file and return structured data"""
        processed_data = []
        try:
            self.logger.info(f"Processing {entity} data from {input_file_path}")
            with open(input_file_path, 'r') as infile:
                for line_num, line in enumerate(infile, 1):
                    try:
                        record = json.loads(line)
                        processed_record = self.process_record(record, entity)
                        if processed_record is not None:
                            if isinstance(processed_record, list):
                                processed_data.extend(processed_record)
                            else:
                                processed_data.append(processed_record)
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Error decoding JSON at line {line_num}: {str(e)}")
                        continue
                    except Exception as e:
                        self.logger.error(f"Error processing record at line {line_num}: {str(e)}")
                        continue

            self.logger.info(f"Successfully processed {len(processed_data)} records for {entity}")
            return processed_data
        except Exception as e:
            self.logger.error(f"Error processing file {input_file_path}: {str(e)}")
            raise

    def process_record(self, record: Dict[str, Any], entity: str) -> Union[Dict[str, Any], List[Dict[str, Any]], None]:
        """Process a single record based on entity type"""
        try:
            if entity == 'orders':
                return self.process_order_record(record)
            elif entity == 'products':
                return self.process_product_record(record)
            elif entity == 'customers':
                return self.process_customer_record(record)
            elif entity == 'inventory_levels':
                return self.process_inventory_record(record)
            elif entity == 'collections':
                return self.process_collection_record(record)
            elif entity == 'product_metafields':
                return self.process_product_metafield_record(record)
            elif entity == 'shop_info':
                return self.process_shop_info_record(record)
            else:
                return record
        except Exception as e:
            self.logger.error(f"Error processing {entity} record: {str(e)}")
            return None

    def process_order_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process order data"""
        customer = record.get('customer', {})
        customer_email = customer.get('email') if customer else None

        line_items = [
            {
                'productId': item.get('product', {}).get('id'),
                'productTitle': item.get('product', {}).get('title'),
                'quantity': item.get('quantity'),
                'price': item.get('originalUnitPriceSet', {}).get('shopMoney', {}).get('amount'),
                'currencyCode': item.get('originalUnitPriceSet', {}).get('shopMoney', {}).get('currencyCode'),
            }
            for item in self.extract_line_items(record)
        ]

        return {
            'id': record.get('id'),
            'name': record.get('name'),
            'createdAt': record.get('createdAt'),
            'totalPrice': record.get('totalPriceSet', {}).get('shopMoney', {}).get('amount'),
            'currencyCode': record.get('totalPriceSet', {}).get('shopMoney', {}).get('currencyCode'),
            'customerEmail': customer_email,
            'lineItems': line_items,
        }

    def process_product_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process product data"""
        variants = [
            {
                'id': variant.get('id'),
                'sku': variant.get('sku'),
                'title': variant.get('title'),
                'price': variant.get('price'),
                'inventoryQuantity': variant.get('inventoryQuantity'),
            }
            for variant in self.extract_variants(record)
        ]

        return {
            'id': record.get('id'),
            'title': record.get('title'),
            'vendor': record.get('vendor'),
            'productType': record.get('productType'),
            'tags': record.get('tags'),
            'createdAt': record.get('createdAt'),
            'updatedAt': record.get('updatedAt'),
            'variants': variants,
        }

    def process_customer_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process customer data"""
        last_order = record.get('lastOrder', {})
        last_order_id = last_order.get('id') if last_order else None

        return {
            'id': record.get('id'),
            'email': record.get('email'),
            'firstName': record.get('firstName'),
            'lastName': record.get('lastName'),
            'createdAt': record.get('createdAt'),
            'ordersCount': record.get('ordersCount'),
            'totalSpent': record.get('totalSpent'),
            'lastOrderId': last_order_id,
        }

    def process_inventory_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process inventory data"""
        item = record.get('item', {})
        location = record.get('location', {})

        return {
            'id': record.get('id'),
            'available': record.get('available'),
            'updatedAt': record.get('updatedAt'),
            'locationId': location.get('id'),
            'locationName': location.get('name'),
            'itemId': item.get('id'),
            'sku': item.get('sku'),
            'tracked': item.get('tracked'),
            'productId': item.get('product', {}).get('id'),
            'productTitle': item.get('product', {}).get('title'),
        }

    def process_collection_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process collection data"""
        products = [
            {
                'id': product.get('id'),
                'title': product.get('title'),
            }
            for product in self.extract_products(record)
        ]

        return {
            'id': record.get('id'),
            'title': record.get('title'),
            'updatedAt': record.get('updatedAt'),
            'productsCount': record.get('productsCount'),
            'products': products,
        }

    def process_product_metafield_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process product metafield data"""
        metafields = [
            {
                'namespace': metafield.get('namespace'),
                'key': metafield.get('key'),
                'value': metafield.get('value'),
                'type': metafield.get('type'),
            }
            for metafield in self.extract_metafields(record)
        ]

        return {
            'id': record.get('id'),
            'title': record.get('title'),
            'metafields': metafields,
        }

    def process_shop_info_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process shop info data"""
        primary_domain = record.get('primaryDomain', {})
        return {
            'id': record.get('id'),
            'name': record.get('name'),
            'email': record.get('email'),
            'currencyCode': record.get('currencyCode'),
            'timezoneAbbreviation': record.get('timezoneAbbreviation'),
            'primaryDomainUrl': primary_domain.get('url'),
        }

    def extract_line_items(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract line items from record"""
        edges = record.get('lineItems', {}).get('edges', [])
        return [edge.get('node', {}) for edge in edges]

    def extract_variants(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract variants from record"""
        edges = record.get('variants', {}).get('edges', [])
        return [edge.get('node', {}) for edge in edges]

    def extract_products(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract products from record"""
        edges = record.get('products', {}).get('edges', [])
        return [edge.get('node', {}) for edge in edges]

    def extract_metafields(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract metafields from record"""
        edges = record.get('metafields', {}).get('edges', [])
        return [edge.get('node', {}) for edge in edges]