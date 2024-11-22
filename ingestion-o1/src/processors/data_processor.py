# src/processors/data_processor.py

import json

class DataProcessor:
    def process_jsonl_file(self, input_file_path, output_file_path=None):
        processed_data = []
        with open(input_file_path, 'r') as infile:
            for line in infile:
                record = json.loads(line)
                processed_record = self.process_record(record)
                processed_data.append(processed_record)
        
        if output_file_path:
            with open(output_file_path, 'w') as outfile:
                json.dump(processed_data, outfile)
        
        return processed_data

    def process_record(self, record):
        # customize this method based on your data processing needs
        # example: extract specific fields from the record
        processed_record = {
            'id': record.get('id'),
            'createdAt': record.get('createdAt'),
            'totalPrice': record.get('totalPriceSet', {}).get('shopMoney', {}).get('amount'),
            'currencyCode': record.get('totalPriceSet', {}).get('shopMoney', {}).get('currencyCode'),
            'customerEmail': record.get('customer', {}).get('email'),
            'lineItems': [
                {
                    'productId': item.get('product', {}).get('id'),
                    'productTitle': item.get('product', {}).get('title'),
                    'quantity': item.get('quantity'),
                    'price': item.get('price')
                }
                for item in self.extract_line_items(record)
            ]
        }
        return processed_record

    def extract_line_items(self, record):
        line_items = []
        edges = record.get('lineItems', {}).get('edges', [])
        for edge in edges:
            node = edge.get('node', {})
            line_items.append(node)
        return line_items