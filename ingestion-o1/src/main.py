# src/main.py

import os
from extractors.bulk_operations import BulkOperationsExtractor
from queries.bulk_queries import GET_ORDERS_QUERY
from processors.data_processor import DataProcessor
# from loaders.gcs_loader import GCSLoader

def main():
    extractor = BulkOperationsExtractor()
    processor = DataProcessor()
    # loader = GCSLoader()

    # ensure 'data' directory exists
    os.makedirs('data', exist_ok=True)

    # define file paths inside 'data/' folder
    raw_data_file = os.path.join('data', 'orders_data.jsonl')
    processed_data_file = os.path.join('data', 'processed_orders_data.json')

    # Extract data
    try:
        extractor.extract(GET_ORDERS_QUERY, raw_data_file)
        print("Data extraction completed.")
    except Exception as e:
        print(f"Data extraction failed: {e}")
        return

    # process data
    processed_data = processor.process_jsonl_file(raw_data_file)
    with open(processed_data_file, 'w') as f:
        json.dump(processed_data, f)
    print("Data processing completed.")

    # try:
    #     loader.upload_file(processed_data_file, os.path.basename(processed_data_file))
    #     print("Data uploaded to Google Cloud Storage.")
    # except Exception as e:
    #     print(f"Data upload failed: {e}")

if __name__ == '__main__':
    main()