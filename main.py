import asyncio
from typing import Dict, List
from src.extractors.bulk_operations import BulkOperationExtractor
from src.processors.data_processor import DataProcessor
from src.queries.bulk_queries import BulkQueries
from utils.helpers import setup_logging, format_error
from config.settings import GCS_BUCKET

logger = setup_logging()

RESOURCE_QUERIES = {
    'products': BulkQueries.PRODUCTS,
    'orders': BulkQueries.ORDERS,
    'customers': BulkQueries.CUSTOMERS
}

async def process_resource(
    extractor: BulkOperationExtractor,
    processor: DataProcessor,
    resource_type: str,
    query: str
) -> None:
    try:
        logger.info(f"Starting bulk operation for {resource_type}...")
        operation_id = extractor.start_bulk_operation(query)
        
        logger.info(f"Waiting for {resource_type} bulk operation to complete...")
        result_url = extractor.wait_for_bulk_operation(operation_id)
        
        if result_url:
            logger.info(f"Processing {resource_type} bulk operation results...")
            gcs_path = processor.process_and_load(result_url, resource_type)
            logger.info(f"{resource_type} data saved to {gcs_path}")
        
    except Exception as e:
        error_details = format_error(e, {'resource_type': resource_type})
        logger.error(f"Error processing {resource_type}: {error_details}")

async def main():
    extractor = BulkOperationExtractor()
    processor = DataProcessor(bucket_name=GCS_BUCKET)
    
    tasks = []
    for resource_type, query in RESOURCE_QUERIES.items():
        task = asyncio.create_task(
            process_resource(extractor, processor, resource_type, query)
        )
        tasks.append(task)
    
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())