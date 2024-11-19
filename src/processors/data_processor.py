import json
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from src.loaders.gcs_loader import GCSLoader
from config.settings import MAX_RETRIES, RETRY_DELAY
import time

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, bucket_name: str):
        self.gcs_loader = GCSLoader(bucket_name)

    def process_bulk_results(self, url: str) -> List[Dict[str, Any]]:
        """
        Process results from a bulk operation
        
        Args:
            url (str): URL to the bulk operation results
            
        Returns:
            List[Dict[str, Any]]: Processed data
        """
        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                results = []
                for line in response.iter_lines():
                    if line:
                        data = json.loads(line.decode('utf-8'))
                        processed_data = self._transform_data(data)
                        if processed_data:
                            results.append(processed_data)
                
                return results
                
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries == MAX_RETRIES:
                    logger.error(f"Failed to process bulk results after {MAX_RETRIES} attempts: {str(e)}")
                    raise
                
                logger.warning(f"Processing failed, retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)

    def _transform_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform raw data into the desired format
        
        Args:
            data (Dict[str, Any]): Raw data to transform
            
        Returns:
            Optional[Dict[str, Any]]: Transformed data or None if invalid
        """
        try:
            # Add processing timestamp
            data['processed_at'] = datetime.utcnow().isoformat()
            
            # Remove any null values
            return {k: v for k, v in data.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            return None

    def process_and_load(self, url: str, resource_type: str) -> str:
        """
        Process bulk results and load them to GCS
        
        Args:
            url (str): URL to the bulk operation results
            resource_type (str): Type of resource being processed
            
        Returns:
            str: GCS path where data was saved
        """
        try:
            logger.info(f"Starting to process {resource_type} data...")
            data = self.process_bulk_results(url)
            
            logger.info(f"Processed {len(data)} {resource_type} records")
            return self.gcs_loader.save_data(data, resource_type)
            
        except Exception as e:
            logger.error(f"Error processing {resource_type}: {str(e)}")
            raise

    def validate_data(self, data: List[Dict[str, Any]], resource_type: str) -> bool:
        """
        Validate processed data before saving
        
        Args:
            data (List[Dict[str, Any]]): Data to validate
            resource_type (str): Type of resource being validated
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not data:
            logger.warning(f"No {resource_type} data to validate")
            return False
            
        required_fields = {
            'products': ['id', 'title'],
            'orders': ['id', 'created_at'],
            'customers': ['id', 'email']
        }
        
        if resource_type not in required_fields:
            logger.warning(f"No validation rules for resource type: {resource_type}")
            return True
            
        fields = required_fields[resource_type]
        for item in data:
            if not all(field in item for field in fields):
                logger.error(f"Missing required fields in {resource_type} data")
                return False
                
        return True