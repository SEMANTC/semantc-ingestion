import json
from datetime import datetime
from google.cloud import storage
from typing import List, Dict, Any
import logging
from config.settings import PROJECT_ID

logger = logging.getLogger(__name__)

class GCSLoader:
    def __init__(self, bucket_name: str):
        """
        Initialize GCS loader
        
        Args:
            bucket_name (str): Name of the GCS bucket
        """
        self.storage_client = storage.Client(project=PROJECT_ID)
        self.bucket = self.storage_client.bucket(bucket_name)
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Ensure the GCS bucket exists, create if it doesn't"""
        if not self.bucket.exists():
            logger.info(f"Creating bucket: {self.bucket.name}")
            self.bucket.create(location="us-central1")

    def save_data(self, data: List[Dict[str, Any]], resource_type: str) -> str:
        """
        Save data to GCS with datetime partitioning
        
        Args:
            data (List[Dict[str, Any]]): Data to save
            resource_type (str): Type of resource being saved
            
        Returns:
            str: GCS path where data was saved
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            date_partition = datetime.now().strftime('%Y/%m/%d')
            
            # Create the blob path with partitioning
            blob_path = f"{resource_type}/{date_partition}/{resource_type}_{timestamp}.json"
            blob = self.bucket.blob(blob_path)
            
            # Add metadata
            metadata = {
                'resource_type': resource_type,
                'record_count': str(len(data)),
                'created_at': datetime.now().isoformat(),
                'content_type': 'application/json'
            }
            blob.metadata = metadata
            
            # Upload the data
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type='application/json'
            )
            
            logger.info(f"Saved {len(data)} records to gs://{self.bucket.name}/{blob_path}")
            return f"gs://{self.bucket.name}/{blob_path}"
            
        except Exception as e:
            logger.error(f"Error saving data to GCS: {str(e)}")
            raise

    def list_files(self, resource_type: str, date: str = None) -> List[str]:
        """
        List files in the bucket for a specific resource type and date
        
        Args:
            resource_type (str): Type of resource to list
            date (str, optional): Date in YYYY/MM/DD format
            
        Returns:
            List[str]: List of file paths
        """
        prefix = f"{resource_type}/"
        if date:
            prefix = f"{prefix}{date}/"
            
        blobs = self.bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]

    def get_latest_file(self, resource_type: str) -> str:
        """
        Get the path to the latest file for a resource type
        
        Args:
            resource_type (str): Type of resource
            
        Returns:
            str: Path to the latest file
        """
        blobs = self.bucket.list_blobs(prefix=f"{resource_type}/")
        latest_blob = None
        latest_timestamp = None
        
        for blob in blobs:
            if blob.metadata and 'created_at' in blob.metadata:
                timestamp = datetime.fromisoformat(blob.metadata['created_at'])
                if not latest_timestamp or timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                    latest_blob = blob
                    
        if latest_blob:
            return f"gs://{self.bucket.name}/{latest_blob.name}"
        return ""