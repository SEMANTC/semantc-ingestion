# src/loaders/gcs_loader.py

import os
from google.cloud import storage
from google.oauth2 import service_account

class GCSLoader:
    def __init__(self):
        # get gcs bucket name from environment variables
        self.bucket_name = os.getenv('GCS_BUCKET_NAME')
        # get path to gcp service account credentials
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME is not set in the environment variables")

        # initialize gcs client with credentials if provided
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = storage.Client(credentials=credentials)
        else:
            # assumes default credentials are set up (e.g., through gcloud auth application-default login)
            self.client = storage.Client()

    def upload_file(self, source_file_name, destination_blob_name):
        """uploads a file to the specified google cloud storage bucket"""
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)
            print(f"file {source_file_name} uploaded to {self.bucket_name}/{destination_blob_name}.")
        except Exception as e:
            print(f"failed to upload {source_file_name} to GCS: {e}")
            raise