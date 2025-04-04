from pymongo import MongoClient, errors
from google.cloud import storage
import pandas as pd
import logging
import json
from datetime import datetime
import os
from google.cloud.exceptions import GoogleCloudError


from config import mongo_uri, database, collection, bucket_name, batch_size

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='mongo_to_gcs_export.log'
)

class MongoToGCSExporter:
    def __init__(self, mongo_uri, 
                 database, 
                 collection,
                 bucket_name,
                 batch_size):
        self.mongo_uri = mongo_uri
        self.database = database
        self.collection = collection
        self.bucket_name = bucket_name
        self.batch_size = batch_size  

    def connect_to_mongo(self):
        """Establish MongoDB connection"""
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.database]
            collection = db[self.collection]
            logging.info("Successfully connected to MongoDB")
            return collection
        except errors.ServerSelectionTimeoutError as e:  
            logging.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def connect_to_gcs(self):
        """Initialize GCS client"""
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.bucket_name)
            logging.info("Successfully connected to GCS")
            return bucket
        except Exception as e:
            logging.error(f"Failed to connect to GCS: {str(e)}")
            raise

    def process_batch_to_jsonl(self, batch_data, batch_number):
        """Convert batch data to JSONL format"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_batch_{batch_number}_{timestamp}.jsonl"
            
            with open(filename, 'w') as f:
                for document in batch_data:
                    # Remove MongoDB's _id if not needed or convert to string
                    if '_id' in document:
                        document['_id'] = str(document['_id'])
                    f.write(json.dumps(document) + '\n')
                    
            return filename
        except Exception as e:
            logging.error(f"Error processing batch to JSONL: {str(e)}")
            raise

    def connect_to_gcs(self):
        """Initialize GCS client"""
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(self.bucket_name)
            logging.info("Successfully connected to GCS")
            return bucket
        except GoogleCloudError as e:
            logging.error(f"Failed to connect to GCS: {str(e)}")
            raise
    
    def upload_to_gcs(self, bucket, filename):
        """Upload file to GCS"""
        try:
            blob = bucket.blob(f"exports/{filename}")
            blob.upload_from_filename(filename)
            logging.info(f"Successfully uploaded {filename} to GCS")
            os.remove(filename)  # Clean up local file
        except Exception as e:
            logging.error(f"Error uploading to GCS: {str(e)}")
            raise
    

    def export_to_gcs(self):
        """Main export function"""
        try:
            # Connect to services
            collection = self.connect_to_mongo()
            bucket = self.connect_to_gcs()

            # Get total document count for progress tracking
            total_docs = collection.estimated_document_count()
            logging.info(f"Total documents to process: {total_docs}")

            # Limit to 10,000 documents for testing
            max_docs = 10000
            if total_docs > max_docs:
                logging.info(f"Limiting processing to the first {max_docs} documents for testing.")
                total_docs = max_docs

            # Process in batches
            batch_number = 0
            skip = 0

            while skip < total_docs:
                # Fetch batch
                batch_data = list(collection.find()
                                    .skip(skip)
                                    .limit(self.batch_size))
                
                if not batch_data:
                    break

                # Process and upload batch
                filename = self.process_batch_to_jsonl(batch_data, batch_number)
                self.upload_to_gcs(bucket, filename)

                # Update counters
                skip += self.batch_size
                batch_number += 1
                logging.info(f"Processed batch {batch_number} - {skip}/{total_docs} documents")

            logging.info("Export completed successfully")
            return True

        except Exception as e:
            logging.error(f"Export failed: {str(e)}")
            return False

def main():
    exporter = MongoToGCSExporter(
        mongo_uri = mongo_uri,
        database = database,
        collection = collection,
        bucket_name = bucket_name,
        batch_size = batch_size
    )
    exporter.export_to_gcs()

if __name__ == "__main__":
    main()