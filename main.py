from pymongo import MongoClient, errors
from google.cloud import storage
import pandas as pd
import logging
import json
from datetime import datetime
import os
from google.cloud.exceptions import GoogleCloudError
import pyarrow

from config import mongo_uri, database, collection, bucket_name, batch_size

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='mongo_to_gcs_export.log'
)


class MongoToGCSExporter:
    #Initializes the MongoToGCSExporter class with MongoDB connection details, GCS bucket name, and batch size.
    def __init__(self, mongo_uri, database, collection, bucket_name, batch_size):
        self.mongo_uri = mongo_uri
        self.database = database
        self.collection = collection
        self.bucket_name = bucket_name
        self.batch_size = batch_size  

    # Establishes a connection to the MongoDB database and returns the specified collection.
    def connect_to_mongo(self):
        """Establish MongoDB connection"""
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.database]
            collection = db[self.collection]
            logging.info("Successfully connected to MongoDB")  # Updated log message
            return collection
        except errors.ServerSelectionTimeoutError as e:  
            logging.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    #: Initializes a connection to Google Cloud Storage and returns the specified bucket.
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

    #Processes a batch of MongoDB documents, converts them to JSONL format with all fields as strings, 
    # ensures specific fields like price and option are handled properly, and logs errors for problematic rows.
    def process_batch_to_jsonl(self, batch_data, batch_number):
        """Convert batch data to JSONL format with all fields as strings, ensuring price is always present"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_batch_{batch_number}_{timestamp}.jsonl"
            error_filename = f"error_cart_products_batch_{batch_number}_{timestamp}.json"
            logging.basicConfig(filename=f"process_batch_{batch_number}_{timestamp}.log", level=logging.DEBUG)

            processed_count = 0
            error_data = []
            row_number = 0

            with open(filename, 'w', encoding='utf-8') as f:
                for doc in batch_data:
                    row_number += 1
                    try:
                        doc_copy = doc.copy()
                        logging.debug(f"Row {row_number} _id {doc.get('_id')}: Original cart_products: {doc_copy.get('cart_products')}")

                        # Stringify all top-level fields (except option and cart_products)
                        for key in list(doc_copy.keys()):
                            if key not in ["option", "cart_products"]:
                                if isinstance(doc_copy[key], str) and doc_copy[key].lower() in ["true", "false"]:
                                    doc_copy[key] = doc_copy[key].lower() == "true"
                                else:
                                    doc_copy[key] = str(doc_copy[key]) if doc_copy[key] is not None else None
                            else:
                                if "cart_products" in doc:
                                    cart_products = []
                                    for cart_product in doc.get("cart_products", []):
                                        if cart_product.get("option") == "":
                                            cart_product["option"] = []
                                        if "price" in cart_product and cart_product["price"] is not None:
                                            cart_product["price"] = str(cart_product["price"])
                                        cart_products.append(cart_product)
                                    doc_copy["cart_products"] = cart_products

                                if "option" in doc:
                                    option = []
                                    if doc.get("option") == "":
                                        doc_copy["option"] = []

                        # Write to JSONL
                        f.write(json.dumps(doc_copy, ensure_ascii=False) + '\n')
                        processed_count += 1

                    except Exception as row_error:
                        logging.error(f"Row {row_number} _id {doc.get('_id', 'Unknown ID')}: Error processing row: {str(row_error)}")
                        error_data.append({
                            "_id": str(doc.get("_id", "Unknown ID")),
                            "cart_products": doc.get("cart_products"),
                            "option": doc.get("option"),
                            "error": str(row_error)
                        })

            if error_data:
                with open(error_filename, "w", encoding='utf-8') as f:
                    json.dump(error_data, f, indent=4)
                logging.info(f"Saved {len(error_data)} errors to {error_filename}")

            logging.info(f"Processed {processed_count}/{len(batch_data)} documents in batch {batch_number}")
            return filename, processed_count, len(batch_data)

        except Exception as e:
            logging.error(f"Unexpected error processing batch {batch_number}: {str(e)}")
            return None, 0, len(batch_data)

    # Uploads a local file to the specified GCS bucket and removes the local file after successful upload.
    def upload_to_gcs(self, bucket, filename):
        """Upload file to GCS"""
        try:
            blob = bucket.blob(f"exports_jsonl/{filename}")
            blob.upload_from_filename(filename)
            logging.info(f"Successfully uploaded {filename} to GCS")
            os.remove(filename)  # Clean up local file
        except Exception as e:
            logging.error(f"Error uploading to GCS: {str(e)}")
            raise

    #The main function that orchestrates the export process by fetching data from MongoDB in batches, processing it into JSONL format, and uploading it to GCS.
    def export_to_gcs(self):
        """Main export function"""
        try:
            collection = self.connect_to_mongo()
            bucket = self.connect_to_gcs()
            total_docs = collection.estimated_document_count()
            logging.info(f"Total documents to process: {total_docs}")

            # Test với 100,000 docs
            max_docs = 100000
            if total_docs > max_docs:
                logging.info(f"Limiting processing to the first {max_docs} documents for testing.")
                total_docs = max_docs

            batch_number = 0
            skip = 0

            while skip < total_docs:
                batch_data = list(collection.find().skip(skip).limit(self.batch_size))
                if not batch_data:
                    break

                filename, valid_count, total_count = self.process_batch_to_jsonl(batch_data, batch_number)
                if filename and valid_count > 0:
                    self.upload_to_gcs(bucket, filename)
                    logging.info(f"Uploaded batch {batch_number} to GCS with {valid_count}/{total_count} rows.")
                else:
                    logging.warning(f"Batch {batch_number} skipped due to errors or no valid data ({valid_count}/{total_count} rows).")
                    error_count = total_count - valid_count
                    if error_count > 0:
                        logging.info(f"Batch {batch_number}: {error_count} rows failed, details saved in error file.")

                skip += self.batch_size
                batch_number += 1

            logging.info("Export completed successfully")
            return True

        except Exception as e:
            logging.error(f"Export failed: {str(e)}")
            return False
# Entry point of the script that initializes the MongoToGCSExporter class and starts the export process.
def main():
    exporter = MongoToGCSExporter(
        mongo_uri=mongo_uri,
        database=database,
        collection=collection,
        bucket_name=bucket_name,
        batch_size=batch_size
    )
    exporter.export_to_gcs()

if __name__ == "__main__":
    main()