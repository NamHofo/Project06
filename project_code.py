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

    # Thử xuất file dưới dạng .jsonl nhưng gặp nhiều xung đột về schema
    #=> Chuyển sang xuất file parquet
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

    def process_batch_to_parquet(self, batch_data, batch_number):
        """Convert batch data to Parquet format, skipping rows with errors and logging problematic rows."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_batch_{batch_number}_{timestamp}.parquet"
            error_filename = f"error_cart_products_batch_{batch_number}_{timestamp}.json"

            processed_data = []
            error_data = []
            valid_count = 0

            # Xử lý từng dòng
            for doc in batch_data:
                try:
                    doc_copy = doc.copy()
                    if '_id' in doc_copy:
                        doc_copy['_id'] = str(doc_copy['_id'])

                    for field in ['utm_source', 'utm_medium', 'order_id', 'key_search', 'show_recommendation']:
                        if field in doc_copy:
                            doc_copy[field] = str(doc_copy[field]) if doc_copy[field] is not None else None

                    # Chuẩn hóa cart_products để đồng nhất kiểu dữ liệu
                    cart = doc_copy.get("cart_products", None)
                    if cart is None:
                        doc_copy["cart_products"] = []
                    elif not isinstance(cart, list):
                        doc_copy["cart_products"] = [str(cart)]  # Chuyển giá trị không phải list thành list chứa 1 phần tử
                    else:
                        cleaned_cart = []
                        for item in cart:
                            if isinstance(item, list):
                                cleaned_cart.extend([str(sub_item) if sub_item is not None else None for sub_item in item])
                            elif item is None:
                                cleaned_cart.append(None)
                            else:
                                cleaned_cart.append(str(item))  # Chuyển tất cả thành string để đồng nhất
                        doc_copy["cart_products"] = cleaned_cart

                    processed_data.append(doc_copy)
                    valid_count += 1

                except Exception as row_error:
                    logging.error(f"Error processing row with _id {doc.get('_id', 'Unknown ID')}: {str(row_error)}")
                    error_data.append({
                        "_id": str(doc.get("_id", "Unknown ID")),
                        "cart_products": doc.get("cart_products"),
                        "error": str(row_error)
                    })

            # Lưu lỗi nếu có
            if error_data:
                with open(error_filename, "w") as f:
                    json.dump(error_data, f, indent=4)
                logging.info(f"Saved {len(error_data)} errors to {error_filename}")

            # Xuất dữ liệu hợp lệ sang Parquet
            if processed_data:
                try:
                    df = pd.json_normalize(processed_data)
                    df.columns = [col.replace(".", "_") for col in df.columns]
                    df.to_parquet(filename, index=False)
                    logging.info(f"Processed {valid_count}/{len(batch_data)} documents in batch {batch_number}")
                    return filename, valid_count, len(batch_data)
                except Exception as parquet_error:
                    logging.error(f"Error converting batch {batch_number} to Parquet: {str(parquet_error)}")
                    return None, 0, len(batch_data)  # Đặt valid_count = 0 nếu lỗi xảy ra ở bước này
            else:
                logging.warning(f"No valid data to process in batch {batch_number}")
                return None, 0, len(batch_data)

        except Exception as e:
            logging.error(f"Unexpected error processing batch {batch_number}: {str(e)}")
            return None, 0, len(batch_data)



    def upload_to_gcs(self, bucket, filename):
            """Upload file to GCS"""
            try:
                blob = bucket.blob(f"exports_parquet/{filename}")
                blob.upload_from_filename(filename)
                logging.info(f"Successfully uploaded {filename} to GCS")
                os.remove(filename)  # Clean up local file
            except Exception as e:
                logging.error(f"Error uploading to GCS: {str(e)}")
                raise
    

    def export_to_gcs(self):
        """Main export function"""
        try:
            collection = self.connect_to_mongo()
            bucket = self.connect_to_gcs()
            total_docs = collection.estimated_document_count()
            logging.info(f"Total documents to process: {total_docs}")

            #Test với 100,000 docs
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

                filename, valid_count, total_count = self.process_batch_to_parquet(batch_data, batch_number)
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