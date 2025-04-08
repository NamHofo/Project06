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
                        document['_id'] = string(document['_id'])
                    f.write(json.dumps(document) + '\n')
                    
            return filename
        except Exception as e:
            logging.error(f"Error processing batch to JSONL: {str(e)}")
            raise

    def process_batch_to_parquet(self, batch_data, batch_number):
        """Chuyển đổi dữ liệu batch sang định dạng Parquet, bỏ qua batch nếu gây lỗi"""
        try:
            # Tạo tên file với timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_batch_{batch_number}_{timestamp}.parquet"

            # Chuẩn hóa dữ liệu trước khi xử lý
            processed_data = []
            for doc in batch_data:
                # Tạo một bản sao của document
                doc_copy = doc.copy()

                # Chuyển _id thành chuỗi
                if '_id' in doc_copy:
                    doc_copy['_id'] = str(doc_copy['_id'])

                # Chuyển các trường cụ thể thành chuỗi
                for field in ['utm_source', 'utm_medium', 'order_id', 'key_search', 'show_recommendation']:
                    if field in doc_copy:
                        doc_copy[field] = str(doc_copy[field]) if doc_copy[field] is not None else None

                # Chuẩn hóa cart_products
                cart = doc_copy.get("cart_products", None)

                # Chuẩn hóa triệt để cart_products
                if not isinstance(cart, list):
                    if cart is None:
                        doc_copy["cart_products"] = []  # Nếu là None, đặt thành danh sách rỗng
                    elif isinstance(cart, dict):
                        doc_copy["cart_products"] = [cart]  # Nếu là dict, bọc trong danh sách
                    else:
                        # Nếu là kiểu dữ liệu khác (chuỗi, số, v.v.), chuyển thành danh sách rỗng
                        doc_copy["cart_products"] = []
                else:
                    # Nếu là danh sách, làm sạch và chuẩn hóa
                    cleaned_cart = []
                    for item in cart:
                        if isinstance(item, list):
                            # Nếu phần tử là danh sách lồng nhau, làm phẳng
                            for sub_item in item:
                                for sub_sub_item in item:
                                    if isinstance(sub_sub_item, (dict, str, int, float)):
                                        cleaned_cart.append(sub_item)
                                    else:
                                        cleaned_cart.append(None)
                        elif isinstance(item, (dict, str, int, float)):
                            cleaned_cart.append(item)
                        # Bỏ qua các kiểu dữ liệu khác
                    doc_copy["cart_products"] = cleaned_cart

                processed_data.append(doc_copy)

            # Thử chuyển toàn bộ batch thành DataFrame và xuất ra Parquet
            try:
                # Chuyển dữ liệu thành DataFrame
                df = pd.json_normalize(processed_data)

                # Thay dấu chấm trong tên cột bằng dấu gạch dưới
                df.columns = [col.replace(".", "_") for col in df.columns]

                # Chuẩn hóa các cột liên quan đến cart_products
                for col in df.columns:
                    if col.startswith("cart_products") and df[col].apply(lambda x: isinstance(x, list)).any():
                        df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])

                # Xuất ra file Parquet
                df.to_parquet(filename, index=False)

                # Ghi log thành công
                print(f"Đã xử lý {len(processed_data)} document hợp lệ trong batch {batch_number}.")
                return filename

            except Exception as e:
                # Nếu batch gây lỗi, ghi log và bỏ qua batch
                print(f"Bỏ qua batch {batch_number} do lỗi: {str(e)}")
                # Ghi log chi tiết giá trị cart_products để debug
                with open(f"error_cart_products_batch_{batch_number}_{timestamp}.json", "w") as f:
                    error_data = [{"_id": doc.get("_id", "Unknown ID"), "cart_products": doc.get("cart_products")} for doc in processed_data]
                    json.dump(error_data, f, indent=4)
                

        except Exception as e:
            print(f"Lỗi không mong muốn khi xử lý batch {batch_number}: {str(e)}")
            return None



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


        # """Upload file to GCS and save a local copy"""
        # try:
        #     # Define a local directory to save the file
        #     local_save_dir = "local_exports"
        #     os.makedirs(local_save_dir, exist_ok=True)  # Create the directory if it doesn't exist

        #     # Save a copy of the file locally
        #     local_file_path = os.path.join(local_save_dir, filename)
        #     os.rename(filename, local_file_path)  # Move the file to the local directory
        #     logging.info(f"Saved a local copy of {filename} to {local_file_path}")

        # except Exception as e:
        #     logging.error(f"Error uploading to GCS or saving locally: {str(e)}")
        #     raise
    

    def export_to_gcs(self):
        """Main export function"""
        try:
            # Connect to services
            collection = self.connect_to_mongo()
            bucket = self.connect_to_gcs()

            # Get total document count for progress tracking
            total_docs = collection.estimated_document_count()
            logging.info(f"Total documents to process: {total_docs}")

            # Limit to 100,000 documents for testing
            max_docs = 10000
            if total_docs > max_docs:
                logging.info(f"Limiting processing to the first {max_docs} documents for testing.")
                total_docs = max_docs

            # Process in batches
            batch_number = 0
            skip = 0

            while skip < total_docs:
                try:
                    # Fetch batch
                    batch_data = list(collection.find()
                                        .skip(skip)
                                        .limit(self.batch_size))
                    
                    if not batch_data:
                        break

                    # Process and upload batch
                    filename = self.process_batch_to_parquet(batch_data, batch_number)
                    self.upload_to_gcs(bucket, filename)

                    # Update counters
                    logging.info(f"Processed batch {batch_number} - {skip}/{total_docs} documents")
                except Exception as batch_error:
                    logging.error(f"Error processing batch {batch_number}: {str(batch_error)}")
                    logging.info(f"Skipping batch {batch_number} and continuing with the next batch.")

                # Update counters regardless of success or failure
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