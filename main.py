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
    def __init__(self, mongo_uri, database, collection, bucket_name, batch_size):
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

                                option = doc_copy.get("option", None)
                                if option is None:
                                    doc_copy["option"] = []
                                elif not isinstance(option, list):
                                    doc_copy["option"] = [{
                                        "option_id": str(option) if option else None,
                                        "option_label": None,
                                        "quality": None,
                                        "quality_label": None,
                                        "value_id": None,
                                        "value_label": None,
                                        "alloy": None,
                                        "diamond": None,
                                        "shapediamond": None
                                    }]
                                else:
                                    cleaned_option = []
                                    for item in option:
                                        if isinstance(item, dict):
                                            cleaned_option.append({
                                                "option_id": str(item.get("option_id")) if item.get("option_id") is not None else None,
                                                "option_label": str(item.get("option_label")) if item.get("option_label") is not None else None,
                                                "quality": str(item.get("quality")) if item.get("quality") is not None else None,
                                                "quality_label": str(item.get("quality_label")) if item.get("quality_label") is not None else None,
                                                "value_id": str(item.get("value_id")) if item.get("value_id") is not None else None,
                                                "value_label": str(item.get("value_label")) if item.get("value_label") is not None else None,
                                                "alloy": str(item.get("alloy")) if item.get("alloy") is not None else None,
                                                "diamond": str(item.get("diamond")) if item.get("diamond") is not None else None,
                                                "shapediamond": str(item.get("shapediamond")) if item.get("shapediamond") is not None else None
                                            })
                                        else:
                                            continue
                                    doc_copy["option"] = cleaned_option

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




    def process_batch_to_parquet(self, batch_data, batch_number):
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"export_batch_{batch_number}_{timestamp}.parquet"
            error_filename = f"error_cart_products_batch_{batch_number}_{timestamp}.json"

            processed_data = []
            error_data = []
            valid_count = 0

            for doc in batch_data:
                try:
                    doc_copy = doc.copy()

                    # Stringify all top-level fields
                    for field in doc_copy:
                        if field not in ['option', 'cart_products']:  # Exclude RECORD fields
                            doc_copy[field] = str(doc_copy[field]) if doc_copy[field] is not None else None

                    # Process option
                    option = doc_copy.get("option", None)
                    if option is None:
                        doc_copy["option"] = []
                    elif not isinstance(option, list):
                        doc_copy["option"] = [{
                            "option_id": str(option) if option else None,
                            "option_label": None,
                            "quality": None,
                            "quality_label": None,
                            "value_id": None,
                            "value_label": None,
                            "alloy": None,
                            "diamond": None,
                            "shapediamond": None
                        }]
                    else:
                        cleaned_option = []
                        for item in option:
                            if isinstance(item, dict):
                                cleaned_option.append({
                                    "option_id": str(item.get("option_id")) if item.get("option_id") is not None else None,
                                    "option_label": str(item.get("option_label")) if item.get("option_label") is not None else None,
                                    "quality": str(item.get("quality")) if item.get("quality") is not None else None,
                                    "quality_label": str(item.get("quality_label")) if item.get("quality_label") is not None else None,
                                    "value_id": str(item.get("value_id")) if item.get("value_id") is not None else None,
                                    "value_label": str(item.get("value_label")) if item.get("value_label") is not None else None,
                                    "alloy": str(item.get("alloy")) if item.get("alloy") is not None else None,
                                    "diamond": str(item.get("diamond")) if item.get("diamond") is not None else None,
                                    "shapediamond": str(item.get("shapediamond")) if item.get("shapediamond") is not None else None
                                })
                            else:
                                continue
                        doc_copy["option"] = cleaned_option

                    # Process cart_products
                    cart = doc_copy.get("cart_products", None)
                    if cart is None:
                        doc_copy["cart_products"] = []
                    elif not isinstance(cart, list):
                        if isinstance(cart, (int, str)) and str(cart).replace(".", "").isdigit():
                            doc_copy["cart_products"] = [{
                                "product_id": str(cart),
                                "amount": "1",
                                "price": None,
                                "currency": None,
                                "option": []
                            }]
                        elif isinstance(cart, str) and "," in cart:
                            product_ids = [pid.strip() for pid in cart.split(",") if pid.strip().replace(".", "").isdigit()]
                            doc_copy["cart_products"] = [
                                {"product_id": str(pid), "amount": "1", "price": None, "currency": None, "option": []}
                                for pid in product_ids
                            ]
                        else:
                            doc_copy["cart_products"] = []
                    else:
                        cleaned_cart = []
                        for item in cart:
                            if isinstance(item, dict):
                                product_id = item.get("product_id")
                                amount = item.get("amount", 1)
                                price = item.get("price")
                                currency = item.get("currency")
                                cart_option = item.get("option", [])
                                if not isinstance(cart_option, list):
                                    cart_option = []
                                cleaned_option = []
                                for opt in cart_option:
                                    if isinstance(opt, dict):
                                        cleaned_option.append({
                                            "option_id": str(opt.get("option_id")) if opt.get("option_id") is not None else None,
                                            "option_label": str(opt.get("option_label")) if opt.get("option_label") is not None else None,
                                            "quality": str(opt.get("quality")) if opt.get("quality") is not None else None,
                                            "quality_label": str(opt.get("quality_label")) if opt.get("quality_label") is not None else None,
                                            "value_id": str(opt.get("value_id")) if opt.get("value_id") is not None else None,
                                            "value_label": str(opt.get("value_label")) if opt.get("value_label") is not None else None,
                                            "alloy": str(opt.get("alloy")) if opt.get("alloy") is not None else None,
                                            "diamond": str(opt.get("diamond")) if opt.get("diamond") is not None else None,
                                            "shapediamond": str(opt.get("shapediamond")) if opt.get("shapediamond") is not None else None
                                        })
                                    else:
                                        continue
                                cleaned_cart.append({
                                    "product_id": str(product_id) if product_id is not None else None,
                                    "amount": str(amount) if amount is not None else "1",
                                    "price": str(price) if price is not None else None,
                                    "currency": str(currency) if currency is not None else None,
                                    "option": cleaned_option
                                })
                            elif isinstance(item, (int, str)) and str(item).replace(".", "").isdigit():
                                cleaned_cart.append({
                                    "product_id": str(item),
                                    "amount": "1",
                                    "price": None,
                                    "currency": None,
                                    "option": []
                                })
                            elif isinstance(item, str) and "," in item:
                                product_ids = [pid.strip() for pid in item.split(",") if pid.strip().replace(".", "").isdigit()]
                                cleaned_cart.extend([
                                    {"product_id": str(pid), "amount": "1", "price": None, "currency": None, "option": []}
                                    for pid in product_ids
                                ])
                            else:
                                continue
                        doc_copy["cart_products"] = cleaned_cart

                    processed_data.append(doc_copy)
                    valid_count += 1

                except Exception as row_error:
                    logging.error(f"Error processing row with _id {doc.get('_id', 'Unknown ID')}: {str(row_error)}")
                    error_data.append({
                        "_id": str(doc.get("_id", "Unknown ID")),
                        "cart_products": doc.get("cart_products"),
                        "option": doc.get("option"),
                        "error": str(row_error)
                    })

            if error_data:
                with open(error_filename, "w") as f:
                    json.dump(error_data, f, indent=4)
                logging.info(f"Saved {len(error_data)} errors to {error_filename}")

            if processed_data:
                try:
                    df = pd.json_normalize(processed_data)
                    df.columns = [col.replace(".", "_") for col in df.columns]
                    # Ensure all columns are strings
                    for col in df.columns:
                        df[col] = df[col].astype(str).replace("nan", None)
                    df.to_parquet(filename, index=False)
                    logging.info(f"Processed {valid_count}/{len(batch_data)} documents in batch {batch_number}")
                    return filename, valid_count, len(batch_data)
                except Exception as parquet_error:
                    logging.error(f"Error converting batch {batch_number} to Parquet: {str(parquet_error)}")
                    return None, 0, len(batch_data)
            else:
                logging.warning(f"No valid data to process in batch {batch_number}")
                return None, 0, len(batch_data)

        except Exception as e:
            logging.error(f"Unexpected error processing batch {batch_number}: {str(e)}")
            return None, 0, len(batch_data)





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