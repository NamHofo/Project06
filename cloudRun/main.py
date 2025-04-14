import os
import json
from flask import Flask
from google.cloud import bigquery, storage

app = Flask(__name__)

BUCKET_NAME = "project-06-bucket"
EXPORT_FOLDER = "exports_jsonl/"
DATASET_NAME = "summary"
TABLE_NAME = "table_jsonl"
SCHEMA_PATH = "schema.json"  # Đường dẫn đến file schema.json

def load_schema_from_file(schema_path):
    with open(schema_path, "r") as f:
        schema_json = json.load(f)

    def parse_fields(fields):
        schema_fields = []
        for field in fields:
            if field["type"] == "RECORD":
                schema_fields.append(
                    bigquery.SchemaField(
                        name=field["name"],
                        field_type=field["type"],
                        mode=field.get("mode", "NULLABLE"),
                        fields=parse_fields(field["fields"])  # Đệ quy cho nested schema
                    )
                )
            else:
                schema_fields.append(
                    bigquery.SchemaField(
                        name=field["name"],
                        field_type=field["type"],
                        mode=field.get("mode", "NULLABLE")
                    )
                )
        return schema_fields

    return parse_fields(schema_json)

@app.route("/")
def load_all_files():
    # Kết nối GCS và liệt kê file
    client = storage.Client()
    blobs = client.list_blobs(BUCKET_NAME, prefix=EXPORT_FOLDER)
    uris = [f"gs://{BUCKET_NAME}/{blob.name}" for blob in blobs if blob.name.endswith(".jsonl")]

    if not uris:
        return "No JSONL files found.", 404
    # Load schema từ file
    schema = load_schema_from_file(SCHEMA_PATH)

    # Kết nối BigQuery và cấu hình job
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False
    )

    errors = []
    for uri in uris:
        try:
            load_job = bq_client.load_table_from_uri(
                uri, f"{DATASET_NAME}.{TABLE_NAME}", job_config=job_config
            )
            load_job.result()
        except Exception as e:
            errors.append(f"{uri}: {e}")

    if errors:
        return f"Completed with errors:\n" + "\n".join(errors), 500
    return f"Loaded {len(uris)} JSONL files successfully into {TABLE_NAME} table."

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
