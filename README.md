# Project 06: Data Pipeline & Storage

This repository contains the implementation of **Project 06**, a data pipeline and storage solution designed to extract, process, and store data efficiently. The pipeline integrates MongoDB, Google Cloud Storage (GCS), and BigQuery, with automation and monitoring capabilities to ensure reliability and performance.

- **Start Date**: March 30, 2025
- **End Date**: April 14, 2025
- **Project ID**: `peppy-primacy-455413-d8`
- **Bucket**: `project-06-bucket`

## Table of Contents

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Prerequisites](#3-prerequisites)
4. [Setup Instructions](#4-setup-instructions)
   - [Authentication](#authentication)
   - [Virtual Machine Setup](#virtual-machine-setup)
   - [MongoDB Installation](#mongodb-installation)
   - [Data Upload and Extraction](#data-upload-and-extraction)
   - [MongoDB Data Import](#mongodb-data-import)
5. [Data Export Process](#5-data-export-process)
   - [Python Script for MongoDB to GCS](#python-script-for-mongodb-to-gcs)
   - [Error Handling and Logging](#error-handling-and-logging)
6. [BigQuery Integration](#6-bigquery-integration)
   - [Dataset and Table Creation](#dataset-and-table-creation)
   - [Schema Definition](#schema-definition)
   - [Automated Data Loading](#automated-data-loading)
7. [DBT for Data Type Conversion](#7-dbt-for-data-type-conversion)
   - [Setup dbt](#setup-dbt)
   - [DBT Model for Data Type Conversion](#dbt-model-for-data-type-conversion)
   - [Key Features of the DBT Model](#key-features-of-the-dbt-model)
   - [Run DBT](#run-dbt)
   - [Handling Common Issues](#handling-common-issues)
   - [Testing and Validation](#testing-and-validation)
   - [Troubleshooting](#troubleshooting)
8. [Testing and Monitoring](#8-testing-and-monitoring)
   - [End-to-End Testing](#end-to-end-testing)
   - [Alert Setup](#alert-setup)
9. [Troubleshooting](#9-troubleshooting)
10. [Contributing](#10-contributing)
11. [License](#11-license)


## 1) Project Overview 

The goal of **Project 06** is to build an end-to-end data pipeline that:

- Extracts data from MongoDB.
- Processes and exports it to Google Cloud Storage (GCS) in JSONL format.
- Loads the data into BigQuery for analysis.
- Implements monitoring and alerting to ensure pipeline reliability.

The pipeline handles data from `glamira_ubl_oct2019_nov2019.tar.gz` and `IP2LOCATION-LITE-DB5.IPV6.BIN`, processing it through a virtual machine (VM) and cloud services.

## 2) Architecture

The pipeline consists of the following components:

1. **MongoDB**: Stores the raw data imported from the provided dataset.
2. **Google Compute Engine (VM)**: Hosts MongoDB and runs the Python script for data export.
3. **Google Cloud Storage (GCS)**: Stores processed JSONL files in the `exports_jsonl/` folder.
4. **BigQuery**: Stores the final data in a structured table (`summary.table_jsonl`).
5. **Cloud Run**: Automates the loading of JSONL files from GCS to BigQuery.
6. **Cloud Monitoring**: Tracks pipeline performance and sends alerts for failures.

## 3) Prerequisites

- Google Cloud Platform (GCP) account with billing enabled.
- Project ID: `peppy-primacy-455413-d8`.
- Service Account with the following roles:
    - `roles/editor`
    - `roles/storage.admin`
    - `roles/storage.objectAdmin`
- Installed tools:
    - `gcloud` CLI
    - Python 3.10+
    - MongoDB 7.0
    - Docker (for Cloud Run deployment)

## 4) Setup Instructions

### Authentication

1. **Create a Service Account**:
    - Navigate to **IAM & Admin** > **Service Accounts** in GCP Console.
    - Create a new Service Account and assign roles: `roles/editor`, `roles/storage.admin`, `roles/storage.objectAdmin`.
    - Generate and download a JSON key file.
2. **Set Environment Variable**:
    
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-key.json"
    ```
    
3. **Install gcloud CL**
    
    ```python
    sudo apt update && sudo apt install google-cloud-sdk -y
    ```
    
4. **Activate Service Account**
    
    ```python
    gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
    ```
    
5. **Verify Authentication**:
    
    ```python
    gcloud projects list
    gcloud auth list
    ```
    

### **Virtual Machine Setup**

1. **Create a VM Instance**:
    - Go to **Compute Engine** > **VM Instances** in GCP Console.
    - Create a new instance with:
        - **OS**: Debian GNU/Linux 11 (Bullseye)
        - **Boot Disk**: Balanced persistent disk, 100 GB
        - **Firewall**: Allow HTTP traffic
    - Zone: us-central1-c
2. **Set Up SSH**:
    - Generate an SSH key:
        
        ```python
        ssh-keygen -t rsa -b 4096 -f ~/.ssh/my_gcp_key -C "hofonam"
        ```
        
    - Add the public key to VM metadata:
        
        ```python
        gcloud compute instances add-metadata instance-20250331-140931 \
            --metadata=ssh-keys="hofonam:$(cat ~/.ssh/my_gcp_key.pub)" \
            --zone=us-central1-c --project=peppy-primacy-455413-d8
        ```
        
    - Connect to the VM:
        
        ```python
        ssh -i ~/.ssh/my_gcp_key hofonam@104.197.248.234
        ```
        

### MongoDB Installation

1. **Update System**:

```bash
sudo apt update && sudo apt upgrade -y
```

1. **Add MongoDB GPG Key**:
    
    ```python
    wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | sudo apt-key add -
    ```
    
2. **Add MongoDB Repository**:
    
    ```python
    echo "deb https://repo.mongodb.org/apt/debian bullseye/mongodb-org/7.0 main" | 
    sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
    ```
    

1. **Install MongoDB**:
    
    ```python
    `sudo apt update
    sudo apt install -y mongodb-org`
    ```
    
2. **Start MongoDB**:
    
    ```python
    `sudo systemctl start mongod
    sudo systemctl enable mongod`
    ```
    
3. **Verify Installation**:
    
    ```python
    `mongod --version`
    ```
    

### Data Upload and Extraction

1. **Upload Data Files to VM**:
    
    ```python
    `scp -i ~/.ssh/my_gcp_key /path/to/glamira_ubl_oct2019_nov2019.tar.gz hofonam@104.197.248.234:~/
    scp -i ~/.ssh/my_gcp_key /path/to/IP-COUNTRY-REGION-CITY.BIN hofonam@104.197.248.234:~/`
    ```
    
2. **Extract Data**:
    
    ```python
    `mkdir -p extracted_data
    tar -xzvf glamira_ubl_oct2019_nov2019.tar.gz -C extracted_data/`
    ```
    

### MongoDB Data Import

1. **Import Data**:
    
    ```python
    `mongorestore --drop -d countly -c summary ~/extracted_data/dump/countly/summary.bson`
    ```
    

## 5) Data Export Process

### Python Script for MongoDB to GCS

The script (main.py) exports data from MongoDB to GCS in JSONL format:

- **Location**: mongo_to_gcs_export/main.py
- **Dependencies**:
    - pymongo
    - google-cloud-storage
    - pandas
    - pyarrow
- **Configuration**:
    - MongoDB URI, database, and collection.
    - GCS bucket name: project-06-bucket.
    - Batch size for processing.

Run the script on the VM:

```python
`python3 main.py`
```

### Error Handling and Logging

- Logs are written to mongo_to_gcs_export.log.
- Errors are captured in error_cart_products_batch_*.json.
- Handles schema inconsistencies (e.g., option, cart_products fields).

**Fixing Permissions Issue**:
If the Service Account lacks GCS write access:

```python
`gcloud projects add-iam-policy-binding peppy-primacy-455413-d8 \
    --member=serviceAccount:597720744281-compute@developer.gserviceaccount.com \
    --role=roles/storage.objectAdmin
gcloud projects add-iam-policy-binding peppy-primacy-455413-d8 \
    --member=serviceAccount:597720744281-compute@developer.gserviceaccount.com \
    --role=roles/storage.admin`
```

Update credentials:

```python
`gcloud auth application-default login`
```

## 6) BigQuery Integration

### Dataset and Table Creation

1. **Create Dataset**:
    - In BigQuery, select the project (peppy-primacy-455413-d8).
    - Click **Create Dataset**, name it summary.
2. **Create Table**:
    - Source: GCS (gs://project-06-bucket/exports_jsonl/*.jsonl)
    - Format: JSON (Newline delimited)
    - Destination: summary.table_jsonl
    - Schema: Auto-detect or use predefined schema.
    - Write Preference: Append

### Schema Definition

The table schema (schema.json) includes fields like:

- currency, price, _id, etc. (STRING)
- Nested option and cart_products (RECORD, REPEATED)
- Full schema available in the repository.

### Automated Data Loading

A Cloud Run service (bq-loader) loads JSONL files from GCS to BigQuery:

1. **Setup Cloud Run**:
    - Create a directory:
        
        ```python
        `mkdir bq-cloudrun-loader && cd bq-cloudrun-loader`
        ```
        
    - Add main.py, requirements.txt, schema.json, and Dockerfile.
2. **Build Container**:
    
    ```python
    `gcloud builds submit --tag gcr.io/peppy-primacy-455413-d8/bq-loader`
    ```
    
3. **Deploy**:
    
    ```python
    `gcloud run deploy bq-loader \
        --image gcr.io/peppy-primacy-455413-d8/bq-loader \
        --platform managed \
        --region us-central1 \
        --allow-unauthenticated`
    ```
    
4. **Run**:
    - Access the Cloud Run URL: https://bq-loader-597720744281.us-central1.run.app
    - Loads all JSONL files into summary.table_jsonl.

**Schema Mismatch Fix**:

- Converted all fields to STRING to avoid conflicts (e.g., user_id_db from INTEGER to STRING).

## 7) DBT for Data Type Conversion

To optimize the data in the BigQuery table (`summary.table_jsonl`) for efficient querying and analysis, we use **dbt (data build tool)** to transform column data types. The source table, loaded from JSONL files, initially stores most fields as `STRING` or other default types, which may not be ideal for analysis (e.g., `price` as `STRING` with values like `1.924.00`, `time_stamp` as `INTEGER`). The dbt model converts these columns to appropriate types such as `FLOAT64`, `TIMESTAMP`, `INTEGER`, and `DATETIME`, while handling issues like localized number formats and nested structures.

## Setup dbt

1. **Install dbt**:
    
    Install `dbt-bigquery` on your local machine or the VM:
    
    ```bash
    pip install dbt-bigquery
    
    ```
    
2. **Initialize dbt Project**:
    
    Create a new dbt project in your repository:
    
    ```bash
    dbt init dbt_bq_glamira_dataset
    cd dbt_bq_glamira_dataset
    
    ```
    
3. **Configure dbt**:
    - Update `profiles.yml` (located in `~/.dbt/`) to connect to your BigQuery project:
        
        ```yaml
        dbt_bq_glamira_dataset:
          target: dev
          outputs:
            dev:
              type: bigquery
              method: service-account
              project: peppy-primacy-455413-d8
              dataset: summary
              threads: 4
              keyfile: /path/to/your-key.json
        
        ```
        
    - Ensure the service account key (`/path/to/your-key.json`) has permissions for BigQuery (`roles/bigquery.admin`).
4. **Define Source**:
    
    Create a `sources.yml` file in the `models/` directory to reference the `summary.table_jsonl` table:
    
    ```yaml
    version: 2
    
    sources:
      - name: summary
        database: peppy-primacy-455413-d8
        schema: summary
        tables:
          - name: glamira_dataset
    ```
    

## DBT Model for Data Type Conversion

A dbt model (`models/example/change_data_type.sql`) transforms the data types of columns in `summary.table_jsonl`. The model handles:

- Converting `price` from `STRING` (e.g., `1.924.00`, `344,00`) to `FLOAT64` by removing thousands separators and standardizing decimal points.
- Converting `time_stamp` from `INTEGER` (Unix timestamp in seconds) to `TIMESTAMP`.
- Converting `local_time` from `STRING` to `DATETIME`.
- Converting identifier fields (e.g., `user_id_db`, `store_id`, `product_id`, `order_id`, `cat_id`, `collect_id`, `viewing_product_id`, `recommendation_product_id`, `option_id`, `value_id`) from `STRING` to `INTEGER`.
- Handling nested `REPEATED RECORD` fields (`option` and `cart_products`) with appropriate type conversions.

**Model Example** (`models/example/change_data_type.sql`)

## Key Features of the DBT Model

- **Safe Casting**: Uses `SAFE_CAST` to handle invalid values (e.g., non-numeric strings return `NULL`).
- **Price Cleaning**: Handles localized number formats (e.g., `1.924.00`, `344,00`) by removing thousands separators and standardizing decimal points.
- **Nested Structures**: Properly transforms nested `REPEATED RECORD` fields (`option`, `cart_products`) while maintaining their structure.
- **Performance Optimization**: Partitions the output table by `time_stamp` and clusters by `store_id` for efficient querying.

## Run DBT

1. **Test the Model**:
    
    Validate the SQL before running:
    
    ```bash
    dbt compile
    ```
    
    Check the compiled SQL in `target/compiled/dbt_bq_glamira_dataset/models/example/change_data_type.sql`.
    
2. **Execute the Model**:
    
    Run dbt to create the transformed table in BigQuery:
    
    ```bash
    dbt run
    ```
    
3. **Verify Output**:
    
    Check the new table (`summary.change_data_type`) in BigQuery to ensure columns have the correct data types (e.g., `price` as `FLOAT64`, `user_id_db` as `INTEGER`).
    

## Handling Common Issues

- **Invalid Number Formats**:
    - The `price` field may contain values like `1.924.00` or `344,00`. The model uses `REGEXP_REPLACE` to standardize these to `1924.00` or `344.00`.
    - If new formats appear, update the `REGEXP_REPLACE` pattern or add additional cleaning logic.
- **Timestamp Conversion**:
    - The `time_stamp` field (Unix timestamp in seconds) is converted to `TIMESTAMP` using `TIMESTAMP_SECONDS`. If milliseconds are used, switch to `TIMESTAMP_MILLIS`.
- **Non-Numeric IDs**:
    - If `user_id_db`, `store_id`, or other `INTEGER` fields contain non-numeric values (e.g., `"abc"`), `SAFE_CAST` returns `NULL`. To debug, query invalid values:
        
        ```sql
        SELECT user_id_db
        FROM summary.table_jsonl
        WHERE SAFE_CAST(user_id_db AS INTEGER) IS NULL
            AND user_id_db IS NOT NULL
        LIMIT 10;
        
        ```
        
- **Nested Field Issues**:
    - The `option` and `cart_products` fields are `REPEATED RECORD`. If `option_id` or `value_id` contain non-numeric values, validate with:
        
        ```sql
        SELECT o.option_id
        FROM summary.table_jsonl,
            UNNEST(option) AS o
        WHERE SAFE_CAST(o.option_id AS INTEGER) IS NULL
            AND o.option_id IS NOT NULL
        LIMIT 10;
        
        ```
        
- **Schema Evolution**:
    - If the source JSONL files change, update the dbt model to accommodate new fields or types. Use `dbt run --full-refresh` to rebuild the table.

## Testing and Validation

1. **Add dbt Tests**:
    
    Create a `schema.yml` file in `models/` to enforce data quality:
    
    ```yaml
    version: 2
    
    models:
      - name: change_data_type
        columns:
          - name: price
            tests:
              - not_null
              - dbt_utils.accepted_range:
                  min_value: 0
          - name: user_id_db
            tests:
              - not_null
          - name: time_stamp
            tests:
              - not_null
    
    ```
    
2. **Run Tests**:
    
    ```bash
    dbt test
    
    ```
    
3. **Monitor Output**:
    - Verify the transformed table in BigQuery.
    - Check for `NULL` values in `INTEGER` or `FLOAT64` columns to identify data issues.

## Troubleshooting

- **DBT Run Errors**:
    - Check logs in `logs/dbt.log` or use `dbt run --debug`.
    - Inspect compiled SQL in `target/compiled/` for errors.
- **Invalid Casts**:
    - If casting fails (e.g., `Bad double value` for `price`), update the cleaning logic in the model.
    - For `INTEGER` fields, ensure source data is numeric.

## 8) Testing and Monitoring

### End-to-End Testing

1. **Run Export Script**:
    - Execute main.py on the VM to upload data to gs://project-06-bucket/exports_jsonl/.
2. **Trigger Cloud Run**:
    - Access the Cloud Run URL to load data into BigQuery.
3. **Verify**:
    - Check BigQuery table summary.table_jsonl for data consistency.

### Alert Setup

1. **Create Alert Policy**:
    - Go to **Monitoring** > **Alerting** > **Create Policy**.
    - Metric: BigQuery Execution Time.
    - Filter: project_id = peppy-primacy-455413-d8.
    - Threshold: 10,000 ms.
    - Notification: Email or SMS.

## 9) Troubleshooting

- **Authentication Errors**:
    - Verify GOOGLE_APPLICATION_CREDENTIALS is set.
    - Ensure Service Account has required roles.
- **Schema Mismatch**:
    - Use STRING for all fields to avoid type conflicts.
    - Update schema in schema.json if needed.
- **GCS Upload Failure**:
    - Check Service Account permissions (roles/storage.objectAdmin).
- **Cloud Run Errors**:
    - Verify Docker image and dependencies in requirements.txt.

## 10) Contributing

Contributions are welcome! Please:

1. Fork the repository.
2. Create a feature branch (git checkout -b feature/your-feature).
3. Commit changes (git commit -m 'Add your feature').
4. Push to the branch (git push origin feature/your-feature).
5. Open a Pull Request.

## 11) License

This project is licensed under the MIT License - see the  file for details.

![gvto](https://github.com/user-attachments/assets/90910686-8a0a-4eb1-ac8b-22d488be7685)
