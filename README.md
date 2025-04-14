# Project 06: Data Pipeline & Storage

This repository contains the implementation of **Project 06**, a data pipeline and storage solution designed to extract, process, and store data efficiently. The pipeline integrates MongoDB, Google Cloud Storage (GCS), and BigQuery, with automation and monitoring capabilities to ensure reliability and performance.

- **Start Date**: March 30, 2025
- **End Date**: April 14, 2025
- **Project ID**: `peppy-primacy-455413-d8`
- **Bucket**: `project-06-bucket`

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Setup Instructions](#setup-instructions)
   - [Authentication](#authentication)
   - [Virtual Machine Setup](#virtual-machine-setup)
   - [MongoDB Installation](#mongodb-installation)
   - [Data Upload and Extraction](#data-upload-and-extraction)
   - [MongoDB Data Import](#mongodb-data-import)
5. [Data Export Process](#data-export-process)
   - [Python Script for MongoDB to GCS](#python-script-for-mongodb-to-gcs)
   - [Error Handling and Logging](#error-handling-and-logging)
6. [BigQuery Integration](#bigquery-integration)
   - [Dataset and Table Creation](#dataset-and-table-creation)
   - [Schema Definition](#schema-definition)
   - [Automated Data Loading](#automated-data-loading)
7. [Testing and Monitoring](#testing-and-monitoring)
   - [End-to-End Testing](#end-to-end-testing)
   - [Alert Setup](#alert-setup)
8. [Troubleshooting](#troubleshooting)
9. [Contributing](#contributing)
10. [License](#license)

## 1) Project Overview {#project-overview}

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

## 7) Testing and Monitoring

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

## 8) Troubleshooting

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

## 9) Contributing

Contributions are welcome! Please:

1. Fork the repository.
2. Create a feature branch (git checkout -b feature/your-feature).
3. Commit changes (git commit -m 'Add your feature').
4. Push to the branch (git push origin feature/your-feature).
5. Open a Pull Request.

## 10) License

This project is licensed under the MIT License - see the  file for details.

![gvto](https://github.com/user-attachments/assets/90910686-8a0a-4eb1-ac8b-22d488be7685)
