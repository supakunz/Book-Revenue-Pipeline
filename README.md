
# 📘 Book Sales End-To-End Data Engineering Project on Google Cloud Platform

> End-to-end data engineering project using Docker, Airflow, Spark, BigQuery and Looker Studio on Google Cloud Platform (GCP)

## 🧾 Project Overview

This project demonstrates a complete modern data pipeline from raw ingestion to BI dashboard.  
It covers orchestration (Airflow), distributed processing (Spark), cloud storage/warehouse (GCS & BigQuery) and visualization (Looker Studio).

## ⚙️ Architecture Diagram
<img width="735" height="728" alt="Project Architecture" src="https://github.com/user-attachments/assets/e8006f93-541d-4606-a744-0d414731accb" />

## 💡 Technology Stack

**Programming Languages :**
- Python
- SQL

**Data Processing & Orchestration :**
- Apache Airflow
- Apache Spark

**Infrastructure & Cloud Platform :**
- Docker
- MinIO (S3)
- BigQuery

**Visualization :**
- Looker Studio

## 🐳 Docker / Infrastructure Setup

<img width="1191" height="836" alt="Image" src="https://github.com/user-attachments/assets/4a8b9b4b-a853-4eec-9aed-2fd9a0ab6405" />

**Services included :**
- `airflow-webserver`, `airflow-scheduler`
- `spark-master`, `spark-worker`
- `mysql`, `phpmyadmin`, `json-server`, etc.

## 💾 Data Lake Storage (MinIO)

<img width="1191" height="836" alt="Image" src="https://github.com/user-attachments/assets/4a8b9b4b-a853-4eec-9aed-2fd9a0ab6405" />

**Bucket Structure :**
- Uses **MinIO** as an S3-compatible object storage.
- All raw, processed, and output data from the pipeline is stored here before being loaded into the data warehouse.
- The `data-lake` bucket is the primary storage location for this project.

## ⚡ Spark Cluster (Master/Workers)

<img width="1920" height="995" alt="Image" src="https://github.com/user-attachments/assets/ad97e266-ba42-4ac3-98d7-2939df45a031" />

This cluster runs in standalone mode with :
- 1 Spark Master
- 1 Spark Workers
- Deployed inside Docker containers

## 📂 Raw Database

### 1. MySQL Tables
<img width="1907" height="995" alt="Image" src="https://github.com/user-attachments/assets/90299f12-7938-4328-86a6-957654183ac8" />

- `data_audible` : raw book sales data (user_id, country, price, rating, etc.)
- Data too large to include in repo.
- Download: [Google Drive Link](https://drive.google.com/...)

### 2. API Source (JSON)
```json
[
  {"date": "2021-04-01", "conversion_rate": 31.194, "id": "dc1b"},
  {"date": "2021-04-02", "conversion_rate": 31.29,  "id": "ac7a"},
  {"date": "2021-04-03", "conversion_rate": 31.256, "id": "b741"},
  {"date": "2021-04-04", "conversion_rate": 31.244, "id": "eaa3"},
  ...
]
```

- Conversion rate data fetched from API.
- File is included in repo at (server/conversion_rate.json).

## 🧾 Data Flow Diagram
<img width="1358" height="748" alt="Image" src="https://github.com/user-attachments/assets/10d9a52e-44a7-470d-9398-616d9b1371ff" />

## 🔄 ETL Workflow Diagram
<img width="1907" height="685" alt="Image" src="https://github.com/user-attachments/assets/36d07150-1e52-48e6-a22d-3b5818cac405" />

#### Spark ETL Components / Airflow Tasks

1. **Extract Stage:**
   - `extract_from_db` → extract data from database → save as parquet
   - `extract_from_api` → extract data from API → save as JSON

2. **Transform Stage:**
   - `db_clean_db` / `api_clean` → clean database & API data
   - `join_table` → join DB and API data
   - `handle_missing` / `final` → handle missing values and eda
   - `transform_data` → perform additional transformations / calculations

3. **Load Stage:**
   - `load_data` → push final tables to BigQuery

## ✅ Final Output
[<img src="https://github.com/user-attachments/assets/9f373252-43cd-43ac-970c-f262ea87e39d" width=70% height=70%>](https://lookerstudio.google.com/reporting/5737527d-e089-47f5-80f1-2adda4ff3019)
* The final output from Looker Studio can be accessed via the following link: [View Dashboard](https://lookerstudio.google.com/reporting/5737527d-e089-47f5-80f1-2adda4ff3019). Note: The dashboard reads data from a static CSV file exported from BigQuery.

## 🚀 Setup & Execution

1. Clone this repository :

```bash
git clone https://github.com/supakunz/Book-Revenue-Pipeline-GCP.git
```

2. Navigate to the project folder and Set up the environment variables :

```
cd Book-Revenue-Pipeline-GCP
```
- Create a `.env` file in the root directory.

- Add the following variables to the .env file, replacing the placeholder values with your own:

```
MYSQL_CONNECTION = mysql_default #file name in Data Storage --> <data_audible_data_merged.csv>
CONVERSION_RATE_URL = <your_api_url> #file name in Data Storage --> <data_conversion_rate.csv>
MYSQL_OUTPUT_PATH = /home/airflow/gcs/data/audible_data_merged.csv
CONVERSION_RATE_OUTPUT_PATH = /home/airflow/gcs/data/conversion_rate.csv
FINAL_OUTPUT_PATH = /home/airflow/gcs/data/output.csv
```

## 🙋‍♂️ Contact

Developed by **Supakun Thata**  
📧 Email: supakunt.thata@gmail.com  
🔗 GitHub: [SupakunZ](https://github.com/SupakunZ)

