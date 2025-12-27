# Data Engineering

This project implements a fully containerized data engineering workflow for ingesting, processing, validating, and preparing NYC Yellow Taxi Trip Data. It uses Apache Spark for computation, Apache Airflow for orchestration, MinIO as S3-compatible object storage and DuckDB as database for analytics. The environment is fully reproducible through Docker Compose.

---

## 1. Project Overview

This workflow simulates a modern data engineering architecture with dedicated layers:

1. **Local Layer** – Raw TIFF/TLC data downloaded locally.
2. **Landing Layer (MinIO/S3)** – Central storage for raw but structured data.
3. **Prepared Layer (MinIO/S3)** – Curated, transformed, analytics-ready data generated via Spark.
4. **Warehouse Layer (DuckDB)** – Analytical storage for ad-hoc analysis.
5. **Analytics Layer (JupyterLab)** – Exploration and further modeling.

The pipeline is orchestrated by Airflow and executed using Spark via containerized services.

---

## 2. Repository Structure

```
├── airflow
│   ├── dags
│   │   └── taxi_pipeline_dag.py
│   ├── Dockerfile
│   └── requirements.txt
├── config
│   └── storage_config.yaml
├── data
│   └── yellow_tripdata_2024-01.parquet
├── infra
│   └── docker
│       ├── notebook.Dockerfile
│       └── spark.Dockerfile
├── notebooks
│   └── 01_taxi_trips_exploration.ipynb
├── spark_jobs
│   ├── ingest_landing.py
│   └── transform_prepared.py
├── warehouse
│   └── taxi.duckdb
├── README.md
├── docker-compose.yml
├── LICENSE
└── requirements.txt
```

---

## 3. Dataset

This pipeline processes the **NYC Yellow Taxi Trip Records (January 2024)** dataset.

Download the dataset to a local `data/` directory:

```bash
mkdir -p data
curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" \
  -o data/yellow_tripdata_2024-01.parquet
```

The following table describes the key fields in the dataset:

| Field Name              | Description |
|-------------------------|-------------|
| **VendorID** | A code indicating the TPEP provider that provided the record. Possible values: Creative Mobile Technologies; VeriFone Inc. |
| **tpep_pickup_datetime** | The date and time when the meter was engaged. |
| **tpep_dropoff_datetime** | The date and time when the meter was disengaged. |
| **Passenger_count** | The number of passengers in the vehicle. This is a driver-entered value. |
| **Trip_distance** | The elapsed trip distance in miles reported by the taximeter. |
| **Pickup_longitude** | Longitude where the meter was engaged. |
| **Pickup_latitude** | Latitude where the meter was engaged. |
| **RateCodeID** | The final rate code in effect at the end of the trip. Possible values: Standard rate; JFK; Newark; Nassau or Westchester; Negotiated fare; Group ride. |
| **Store_and_fwd_flag** | Indicates whether the trip record was held in vehicle memory before sending to the vendor (“store and forward”). Values: Y = store and forward trip; N = not a store and forward trip. |
| **Dropoff_longitude** | Longitude where the meter was disengaged. |
| **Dropoff_latitude** | Latitude where the meter was disengaged. |
| **Payment_type** | A numeric code signifying how the passenger paid for the trip. Possible values: Credit card; Cash; No charge; Dispute; Unknown; Voided trip. |
| **Fare_amount** | The time-and-distance fare calculated by the meter. |
| **Extra** | Miscellaneous extras and surcharges, including the $0.50 and $1 rush hour and overnight charges. |
| **MTA_tax** | $0.50 MTA tax automatically triggered based on the metered rate in use. |
| **Improvement_surcharge** | $0.30 improvement surcharge assessed at the flag drop; introduced in 2015. |
| **Tip_amount** | Tip amount. Automatically populated for credit card tips; cash tips are not included. |
| **Tolls_amount** | Total amount of all tolls paid in the trip. |
| **Total_amount** | The total amount charged to passengers. Does not include cash tips. |

More data can be found in [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

---

## 4. System Architecture

### Component Overview

![Technologies](assets/stack.png)

| Component | Role |
|----------|------|
| Apache Airflow | Orchestrates the DAG that runs Spark ingestion and transformation. |
| Apache Spark | Executes ETL logic. |
| MinIO | Provides S3-compatible storage for Landing & Prepared layers. |
| DuckDB | Analytical storage for queries and notebook exploration. |
| JupyterLab | Notebook environment for analytics. |
| Docker Compose | Provides full reproducible environment. |

### High-Level Flow

```
Local → Spark (Ingest) → MinIO "Landing" → Spark (Transform) → MinIO "Prepared" → DuckDB → Jupyter
```

The data flows from local raw files into MinIO S3 storage (Landing), then transformed by Spark into a cleaned format (Prepared), and finally loaded into DuckDB for exploration and analysis using JupyterLab notebooks.

---

## 5. Data Layouts in MinIO

### Landing Zone
```
s3://<bucket>/landing/
    yellow_tripdata_2024-01/
        part-*.parquet
```

### Prepared Zone
```
s3://<bucket>/prepared/
    taxi_trips_clean/
        part-*.parquet
```

---

## 6. Docker Services

### Airflow
- Runs scheduler + webserver
- Airflow UI: `http://localhost:8080`

### Spark
- Executes ETL via:
  - `spark_jobs/ingest_landing.py`
  - `spark_jobs/transform_prepared.py`

### MinIO
- S3 endpoint: `http://localhost:9000`
- Console UI: `http://localhost:9001`

### JupyterLab Notebook
- Available at: `http://localhost:8888`
- Includes DuckDB and local workspace

---

## 7. Airflow Pipeline

### DAG: `taxi_spark_pipeline`
Located at `airflow/dags/taxi_pipeline_dag.py`.

#### **Tasks**
1. **ingest_to_landing**
   - Uses Spark to load local Parquet data and write into MinIO Landing.

2. **transform_to_prepared**
   - Applies transformation logic and writes cleaned output into Prepared.

#### Execution Mode
- Triggered manually
- No schedule (batch or experimental mode)

---

## 8. Spark Jobs

### ingest_landing.py
**Responsibilities:**
- Reads raw local Parquet data from `data/`
- Writes to Landing zone using S3A connector
- Ensures folder partitioning and consistency

### transform_prepared.py
**Responsibilities:**
- Reads Landing data
- Cleans and normalizes schema (types, missing values, invalid records)
- Writes analytics-ready Parquet output to Prepared

---

## 9. DuckDB Analytics Layer

### Warehouse
```
warehouse/taxi.duckdb
```

### Notebook
```
notebooks/01_taxi_trips_exploration.ipynb
```

Notebook includes:
- DuckDB queries
- EDA visualizations
- Schema inspection

---

## 10. Environment Variables

All secrets and configurations are handled via `.env` file.

As the projects run locally, the repository does include a sample `.env.example` file. Create your own `.env` based on it.

### Required Variables
```
MINIO_ROOT_USER=...
MINIO_ROOT_PASSWORD=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET_NAME=...
S3_ENDPOINT_URL=http://minio:9000
AIRFLOW_ADMIN_USER=...
AIRFLOW_ADMIN_PASSWORD=...
AIRFLOW_ADMIN_EMAIL=...
AIRFLOW_FERNET_KEY=...
TZ=UTC
```

---

## 11. Setup and Execution

### Step 1: Build services
```bash
docker compose build
```

### Step 2: Start system
```bash
docker compose up -d
```

### Step 3: Access UIs
| Service | URL |
|---------|-----|
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| JupyterLab | http://localhost:8888 |

### Step 4: Run the Pipeline
1. Open Airflow
2. Trigger the `taxi_spark_pipeline` DAG
3. Monitor Spark jobs

### Step 5: Explore Data
Open JupyterLab and run the EDA notebook.

---

## 12. Troubleshooting

### Containers not starting
Check logs:
```bash
docker compose logs <service>
```

### Airflow permission issues
Ensure Airflow home has correct ownership.

### Spark cannot access MinIO
- Verify S3 credentials in `.env`
- Confirm correct bucket name

---

## 13. Future Enhancements
- Add more data quality checks
- Add incremental ingestion using Airflow Variables
- Add CI/CD workflows
- Add dataset updates and versioning

---

## 14. License
This project is distributed under the terms defined in the `LICENSE` file.

---
## 15. Acknowledgements
- NYC Taxi & Limousine Commission for the dataset
- Apache Spark, Airflow, MinIO, DuckDB communities for their open-source tools
