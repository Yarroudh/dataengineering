# Data Engineering

This project implements a fully containerized data engineering workflow for downloading, ingesting, transforming, validating, and preparing NYC Yellow Taxi Trip Data. It uses **Apache Spark** for computation, **Apache Airflow** for orchestration, **MinIO** as S3-compatible object storage, and **DuckDB** as an analytics warehouse. The entire environment is reproducible using Docker Compose.

---

## 1. Project Overview

This workflow simulates a modern data engineering architecture with dedicated layers:

1. **Local Layer** – Raw TLC data downloaded directly to the local filesystem.
2. **Landing Layer (MinIO/S3)** – Central storage for raw but structured data.
3. **Prepared Layer (MinIO/S3)** – Cleaned, transformed, analytics-ready datasets generated via Spark.
4. **Warehouse Layer (DuckDB)** – Analytical storage loaded from the Prepared layer.
5. **Analytics Layer (JupyterLab)** – Notebook-driven exploration and downstream modeling.

All pipeline steps are orchestrated using Airflow and executed within fully isolated containers.

---

## 2. Repository Structure

```bash
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
│   ├── 01_raw_data_exploration.ipynb
│   └── 02_prepared_data_exploration.ipynb
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

This pipeline processes **NYC Yellow Taxi Trip Records**.

The primary usage is **Airflow-driven download**, but you can still download data manually if needed.

### Manual Download (Optional)

```bash
mkdir -p data
curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" \
  -o data/yellow_tripdata_2024-01.parquet
```

Key fields include:

| Field Name            | Description                                                            |
| --------------------- | ---------------------------------------------------------------------- |
| VendorID              | Code for the TPEP provider (Creative Mobile Technologies or VeriFone). |
| tpep_pickup_datetime  | When the trip meter was started.                                       |
| tpep_dropoff_datetime | When the trip meter was stopped.                                       |
| Passenger_count       | Number of passengers (driver-entered).                                 |
| Trip_distance         | Distance in miles measured by the taximeter.                           |
| RateCodeID            | Final rate code (Standard, JFK, Newark, etc.).                         |
| Store_and_fwd_flag    | Whether the record was stored and forwarded later (“Y” or “N”).        |
| Payment_type          | Code for payment method (Credit card, Cash, Dispute, etc.).            |
| Fare_amount           | Meter-calculated fare.                                                 |
| Extra                 | Additional surcharges (rush-hour, etc.).                               |
| MTA_tax               | $0.50 MTA surcharge.                                                   |
| Improvement_surcharge | $0.30 surcharge at trip start.                                         |
| Tip_amount            | Tip (credit card only).                                                |
| Tolls_amount          | Total tolls.                                                           |
| Total_amount          | Total charged to the passenger (excluding cash tips).                  |

More details: [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## 4. System Architecture

### Component Overview

![Technologies](assets/stack.png)

| Component      | Responsibility                                                          |
| -------------- | ----------------------------------------------------------------------- |
| Airflow        | Orchestrates the ETL pipeline end-to-end.                               |
| Spark          | Performs ingestion and transformation into Landing and Prepared layers. |
| MinIO          | S3-compatible storage for Landing and Prepared zones.                   |
| DuckDB         | Analytical warehouse loaded from the Prepared layer.                    |
| JupyterLab     | Exploration and EDA environment.                                        |
| Docker Compose | Orchestrates and isolates the entire system.                            |

### High-Level Flow

```text
Airflow → Download → Local → Spark Ingest → MinIO Landing → Spark Transform → MinIO Prepared → DuckDB → Jupyter
```

---

## 5. Data Layouts in MinIO

### Landing Zone

```text
s3://<bucket>/landing/taxi/year=YYYY/month=MM/
    part-*.parquet
```

### Prepared Zone

```text
s3://<bucket>/prepared/taxi/year=YYYY/month=MM/
    pickup_date=YYYY-MM-DD/
        part-*.parquet
```

Both paths are dynamically parameterized with **year** and **month**, which are provided by Airflow at runtime.

---

## 6. Docker Services

### Airflow

* Web UI: `http://localhost:8080`
* Executes the `taxi_spark_pipeline` DAG.

### Spark

* Runs ingestion and transformation:

  * `spark_jobs/ingest_landing.py`
  * `spark_jobs/transform_prepared.py`

### MinIO

* S3 endpoint: `http://localhost:9000`
* Console UI: `http://localhost:9001`

### JupyterLab

* Available at: `http://localhost:8888`
* Includes DuckDB and local workspace.

---

## 7. Airflow Pipeline

### DAG: `taxi_spark_pipeline`

Location: `airflow/dags/taxi_pipeline_dag.py`

### Tasks

1. **download_taxi_data**

   * Downloads the TLC Parquet file for a given **year** / **month** into `data/`.
   * URL pattern:
     `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`

2. **ingest_to_landing**

   * Runs `ingest_landing.py` on Spark with `--year` and `--month`.
   * Reads local Parquet from `data/`.
   * Writes raw structured data into MinIO Landing:

     ```text
     s3a://<bucket>/landing/taxi/year=YYYY/month=MM/
     ```

3. **transform_to_prepared**

   * Runs `transform_prepared.py` on Spark with `--year` and `--month`.
   * Reads Landing data for the selected month.
   * Cleans and enriches the data.
   * Writes analytics-ready data into Prepared:

     ```text
     s3a://<bucket>/prepared/taxi/year=YYYY/month=MM/
     ```

4. **load_to_duckdb**

   * Uses DuckDB to read the Prepared Parquet for the selected month.

   * Creates schema and table if needed:

     ```sql
     CREATE SCHEMA IF NOT EXISTS taxi.taxi;
     CREATE OR REPLACE TABLE taxi.taxi.trips_prepared AS
     SELECT * FROM read_parquet('<prepared_glob>');
     ```

   * The `taxi.taxi.trips_prepared` table is **overwritten** for each pipeline run. After each run, it contains only the data for the triggered year/month.

### Triggering the DAG and Passing Parameters

The DAG is **manual only** (`schedule=None`).

In the Airflow UI:

1. Open the `taxi_spark_pipeline` DAG.
2. Click **Trigger DAG w/ config**.
3. Provide the run configuration as JSON:

```json
{
  "year": 2024,
  "month": 1
}
```

If `year`/`month` are omitted, the DAG falls back to its default params (`year=2024`, `month=1`).

---

## 8. Spark Jobs

### `spark_jobs/ingest_landing.py`

Responsibilities:

* Accepts CLI parameters: `--year` and `--month`.
* Builds input/output paths based on these values:

  * Local file: `/opt/de_project/data/yellow_tripdata_YYYY-MM.parquet`
  * Landing path: `s3a://<bucket>/landing/taxi/year=YYYY/month=MM/`
* Reads the Parquet file and writes to MinIO using the S3A connector.

### `spark_jobs/transform_prepared.py`

Responsibilities:

* Accepts `--year` and `--month`.
* Builds Landing and Prepared paths for the given month.
* Reads Landing data, then:

  * Filters invalid rows (e.g., negative distances, invalid timestamps).
  * Derives:

    * `pickup_date`, `pickup_hour`
    * `dropoff_date`
    * `trip_duration_min`
    * `avg_mph`
  * Casts core fields to appropriate types.
* Writes transformed data into Prepared, partitioned by `pickup_date`.

---

## 9. DuckDB Analytics Layer

### Warehouse

```text
warehouse/taxi.duckdb
```

The pipeline targets the table:

```sql
taxi.taxi.trips_prepared
```

Each Airflow run:

* Reads the Prepared Parquet for the configured year/month.
* Overwrites `taxi.taxi.trips_prepared` with the new data.

---

## 10. Notebooks

This project includes two Jupyter notebooks that support exploratory analysis of both the **raw TLC data** and the **prepared analytics-ready data**.

### 10.1 Raw Data Exploration

**File:** `notebooks/01_raw_data_exploration.ipynb`

* Inspects and validates the raw TLC Parquet file in `data/`.
* Uses **pandas**, including pandas built-in plotting (`DataFrame.plot` / `Series.plot`).
* Typical checks:

  * schema and dtypes
  * missing values
  * negative or unexpected values
  * outliers and distribution profiling
  * temporal patterns (e.g., by day/hour)

### 10.2 Prepared Data Exploration

**File:** `notebooks/02_prepared_data_exploration.ipynb`

* Analyzes curated data after Spark transformation and DuckDB load.
* Connects to `warehouse/taxi.duckdb` and queries `taxi.taxi.trips_prepared`.
* Includes SQL-based aggregations and pandas-based visualization.

---

## 11. Environment Variables

All secrets and configuration are driven through a `.env` file in the project root.

The repository ships an `.env.example` file as a starting point. You must create your own `.env` and fill in values, including `AIRFLOW_FERNET_KEY` (which is **not** committed).

### Required Variables

```text
MINIO_ROOT_USER=...
MINIO_ROOT_PASSWORD=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET_NAME=...
S3_ENDPOINT_URL=http://minio:9000

AIRFLOW_ADMIN_USER=...
AIRFLOW_ADMIN_PASSWORD=...
AIRFLOW_ADMIN_EMAIL=...

AIRFLOW_FERNET_KEY=...   # must be generated locally (see below)

TZ=UTC
```

---

## 12. Generating `AIRFLOW_FERNET_KEY`

Airflow uses a Fernet key to encrypt sensitive values (e.g., connections, variables). You must generate a key and set `AIRFLOW_FERNET_KEY` in your `.env`.

### Option 1: Generate using Docker (recommended)

From the project root:

```bash
docker compose run --rm airflow \
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the printed value and set it in `.env`:

```text
AIRFLOW_FERNET_KEY=<paste-generated-key-here>
```

### Option 2: Generate using local Python

If your host Python has `cryptography` installed:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 13. Setup and Execution

### Step 1 — Prepare environment

```bash
git clone <this-repo-url>
cd <this-repo>
cp .env.example .env
```

Edit `.env` and fill in:

* `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
* `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
* `S3_BUCKET_NAME`, `S3_ENDPOINT_URL`
* `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL`
* Generate and set `AIRFLOW_FERNET_KEY` (see section 12)

### Step 2 — Build services

```bash
docker compose build
```

### Step 3 — Start services

```bash
docker compose up -d
```

### Step 4 — Access UIs

| Service       | URL                                            |
| ------------- | ---------------------------------------------- |
| Airflow       | [http://localhost:8080](http://localhost:8080) |
| MinIO Console | [http://localhost:9001](http://localhost:9001) |
| JupyterLab    | [http://localhost:8888](http://localhost:8888) |

### Step 5 — Run the pipeline via Airflow

1. Open Airflow at `http://localhost:8080`.

2. Log in using the admin credentials from `.env`.

3. Open the `taxi_spark_pipeline` DAG.

4. Click **Trigger DAG w/ config**.

5. Provide year/month, for example:

   ```json
   {
     "year": 2024,
     "month": 1
   }
   ```

6. Monitor tasks:

   * `download_taxi_data`
   * `ingest_to_landing`
   * `transform_to_prepared`
   * `load_to_duckdb`

After a successful run, DuckDB (`warehouse/taxi.duckdb`) contains `taxi.taxi.trips_prepared` for the selected month.

### Step 6 — Explore data in JupyterLab

1. Open `http://localhost:8888`.
2. Run:

   * `notebooks/01_raw_data_exploration.ipynb` (raw profiling)
   * `notebooks/02_prepared_data_exploration.ipynb` (analytics-ready data)

---

## 14. Troubleshooting

### Containers not starting

```bash
docker compose logs <service>
```

### Airflow cannot parse the DAG

* Ensure Airflow dependencies include `duckdb` and `requests` in `airflow/requirements.txt`.
* Rebuild Airflow:

  ```bash
  docker compose build airflow
  docker compose up -d airflow
  ```

### Spark cannot access MinIO

* Verify `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`.
* Confirm the correct bucket name in `S3_BUCKET_NAME`.
* Ensure `S3_ENDPOINT_URL` is `http://minio:9000` (inside the Docker network).

---

## 15. Future Enhancements

* Add data quality checks (e.g., Great Expectations / Deequ).
* Implement incremental and multi-month ingestion.
* Add CI/CD workflows.
* Introduce dataset versioning and data catalog integration.

---

## 16. License

This project is distributed under the terms defined in the `LICENSE` file.

---

## 17. Acknowledgements

* NYC Taxi & Limousine Commission for the dataset.
* Apache Spark, Apache Airflow, MinIO, and DuckDB communities for their open-source tools.
