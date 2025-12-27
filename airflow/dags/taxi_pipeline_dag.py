import os
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Paths / constants
PROJECT_ROOT = "/opt/de_project"

DATA_DIR = Path(PROJECT_ROOT) / "data"
WAREHOUSE_DIR = Path(PROJECT_ROOT) / "warehouse"
DUCKDB_PATH = WAREHOUSE_DIR / "taxi.duckdb"

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "de-lake")
S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]


# Helpers
def _get_year_month_from_context(context):
    """
    Resolve (year, month) from dag_run.conf if provided, otherwise from params.
    """
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {}

    default_year = int(context["params"].get("year", 2024))
    default_month = int(context["params"].get("month", 1))

    year = int(conf.get("year", default_year))
    month = int(conf.get("month", default_month))

    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {month}. Must be between 1 and 12.")
    return year, month


def download_taxi_data(**context):
    """
    Download raw TLC Parquet for the requested (year, month)
    into the shared local data folder.
    """
    # Local import to avoid DAG parse-time failure if package missing
    import requests

    year, month = _get_year_month_from_context(context)

    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target_path = DATA_DIR / filename

    print(f"Downloading {url} -> {target_path}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    with open(target_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    print(f"Download complete: {target_path}")


def load_to_duckdb(**context):
    """
    Load prepared data for a given (year, month) from MinIO into DuckDB,
    replacing whatever is currently in taxi.taxi.trips_prepared.

    Equivalent to:

        prepared_glob = "s3://de-lake/prepared/taxi/year=YYYY/month=MM/*/*.parquet"

        CREATE SCHEMA IF NOT EXISTS taxi.taxi;
        CREATE OR REPLACE TABLE taxi.taxi.trips_prepared AS
        SELECT * FROM read_parquet(prepared_glob);
    """
    # Local import to avoid DAG parse-time failure if package missing
    import duckdb

    year, month = _get_year_month_from_context(context)
    ym_str = f"{year}-{month:02d}"

    prepared_glob = (
        f"s3://{S3_BUCKET_NAME}/prepared/taxi/year={year}/month={month:02d}/*/*.parquet"
    )

    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"DuckDB DB path: {DUCKDB_PATH}")
    print(f"Overwriting taxi.taxi.trips_prepared with data from: {prepared_glob}")

    con = duckdb.connect(str(DUCKDB_PATH))

    # Configure DuckDB HTTPFS/S3 for MinIO
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    endpoint_no_scheme = (
        S3_ENDPOINT_URL.replace("http://", "").replace("https://", "")
    )

    con.execute("SET s3_endpoint = ?;", [endpoint_no_scheme])
    con.execute("SET s3_url_style = 'path';")
    con.execute("SET s3_use_ssl = false;")
    con.execute("SET s3_access_key_id = ?;", [os.environ["AWS_ACCESS_KEY_ID"]])
    con.execute("SET s3_secret_access_key = ?;", [os.environ["AWS_SECRET_ACCESS_KEY"]])

    # Same pattern you used in the notebook
    con.execute("CREATE SCHEMA IF NOT EXISTS taxi.taxi;")

    con.execute(
        """
        CREATE OR REPLACE TABLE taxi.taxi.trips_prepared AS
        SELECT *
        FROM read_parquet(?);
        """,
        [prepared_glob],
    )

    row_count = con.execute(
        "SELECT COUNT(*) FROM taxi.taxi.trips_prepared;"
    ).fetchone()[0]
    print(f"Rows in taxi.taxi.trips_prepared after load for {ym_str}: {row_count}")

    con.close()
    print("DuckDB load (overwrite) completed.")


# DAG definition
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Taxi pipeline: download -> local -> landing -> prepared -> duckdb",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,          # run manually
    catchup=False,
    tags=["taxi", "spark", "minio", "duckdb"],
    params={                # defaults if no config is passed when triggering
        "year": 2024,
        "month": 1,
    },
):

    # 1) Download raw Parquet from TLC for given year/month
    download_taxi_data_task = PythonOperator(
        task_id="download_taxi_data",
        python_callable=download_taxi_data,
        provide_context=True,
    )

    # 2) Ingest local Parquet -> MinIO landing (Spark)
    ingest_to_landing = BashOperator(
        task_id="ingest_to_landing",
        bash_command=(
            "docker exec de_spark "
            "spark-submit --master local[*] "
            "/opt/de_project/spark_jobs/ingest_landing.py "
            "--year {{ dag_run.conf.get('year', params.year) if dag_run else params.year }} "
            "--month {{ dag_run.conf.get('month', params.month) if dag_run else params.month }}"
        ),
    )

    # 3) Transform landing -> prepared (Spark)
    transform_to_prepared = BashOperator(
        task_id="transform_to_prepared",
        bash_command=(
            "docker exec de_spark "
            "spark-submit --master local[*] "
            "/opt/de_project/spark_jobs/transform_prepared.py "
            "--year {{ dag_run.conf.get('year', params.year) if dag_run else params.year }} "
            "--month {{ dag_run.conf.get('month', params.month) if dag_run else params.month }}"
        ),
    )

    # 4) Load prepared -> DuckDB 
    ingest_duckdb_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb,
        provide_context=True,
    )

    download_taxi_data_task >> ingest_to_landing >> transform_to_prepared >> ingest_duckdb_task
