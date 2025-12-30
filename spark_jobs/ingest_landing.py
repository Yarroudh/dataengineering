import os
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

BUCKET = os.environ["S3_BUCKET_NAME"]
ENDPOINT = os.environ["S3_ENDPOINT_URL"]

LOCAL_ZONES_FILE = "/opt/de_project/data/taxi_zone_lookup.csv"
ZONES_LANDING_PATH = f"s3a://{BUCKET}/landing/reference/taxi_zones/"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest local TLC Parquet into MinIO landing zone."
    )
    parser.add_argument("--year", type=int, required=True, help="Year, e.g. 2024")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    return parser.parse_args()


def build_paths(year: int, month: int):
    """
    Build local file path and landing S3A path based on year/month.
    """
    local_file = f"/opt/de_project/data/yellow_tripdata_{year}-{month:02d}.parquet"
    landing_path = f"s3a://{BUCKET}/landing/taxi/year={year}/month={month:02d}/"
    return local_file, landing_path


def get_spark():
    return (
        SparkSession.builder
        .appName("TaxiIngestToLanding")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ingest_zones(spark: SparkSession):
    """
    Ingest taxi_zone_lookup.csv (dimension) to landing/reference/taxi_zones as Parquet.
    """
    if not os.path.exists(LOCAL_ZONES_FILE):
        raise FileNotFoundError(
            f"Zones file not found at {LOCAL_ZONES_FILE}. "
            "Ensure download task wrote it into /opt/de_project/data."
        )

    print(f"Reading zones CSV from: {LOCAL_ZONES_FILE}")
    zones = (
        spark.read
        .option("header", True)
        .csv(LOCAL_ZONES_FILE)
    )

    # Normalize schema
    zones = (
        zones
        .withColumn("LocationID", F.col("LocationID").cast(T.IntegerType()))
        .withColumn("Borough", F.col("Borough").cast(T.StringType()))
        .withColumn("Zone", F.col("Zone").cast(T.StringType()))
        .withColumn("service_zone", F.col("service_zone").cast(T.StringType()))
    )

    print(f"Zones row count: {zones.count()}")
    print(f"Writing zones to MinIO path: {ZONES_LANDING_PATH}")

    (
        zones.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(ZONES_LANDING_PATH)
    )

    print("Zones ingest done.")


def main():
    args = parse_args()

    year = args.year
    month = args.month
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {month}. Must be between 1 and 12.")

    local_file, landing_path = build_paths(year, month)

    spark = get_spark()

    # 1) Trips
    print(f"Reading local Parquet from: {local_file}")
    df = spark.read.parquet(local_file)

    print("Trip schema:")
    df.printSchema()
    print(f"Trip row count: {df.count()}")

    print(f"Writing trips to MinIO path: {landing_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(landing_path)
    )

    # 2) Zones (dimension)
    ingest_zones(spark)

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
