import os
import argparse
from pyspark.sql import SparkSession

BUCKET = os.environ["S3_BUCKET_NAME"]
ENDPOINT = os.environ["S3_ENDPOINT_URL"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest local TLC Parquet into MinIO landing zone."
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year of the dataset, e.g. 2025",
    )
    parser.add_argument(
        "--month",
        type=int,
        required=True,
        help="Month of the dataset (1-12)",
    )
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


def main():
    args = parse_args()

    year = args.year
    month = args.month
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {month}. Must be between 1 and 12.")

    local_file, landing_path = build_paths(year, month)

    spark = get_spark()

    print(f"Reading local file: {local_file}")
    df = spark.read.parquet(local_file)

    print("Schema:")
    df.printSchema()
    print(f"Row count: {df.count()}")

    print(f"Writing to MinIO path: {landing_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(landing_path)
    )

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
