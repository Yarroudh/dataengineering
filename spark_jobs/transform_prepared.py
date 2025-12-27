import os
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

BUCKET = os.environ["S3_BUCKET_NAME"]
ENDPOINT = os.environ["S3_ENDPOINT_URL"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform landing data into prepared zone (MinIO S3)."
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year of the dataset, e.g. 2024",
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
    Build landing and prepared S3A paths based on year/month.
    """
    landing_path = f"s3a://{BUCKET}/landing/taxi/year={year}/month={month:02d}/"
    prepared_path = f"s3a://{BUCKET}/prepared/taxi/year={year}/month={month:02d}/"
    return landing_path, prepared_path


def get_spark():
    return (
        SparkSession.builder
        .appName("TaxiTransformToPrepared")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def transform(df):
    """
    Basic cleaning + enrichment for analytics and warehouse loading.
    """

    # Filter out obviously bad rows
    df = df.where(
        (F.col("tpep_pickup_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime").isNotNull())
        & (F.col("trip_distance") >= 0)
        & (F.col("total_amount") >= 0)
    )

    # Derive date & time features
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    df = df.withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))

    # Trip duration in minutes
    df = df.withColumn(
        "trip_duration_min",
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        )
        / 60.0,
    )

    # Filter out weird durations
    df = df.where(F.col("trip_duration_min") > 0)

    # Average speed
    df = df.withColumn(
        "avg_mph",
        F.when(
            F.col("trip_duration_min") > 0,
            F.col("trip_distance") / (F.col("trip_duration_min") / 60.0),
        ).otherwise(None),
    )

    # Cast few columns to more appropriate types (if columns exist)
    if "passenger_count" in df.columns:
        df = df.withColumn(
            "passenger_count", F.col("passenger_count").cast(T.IntegerType())
        )
    if "payment_type" in df.columns:
        df = df.withColumn(
            "payment_type", F.col("payment_type").cast(T.IntegerType())
        )
    if "RatecodeID" in df.columns:
        df = df.withColumn(
            "RatecodeID", F.col("RatecodeID").cast(T.IntegerType())
        )

    return df


def main():
    args = parse_args()

    year = args.year
    month = args.month
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {month}. Must be between 1 and 12.")

    landing_path, prepared_path = build_paths(year, month)

    spark = get_spark()

    print(f"Reading landing data from: {landing_path}")
    df = spark.read.parquet(landing_path)
    print("Landing schema:")
    df.printSchema()
    print(f"Landing row count: {df.count()}")

    df_prep = transform(df)

    print("Prepared schema:")
    df_prep.printSchema()
    print(f"Prepared row count: {df_prep.count()}")

    print(f"Writing prepared data to: {prepared_path}")
    (
        df_prep
        .repartition("pickup_date")  # simple partitioning by day
        .write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(prepared_path)
    )

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
