import os
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

BUCKET = os.environ["S3_BUCKET_NAME"]
ENDPOINT = os.environ["S3_ENDPOINT_URL"]

ZONES_LANDING_PATH = f"s3a://{BUCKET}/landing/reference/taxi_zones/"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform landing data into prepared zone (MinIO S3)."
    )
    parser.add_argument("--year", type=int, required=True, help="Year, e.g. 2024")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    return parser.parse_args()


def build_paths(year: int, month: int):
    landing_trips_path = f"s3a://{BUCKET}/landing/taxi/year={year}/month={month:02d}/"
    prepared_path = f"s3a://{BUCKET}/prepared/taxi/year={year}/month={month:02d}/"
    return landing_trips_path, prepared_path


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


def base_transform(df):
    """
    Your existing cleaning + enrichment.
    """
    df = df.where(
        (F.col("tpep_pickup_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime").isNotNull())
        & (F.col("trip_distance") >= 0)
        & (F.col("total_amount") >= 0)
    )

    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    df = df.withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))

    df = df.withColumn(
        "trip_duration_min",
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        )
        / 60.0,
    )
    df = df.where(F.col("trip_duration_min") > 0)

    df = df.withColumn(
        "avg_mph",
        F.when(
            F.col("trip_duration_min") > 0,
            F.col("trip_distance") / (F.col("trip_duration_min") / 60.0),
        ).otherwise(None),
    )

    if "passenger_count" in df.columns:
        df = df.withColumn("passenger_count", F.col("passenger_count").cast(T.IntegerType()))
    if "payment_type" in df.columns:
        df = df.withColumn("payment_type", F.col("payment_type").cast(T.IntegerType()))
    if "RatecodeID" in df.columns:
        df = df.withColumn("RatecodeID", F.col("RatecodeID").cast(T.IntegerType()))

    return df


def add_zone_attributes(trips_df, zones_df):
    """
    Add pickup/dropoff zone attributes via two joins (PU and DO).
    """
    zones = (
        zones_df
        .select("LocationID", "Borough", "Zone", "service_zone")
        .withColumn("LocationID", F.col("LocationID").cast(T.IntegerType()))
    )

    # Build two aliased versions to avoid column collisions
    pu = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("PU_Borough"),
        F.col("Zone").alias("PU_Zone"),
        F.col("service_zone").alias("PU_service_zone"),
    )

    do = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("DO_Borough"),
        F.col("Zone").alias("DO_Zone"),
        F.col("service_zone").alias("DO_service_zone"),
    )

    # Zones are tiny -> broadcast joins
    trips_df = trips_df.join(F.broadcast(pu), on="PULocationID", how="left")
    trips_df = trips_df.join(F.broadcast(do), on="DOLocationID", how="left")

    return trips_df


def main():
    args = parse_args()

    year = args.year
    month = args.month
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {month}. Must be between 1 and 12.")

    landing_trips_path, prepared_path = build_paths(year, month)

    spark = get_spark()

    print(f"Reading landing trips from: {landing_trips_path}")
    trips = spark.read.parquet(landing_trips_path)

    print(f"Reading zones from: {ZONES_LANDING_PATH}")
    zones = spark.read.parquet(ZONES_LANDING_PATH)

    df_prep = base_transform(trips)
    df_prep = add_zone_attributes(df_prep, zones)

    print(f"Writing prepared data to: {prepared_path}")
    (
        df_prep
        .repartition("pickup_date")
        .write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(prepared_path)
    )

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
