import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# --- Glue job boilerplate ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- CHANGE THIS to your actual bucket name ---
BUCKET = "nyc-taxi-001"
RAW_PATH = f"s3://{BUCKET}/raw/yellow/year=2026/"
OUTPUT_PATH = f"s3://{BUCKET}/processed/yellow/year=2026/"

# --- 1. Read ALL 6 months at once ---
# Spark automatically reads the year=2026/month=XX partition structure
# and adds "year" and "month" as columns automatically (partition discovery)
df = spark.read.parquet(RAW_PATH)
print(f"Raw Row Count (all 6 months): {df.count()}")

# --- 2. Core filters (same logic as local PySpark script, validated on January data) ---
df = df.filter(
    (F.col("fare_amount") >= 0)
    & (F.col("trip_distance") > 0)
    & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    & (F.col("VendorID").isin([1, 2, 6, 7]))
)
print(f"Row Count After Core Filters: {df.count()}")

# --- 3. Flag the systematic missing-metadata block ---
df = df.withColumn(
    "is_incomplete_metadata",
    F.col("RatecodeID").isNull()
    & F.col("passenger_count").isNull()
    & F.col("store_and_fwd_flag").isNull()
    & F.col("congestion_surcharge").isNull()
    & F.col("Airport_fee").isNull()
)

# --- 4. payment_type cleanup ---
df = df.withColumn(
    "payment_type_clean",
    F.when(F.col("payment_type").isin([1, 2, 3, 4, 5, 6]), F.col("payment_type"))
     .otherwise(F.lit(0))
)

# --- 5. RatecodeID cleanup ---
df = df.withColumn(
    "ratecode_clean",
    F.when(F.col("RatecodeID").isin([1, 2, 3, 4, 5, 6]), F.col("RatecodeID"))
     .otherwise(F.lit(99))
)

# --- 6. passenger_count filter ---
df = df.filter(
    (F.col("passenger_count").isNull()) | (F.col("passenger_count") > 0)
)

# --- 7. Derived columns ---
df = df.withColumn(
    "trip_duration_minutes",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
).withColumn(
    "pickup_hour", F.hour("tpep_pickup_datetime")
).withColumn(
    "pickup_date", F.to_date("tpep_pickup_datetime")
).withColumn(
    "pickup_day_of_week", F.date_format("tpep_pickup_datetime", "EEEE")
).withColumn(
    "total_revenue",
    F.col("total_amount"))

# --- 8. Duration filter ---
df = df.filter(
    (F.col("trip_duration_minutes") > 0)
    & (F.col("trip_duration_minutes") < 300)
)
print(f"Row Count After Duration Filter: {df.count()}")

# --- 9. Write partitioned output back to S3, partitioned by month for query efficiency ---
df.write.mode("overwrite").partitionBy("month").parquet(OUTPUT_PATH)
print(f"Written processed data to: {OUTPUT_PATH}")

job.commit()
