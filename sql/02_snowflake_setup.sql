CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
    with WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS NYC_TAXI_DB;
USE DATABASE NYC_TAXI_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

USE WAREHOUSE NYC_TAXI_WH;
USE SCHEMA RAW;

CREATE STAGE IF NOT EXISTS S3_PROCESSED_STAGE
    URL = 's3://nyc-taxi-001/processed/'
    STORAGE_INTEGRATION = s3_taxi_integration
    FILE_FORMAT = (TYPE = PARQUET);

CREATE TABLE IF NOT EXISTS RAW.YELLOW_TAXI_2026_H1(
VendorID INTEGER,
tpep_pickup_datetime TIMESTAMP_NTZ,
tpep_dropoff_datetime TIMESTAMP_NTZ,
passenger_count INTEGER,
trip_distance FLOAT,
RatecodeID INTEGER,
store_and_fwd_flag STRING,
PULocationID INTEGER,
DOLocationID INTEGER,
payment_type INTEGER,
fare_amount FLOAT,
extra FLOAT,
mta_tax FLOAT,
tip_amount FLOAT,
tolls_amount FLOAT,
improvement_surcharge FLOAT,
total_amount  FLOAT,
congestion_surcharge FLOAT,
Airport_fee FLOAT,
cbd_congestion_fee FLOAT,
is_incomplete_metadata BOOLEAN,
payment_type_clean INTEGER,
ratecode_clean INTEGER,
trip_duration_minutes FLOAT,
pickup_hour INTEGER,
pickup_date DATE,
pickup_day_of_week STRING,
total_revenue FLOAT
);