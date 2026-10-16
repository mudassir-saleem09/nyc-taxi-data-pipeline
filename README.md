# 🚖 NYC Yellow Taxi — End-to-End Data Engineering Pipeline

An end-to-end, production-style data pipeline built on 6 months (Jan–Jun 2026) of NYC Yellow Taxi trip data — ~21.6 million rows — covering ingestion, distributed processing, cloud storage, a cloud data warehouse, analytics engineering, orchestration, and BI reporting.

## 📐 Architecture

```
NYC TLC Parquet Files (local download)
            │
            ▼
   AWS S3 (raw data lake)
            │
            ▼
   AWS Glue (PySpark) — cleaning, validation, derived columns
            │
            ▼
   AWS S3 (processed data, partitioned by month)
            │
            ▼
   Snowflake (Storage Integration + COPY INTO)
            │
            ▼
   dbt (staging → marts, medallion-style layering)
            │
            ▼
   Power BI Dashboard

   Apache Airflow (Docker) orchestrates: Glue job → Snowflake load → dbt run → dbt test
```

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Ingestion / local processing | Python, PySpark |
| Cloud storage (data lake) | AWS S3 |
| Cloud ETL | AWS Glue (managed Spark) |
| Data warehouse | Snowflake |
| Transformation / analytics engineering | dbt |
| Orchestration | Apache Airflow (Docker) |
| BI / Dashboard | Power BI |
| IAM / Security | AWS IAM Roles, Snowflake Storage Integration |

## 📊 What the pipeline does

1. **Ingests** 6 months of raw NYC Yellow Taxi trip data (~21.6M raw rows) into a partitioned S3 data lake.
2. **Cleans and transforms** the data with PySpark on AWS Glue: filters invalid records (negative fares, zero-distance trips, bad timestamps), standardizes categorical codes, and adds derived columns (trip duration, revenue, pickup hour/day).
3. **Loads** the cleaned data into Snowflake via a Storage Integration (S3 → Snowflake, no manual upload).
4. **Transforms** the warehouse data with dbt into a business-ready fact table (`fct_trips`), following a staging → marts layering pattern.
5. **Visualizes** the results in an interactive Power BI dashboard (trips by hour/day/month, revenue by payment type).
6. **Orchestrates** the entire flow with Airflow, so the pipeline can be re-run end-to-end with a single trigger.

## 🔍 Data Quality Highlights

During initial exploration, several data quality issues were identified and explicitly handled rather than blindly dropped:

- **~29% of rows** had a consistent block of missing fields (`RatecodeID`, `passenger_count`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`) that were always missing together and always paired with `payment_type = 0`. Rather than dropping ~29% of the data, these rows are **flagged** (`is_incomplete_metadata`) so downstream analysis can account for them.
- **39,463 rows** had a negative `fare_amount` — filtered out as invalid.
- **125,738 rows** had zero `trip_distance` — filtered out as invalid.
- Only **1 row** genuinely had a dropoff time before its pickup time (an initial exploration script had an inverted comparison that misreported this as ~3.6M rows — caught and corrected).
- `VendorID` values of `6` and `7` (in addition to the documented `1` and `2`) were found in the data and confirmed as valid, newer provider codes.

## 💰 Cost-Control Decisions

- Snowflake warehouse sized `XSMALL` with `AUTO_SUSPEND = 60` seconds.
- AWS Glue jobs run on the minimum practical worker configuration (`G.1X`, 2 workers) with a job timeout as a safety net.
- PySpark transformation logic was fully developed and tested **locally** before being ported to Glue, to avoid iterating (and paying) on the cloud.

## 🚀 Running the pipeline

> Requires: Python 3.11+, an AWS account, a Snowflake account, and Docker Desktop.

1. Clone the repo and create a virtual environment:
   ```
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Download the NYC Yellow Taxi Parquet files from the NYC TLC and upload them to your S3 raw-data location.
3. Configure your own AWS and Snowflake credentials.
4. Run the local exploration and transformation scripts under `src/`.
5. Set up the AWS S3 bucket, Glue job, and Snowflake objects using the SQL/scripts under `sql/` and `glue/`.
6. Run `dbt run` from the `nyc_taxi_dbt/` folder.
7. Start Airflow: `cd airflow && docker-compose up -d`, then trigger the `nyc_taxi_pipeline` DAG from the UI at `localhost:8081`.
8. A Power BI dashboard was built on top of the Snowflake mart layer. Dashboard screenshots are included in the documentation.

## Pipeline Execution Order

1. Upload raw NYC Taxi Parquet files to S3
2. Trigger AWS Glue PySpark job
3. Write cleaned data to processed S3
4. Load processed data into Snowflake
5. Run dbt transformations
6. Run dbt data quality tests
7. Consume marts in Power BI

## 📁 Project Structure

```
├── data/raw/              # Raw parquet files (not committed)
├── src/                   # Local exploration & PySpark scripts
├── glue/                  # AWS Glue job script
├── sql/                   # Snowflake setup, load, and fix scripts
├── nyc_taxi_dbt/          # dbt project (staging + mart models)
├── airflow/               # Airflow DAGs + Docker setup
└── powerbi/               # Power BI dashboard file
```
