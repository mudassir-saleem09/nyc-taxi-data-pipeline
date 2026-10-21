# 🚖 NYC Yellow Taxi — End-to-End Data Engineering Pipeline

An end-to-end data pipeline built on 6 months (Jan–Jun 2026) of NYC Yellow Taxi trip data — **22.8 million raw rows, ~21.66 million after cleaning** — covering ingestion, distributed processing, cloud storage, a cloud data warehouse, analytics engineering, orchestration, and BI reporting.

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
   Snowflake (Storage Integration + External Stage + COPY INTO)
            │
            ▼
   dbt (staging → marts layering)
            │
            ▼
   Power BI Dashboard

   Apache Airflow (Docker) orchestrates: Glue job → Snowflake load → dbt run → dbt test
```

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Local exploration / prototyping | Python, PySpark |
| Cloud storage (data lake) | AWS S3 |
| Cloud ETL | AWS Glue (managed Spark) |
| Data warehouse | Snowflake |
| Transformation / analytics engineering | dbt |
| Orchestration | Apache Airflow (Docker) |
| BI / Dashboard | Power BI |
| IAM / Security | AWS IAM Role, Snowflake Storage Integration (no access keys in code) |

## 📊 What the pipeline does

1. **Ingests** 6 months of raw NYC Yellow Taxi data (22.8M rows) into an S3 data lake. *(The download from NYC TLC and the upload to S3 are manual one-time steps; the automated pipeline starts at the Glue job.)*
2. **Cleans and transforms** the data with PySpark on AWS Glue: filters invalid records (negative fares, zero-distance trips, invalid vendors, bad timestamps, duration outliers), standardizes categorical codes, and adds derived columns (trip duration, revenue, pickup hour/day).
3. **Loads** the cleaned data into Snowflake through a Storage Integration and an external stage (S3 → Snowflake, no manual file upload).
4. **Transforms** the warehouse data with dbt into a business-ready fact table (`fct_trips`) using a staging → marts layering pattern, with dbt tests for data quality.
5. **Visualizes** the results in a Power BI dashboard (trips by hour / day / month, revenue trend, payment type split).
6. **Orchestrates** Glue → Snowflake load → `dbt run` → `dbt test` with Airflow, so the pipeline can be re-run with a single trigger.

## 🔍 Data Quality Highlights

During exploration, several data quality issues were identified and explicitly handled rather than blindly dropped:

- **~29% of rows** had a consistent block of missing fields (`RatecodeID`, `passenger_count`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`) that were always missing together and always paired with `payment_type = 0`. Rather than dropping that much data, these rows are **flagged** (`is_incomplete_metadata`) so downstream analysis can include or exclude them. In the dashboard they appear as the **"Unknown"** payment type.
- **39,463 rows** had a negative `fare_amount` — filtered out as invalid.
- **125,738 rows** had zero `trip_distance` — filtered out as invalid.
- Only **1 row** genuinely had a dropoff time before its pickup time (an early exploration script had an inverted comparison that misreported this as ~3.6M rows — caught and corrected).
- `VendorID` values `6` and `7` (in addition to the documented `1` and `2`) were found in the data and confirmed as valid, newer provider codes.

## 🧮 Row Count Reconciliation

Row counts were checked at every layer to make sure no data is silently lost or duplicated.

| Layer | Rows |
|---|---|
| Raw NYC TLC data (Jan–Jun 2026, 6 parquet files) | 22,836,530 |
| After Glue core filters | 21,734,498 |
| After duration filter (Glue output) | 21,656,435 |
| Snowflake `RAW.YELLOW_TAXI_2026_H1` | 21,656,435 |
| dbt mart `STAGING_MARTS.FCT_TRIPS` | 21,656,435 |

**1,180,095 rows (~5.2%) were removed as invalid:**
- 1,102,032 rows by the core filters (negative fares, zero distance, invalid vendors, bad timestamps)
- 78,063 rows by the trip-duration filter

The Glue output (21,656,435) matches the Snowflake raw table and the dbt mart exactly, so no rows are lost or duplicated between S3, Snowflake and dbt.

Raw rows per month:

| Month | Raw rows |
|---|---|
| Jan 2026 | 3,724,889 |
| Feb 2026 | 3,399,866 |
| Mar 2026 | 3,952,451 |
| Apr 2026 | 3,831,240 |
| May 2026 | 4,090,836 |
| Jun 2026 | 3,837,248 |


## 💰 Cost-Control Decisions

- Snowflake warehouse sized `XSMALL` with `AUTO_SUSPEND = 60` seconds.
- AWS Glue job runs on a minimal worker configuration (`G.1X`, 2 workers) with a job timeout as a safety net.
- PySpark transformation logic was developed and tested **locally** before being ported to Glue, to avoid iterating (and paying) on the cloud.

## 🚀 Setup & Running the Pipeline

> **Requirements:** Python 3.12+, an AWS account, a Snowflake account, Docker Desktop.
>
> Before you start, replace the placeholders for your own values: S3 bucket name (`nyc-taxi-001` in the Glue script and SQL files), AWS account ID / IAM role ARN in `sql/01_storage_integration.sql`, and AWS region (`ap-south-1`).

### 1. Local environment

```bash
python -m venv venv
venv\Scripts\activate        # Windows (use `source venv/bin/activate` on Linux/macOS)
pip install -r requirements.txt
```

### 2. AWS setup

1. Create the S3 bucket with `raw/` and `processed/` prefixes.
2. Create an IAM role for Glue (S3 access) and an IAM role for Snowflake (S3 read access to `processed/`).
3. Create the Glue job `nyc_taxi_glue_transform` (PySpark, `G.1X`, 2 workers) using `glue/glue_transform_script.py`.

### 3. Snowflake setup — run the SQL files **in this order**

| Order | File | What it does |
|---|---|---|
| 1 | `sql/01_storage_integration.sql` | Creates `s3_taxi_integration` (run as `ACCOUNTADMIN`) |
| — | *(manual step)* | Run `DESC INTEGRATION s3_taxi_integration;`, copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`, and paste them into the **trust policy** of the Snowflake IAM role in AWS. Without this, Snowflake cannot read S3. |
| 2 | `sql/02_snowflake_setup.sql` | Creates database, warehouse, schemas (`RAW`, `STAGING`, `MARTS`), the external stage `S3_PROCESSED_STAGE`, and the raw table |
| 3 | `sql/03_load_processed_data.sql` | `COPY INTO` the raw table from the stage (first load / manual load) |
| 4 | `sql/04_validate_data.sql` | Basic row-count and sample checks |

### 4. dbt

Create `~/.dbt/profiles.yml` with your Snowflake connection, then:

```bash
cd nyc_taxi_dbt
dbt run
dbt test
```

With the default profile, dbt builds the staging view in the `STAGING` schema and the mart `fct_trips` in `STAGING_MARTS` (dbt appends the custom schema name to the profile schema).

### 5. Airflow (Docker)

1. Create a file named `airflow/.env` (it is gitignored, so it is never committed) with the following content, using your own paths:

   ```
   AIRFLOW_UID=50000
   # These must be HOST machine paths (not paths inside the container)
   DBT_PROJECT_PATH=C:/path/to/nyc-taxi-data-pipeline/nyc_taxi_dbt
   DBT_PROFILES_PATH=C:/Users/<your-user>/.dbt
   ```

2. Start Airflow:

   ```bash
   docker-compose up -d
   ```

3. Open `http://localhost:8081` (default login `airflow` / `airflow`) and create two connections under **Admin → Connections**:
   - `aws_default` — AWS credentials / role with permission to run Glue jobs
   - `snowflake_default` — your Snowflake account, user, warehouse, database `NYC_TAXI_DB`, schema `RAW`
4. Unpause and trigger the `nyc_taxi_pipeline` DAG.

### Pipeline execution order

1. Upload raw NYC Taxi parquet files to S3 *(manual)*
2. Trigger AWS Glue PySpark job *(Airflow: `trigger_glue_job` + `wait_for_glue`)*
3. Glue writes cleaned data to processed S3, partitioned by month
4. Load processed data into Snowflake *(Airflow: `load_to_snowflake`)*
5. Run dbt transformations *(Airflow: `dbt_run`)*
6. Run dbt data quality tests *(Airflow: `dbt_test`)*
7. Consume marts in Power BI

## 📈 Dashboard

Built in Power BI on top of the Snowflake mart layer: total trips, total trip amount, average fare, average trip duration, monthly revenue trend, trips by pickup hour / day of week / month, and payment type split. Slicers for month and payment type.


## ⚠️ Known Limitations & Planned Improvements

Being upfront about what this project does *not* do yet:

- **Full reload, not incremental.** The Snowflake load truncates and reloads the whole table each run.
- **Manual ingestion.** Downloading from NYC TLC and uploading to S3 is not automated.
- **No dimension tables.** `fct_trips` has no `dim_zone` / `dim_date`, so there is no borough-level analysis yet.
- **Money columns are `FLOAT`.** Planned: `NUMBER(10,2)`.
- **`fct_trips` is a view by default.** Planned: materialize as a table for faster BI queries.
- **No outlier caps** on fare and distance beyond the trip-duration filter.
- **Local-only Airflow** (Docker socket mount, default credentials) — not production-hardened.

## 📁 Project Structure

```
├── data/raw/              # Raw parquet files (not committed)
├── src/                   # Local exploration & PySpark prototyping scripts
├── glue/                  # AWS Glue job script
├── sql/                   # Snowflake setup, load, and validation scripts (numbered)
├── nyc_taxi_dbt/          # dbt project (staging + mart models + tests)
├── airflow/               # Airflow DAG + docker-compose setup
└── powerbi/               # Dashboard screenshot
```