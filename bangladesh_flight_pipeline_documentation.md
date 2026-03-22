# Bangladesh Flight Price Data Pipeline — Technical Documentation

**Project:** End-to-End Flight Price Analytics Pipeline
**Technologies:** Apache Airflow · MySQL · PostgreSQL · Python · Docker
**Data Source:** Flight Price Dataset of Bangladesh (Kaggle)
**Author:** Data Engineering Team
**Date:** March 2026

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Pipeline Architecture](#2-pipeline-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Environment Setup](#4-environment-setup)
5. [Airflow DAG & Task Descriptions](#5-airflow-dag--task-descriptions)
6. [Data Schema](#6-data-schema)
7. [KPI Definitions & Computation Logic](#7-kpi-definitions--computation-logic)
8. [Data Validation Rules](#8-data-validation-rules)
9. [Challenges & Resolutions](#9-challenges--resolutions)
10. [Running the Pipeline](#10-running-the-pipeline)

---

## 1. Project Overview

This project implements a production-grade batch data pipeline that ingests raw flight price data for Bangladesh, validates and transforms it, computes analytical KPIs, and loads the results into a PostgreSQL analytics database for downstream analysis and reporting.

### Objectives

- Ingest raw CSV flight data into a MySQL staging database
- Validate data quality and flag or remove invalid records
- Transform and enrich data for analytical use
- Compute key business KPIs across airlines, routes, seasons, and booking patterns
- Load clean data and KPIs into PostgreSQL for analytics

### Dataset Overview

The source dataset contains flight pricing data across Bangladesh's domestic and international routes, with 25 airlines, 8 departure airports, and 20 destination airports. Key attributes include fare components, flight duration, booking lead time, travel class, and seasonal markers.

| Column | Type | Description |
|---|---|---|
| Airline | String | Airline name (25 options) |
| Source | String | Departure IATA code |
| Source Name | String | Full departure airport name |
| Destination | String | Arrival IATA code |
| Destination Name | String | Full arrival airport name |
| Departure Date & Time | DateTime | Departure timestamp |
| Arrival Date & Time | DateTime | Arrival timestamp |
| Duration (hrs) | Float | Flight duration in hours |
| Stopovers | String | Direct / 1 Stop / 2 Stops |
| Aircraft Type | String | Aircraft model |
| Class | String | Economy / Business / First Class |
| Booking Source | String | Direct Booking / Travel Agency / Online Website |
| Base Fare (BDT) | Float | Base price before tax |
| Tax & Surcharge (BDT) | Float | Tax and fees |
| Total Fare (BDT) | Float | Final price |
| Seasonality | String | Regular / Eid / Hajj / Winter Holidays |
| Days Before Departure | Integer | Booking lead time (1–90 days) |

---

## 2. Pipeline Architecture

### High-Level Flow

```
CSV File (Kaggle)
      │
      ▼
┌─────────────────┐
│   Task 1        │
│  Ingest CSV     │  → Loads raw data into MySQL staging table (raw_flights)
│  → MySQL        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Task 2        │
│  Validate &     │  → Quality checks, type casting, null handling,
│  Transform      │    categorical validation, fare verification,
└────────┬────────┘    derived column generation
         │
         ▼
┌─────────────────┐
│   Task 2 cont.  │
│  Compute KPIs   │  → Aggregations across airline, route,
│                 │    season, and booking lead time
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Task 2 cont.  │
│  Load →         │  → Inserts flight_facts and all KPI tables
│  PostgreSQL     │    into flight_analytics_bd
└─────────────────┘
```

### Database Architecture

```
my-postgres-container (single PostgreSQL instance)
├── airflow_metadata      — Airflow internal metadata (DAG runs, task logs, users)
├── e_commerce_events_db  — Pre-existing e-commerce project database
└── flight_analytics_bd   — Flight pipeline analytics database
    ├── flight_facts          (cleaned fact table)
    ├── kpi_airline_summary   (KPI aggregation)
    ├── kpi_seasonality_summary
    ├── kpi_route_summary
    └── kpi_booking_leadtime

mysql-staging (MySQL 8.0)
└── flight_staging
    └── raw_flights           (raw ingested CSV data)
```

### Container Architecture

All services run inside Docker containers on a shared internal bridge network (`flight_pipeline_network`):

| Container | Image | Port | Role |
|---|---|---|---|
| `my-postgres-container` | postgres:15 | 5432 | All PostgreSQL databases |
| `mysql-staging` | mysql:8.0 | 3306 | Staging database |
| `airflow-webserver` | apache/airflow:2.8.1-python3.9 | 8080 | Airflow UI |
| `airflow-scheduler` | apache/airflow:2.8.1-python3.9 | — | DAG scheduling |
| `airflow-init` | apache/airflow:2.8.1-python3.9 | — | One-time bootstrap |

---

## 3. Technology Stack

| Technology | Version | Purpose |
|---|---|---|
| Apache Airflow | 2.8.1 | Workflow orchestration and task scheduling |
| MySQL | 8.0 | Staging database for raw ingested data |
| PostgreSQL | 15 | Analytics database for clean facts and KPIs |
| Python | 3.9 | Data processing, validation, transformation |
| pandas | 2.1.4 | DataFrame operations and data manipulation |
| SQLAlchemy | 1.4.51 | Database connection abstraction |
| PyMySQL | 1.1.0 | MySQL driver for Python |
| psycopg2-binary | 2.9.9 | PostgreSQL driver for Python |
| Docker | — | Container orchestration |
| Docker Compose | — | Multi-container environment definition |

---

## 4. Environment Setup

### Project Structure

```
flight_pipeline/
├── dags/
│   └── flight_pipeline_dag.py          # Airflow DAG definition
├── data/
│   └── raw/
│       └── Flight_Price_Dataset_of_Bangladesh.csv
├── scripts/
│   ├── ingest.py                        # Task 1: CSV → MySQL
│   └── validate_transform_load.py       # Task 2: Validate, transform, KPIs, load
├── sql/
│   ├── init_postgres.sql                # Creates airflow_metadata + flight_analytics_bd
│   ├── postgres_analytics_schema.sql    # Creates all analytics tables
│   └── mysql_staging_schema.sql         # Creates raw_flights staging table
├── Dockerfile                           # Custom Airflow image with dependencies
├── docker-compose.yml                   # Full stack definition
└── requirements.txt                     # Python package dependencies
```

### Key Configuration

**Airflow Connections pre-registered via environment variables:**

```
AIRFLOW_CONN_MYSQL_STAGING     = mysql://staging_user:***@mysql-staging:3306/flight_staging
AIRFLOW_CONN_POSTGRES_ANALYTICS = postgresql://postgres:***@postgres:5432/flight_analytics_bd
AIRFLOW_CONN_POSTGRES_DEFAULT   = postgresql://postgres:***@postgres:5432/flight_analytics_bd
```

**Python dependencies (`requirements.txt`):**

```
apache-airflow-providers-mysql==3.4.0
apache-airflow-providers-postgres==5.7.1
pandas==2.1.4
numpy==1.26.3
SQLAlchemy==1.4.51
PyMySQL==1.1.0
psycopg2-binary==2.9.9
python-dotenv==1.0.0
```

---

## 5. Airflow DAG & Task Descriptions

### DAG: `bangladesh_flight_price_pipeline`

| Property | Value |
|---|---|
| DAG ID | `bangladesh_flight_price_pipeline` |
| Schedule | Manual trigger (`schedule=None`) |
| Catchup | Disabled |
| Tags | `flight`, `bangladesh` |
| Start Date | 2024-01-01 |
| Retries | 1 per task |

### Task 1: `ingest_csv_to_mysql`

**Operator:** `PythonOperator`
**Script:** `scripts/ingest.py`
**Function:** `ingest.run()`

**Responsibilities:**

1. Loads the raw CSV from `/opt/airflow/data/raw/`
2. Drops unnamed index columns Kaggle datasets often include
3. Normalizes column names to match the staging schema
4. Casts column types (datetime, numeric, integer)
5. Validates that all required columns are present
6. Checks for nulls in critical columns and drops affected rows
7. Removes duplicate rows
8. Truncates the MySQL `raw_flights` table for a clean load
9. Bulk inserts in chunks of 1,000 rows using `pandas.to_sql`
10. Verifies the row count in MySQL matches the inserted count

**Input:** `Flight_Price_Dataset_of_Bangladesh.csv`
**Output:** `flight_staging.raw_flights` (MySQL)

**Column mapping (CSV → DB):**

| CSV Header | DB Column |
|---|---|
| Airline | airline |
| Source | source |
| Source Name | source_name |
| Destination | destination |
| Destination Name | destination_name |
| Departure Date & Time | departure_datetime |
| Arrival Date & Time | arrival_datetime |
| Duration (hrs) | duration_hrs |
| Stopovers | stopovers |
| Aircraft Type | aircraft_type |
| Class | class |
| Booking Source | booking_source |
| Days Before Departure | days_before_departure |
| Base Fare (BDT) | base_fare_bdt |
| Tax & Surcharge (BDT) | tax_surcharge_bdt |
| Total Fare (BDT) | total_fare_bdt |
| Seasonality | seasonality |

---

### Task 2: `validate_transform_load`

**Operator:** `PythonOperator`
**Script:** `scripts/validate_transform_load.py`
**Function:** `validate_transform_load.run()`
**Depends on:** `ingest_csv_to_mysql`

This task runs four sequential sub-processes:

#### 2a. Extract

Reads all rows from MySQL `raw_flights` into a pandas DataFrame using `pd.read_sql`.

#### 2b. Validate

Applies data quality checks in the following order:

1. Confirms all required columns exist
2. Casts numeric and datetime columns with error coercion
3. Drops rows with nulls in critical columns
4. Fills non-critical nulls with sensible defaults
5. Validates non-empty strings in categorical fields
6. Checks class, stopovers, seasonality, and booking source against allowed value sets
7. Drops rows with negative or zero fares
8. Drops rows where arrival is before or equal to departure
9. Drops rows where days before departure is outside the 1–90 range
10. Removes duplicate rows
11. Logs all issues found with counts

#### 2c. Transform

Applies enrichment and standardization:

1. Corrects fare mismatches where `total_fare ≠ base_fare + tax` (tolerance: 1 BDT)
2. Derives `route` column as `source → destination`
3. Derives `is_peak_season` boolean flag
4. Derives `departure_date` and `departure_hour` from datetime
5. Standardizes text fields (strip whitespace, title case for airline, uppercase for IATA codes)
6. Fixes `days_before_departure` to float to resolve SQLAlchemy nullable integer incompatibility
7. Removes timezone info from datetime columns for PostgreSQL `TIMESTAMP` compatibility

#### 2d. Compute KPIs

See Section 7 for full KPI definitions.

#### 2e. Load

1. Truncates `flight_facts` and reloads with clean data
2. Truncates and reloads all four KPI tables
3. Verifies row counts in all five tables after load

**Input:** `flight_staging.raw_flights` (MySQL)
**Output:** `flight_analytics_bd.flight_facts` + four KPI tables (PostgreSQL)

---

### Task Dependency Graph

```
ingest_csv_to_mysql  ──►  validate_transform_load
```

---

## 6. Data Schema

### MySQL — `flight_staging.raw_flights`

| Column | Type | Notes |
|---|---|---|
| id | INT AUTO_INCREMENT | Primary key |
| airline | VARCHAR(100) | Airline name |
| source | VARCHAR(10) | IATA departure code |
| source_name | VARCHAR(255) | Full departure airport name |
| destination | VARCHAR(10) | IATA arrival code |
| destination_name | VARCHAR(255) | Full arrival airport name |
| departure_datetime | DATETIME | Departure timestamp |
| arrival_datetime | DATETIME | Arrival timestamp |
| duration_hrs | DECIMAL(6,2) | Flight duration in hours |
| stopovers | VARCHAR(50) | Direct / 1 Stop / 2 Stops |
| aircraft_type | VARCHAR(100) | Aircraft model |
| class | VARCHAR(50) | Economy / Business / First Class |
| booking_source | VARCHAR(100) | Booking channel |
| days_before_departure | INT | Booking lead time |
| base_fare_bdt | DECIMAL(12,2) | Base fare in BDT |
| tax_surcharge_bdt | DECIMAL(12,2) | Tax and surcharges |
| total_fare_bdt | DECIMAL(12,2) | Total fare in BDT |
| seasonality | VARCHAR(50) | Season label |
| ingested_at | TIMESTAMP | Pipeline load timestamp |
| source_file | VARCHAR(255) | Source CSV filename |

### PostgreSQL — `flight_analytics_bd.flight_facts`

| Column | Type | Notes |
|---|---|---|
| id | SERIAL | Primary key |
| airline | VARCHAR(100) | Standardized airline name |
| source | VARCHAR(10) | IATA departure code (uppercase) |
| source_name | VARCHAR(255) | Full departure airport name |
| destination | VARCHAR(10) | IATA arrival code (uppercase) |
| destination_name | VARCHAR(255) | Full arrival airport name |
| departure_datetime | TIMESTAMP | Timezone-naive departure timestamp |
| arrival_datetime | TIMESTAMP | Timezone-naive arrival timestamp |
| duration_hrs | NUMERIC(6,2) | Flight duration in hours |
| stopovers | VARCHAR(50) | Validated stopover value |
| aircraft_type | VARCHAR(100) | Aircraft model |
| class | VARCHAR(50) | Travel class |
| booking_source | VARCHAR(100) | Booking channel |
| days_before_departure | INT | Booking lead time |
| base_fare_bdt | NUMERIC(12,2) | Base fare in BDT |
| tax_surcharge_bdt | NUMERIC(12,2) | Tax and surcharges |
| total_fare_bdt | NUMERIC(12,2) | Verified total fare |
| seasonality | VARCHAR(50) | Season label |
| loaded_at | TIMESTAMP | Pipeline load timestamp |

---

## 7. KPI Definitions & Computation Logic

### KPI 1 — Airline Summary (`kpi_airline_summary`)

**Business question:** Which airlines offer the best value and how are bookings distributed across them?

| KPI Column | Formula | Purpose |
|---|---|---|
| avg_fare_bdt | `MEAN(total_fare_bdt)` GROUP BY airline | Average ticket price per airline |
| min_fare_bdt | `MIN(total_fare_bdt)` GROUP BY airline | Cheapest fare offered |
| max_fare_bdt | `MAX(total_fare_bdt)` GROUP BY airline | Most expensive fare offered |
| total_flights | `COUNT(*)` GROUP BY airline | Booking count per airline |
| avg_duration_hrs | `MEAN(duration_hrs)` GROUP BY airline | Average flight duration |
| economy_pct | `(class == "Economy").sum() / COUNT(*) × 100` | Share of economy bookings |
| business_pct | `(class == "Business").sum() / COUNT(*) × 100` | Share of business bookings |
| first_pct | `(class == "First Class").sum() / COUNT(*) × 100` | Share of first class bookings |

**Python implementation:**
```python
df.groupby("airline").agg(
    avg_fare_bdt     = ("total_fare_bdt", "mean"),
    min_fare_bdt     = ("total_fare_bdt", "min"),
    max_fare_bdt     = ("total_fare_bdt", "max"),
    total_flights    = ("total_fare_bdt", "count"),
    avg_duration_hrs = ("duration_hrs", "mean"),
    economy_pct      = ("class", lambda x: round((x == "Economy").sum() / len(x) * 100, 2)),
    business_pct     = ("class", lambda x: round((x == "Business").sum() / len(x) * 100, 2)),
    first_pct        = ("class", lambda x: round((x == "First Class").sum() / len(x) * 100, 2)),
)
```

---

### KPI 2 — Seasonal Fare Variation (`kpi_seasonality_summary`)

**Business question:** How do fares vary across different seasons, and what is the premium during peak periods?

**Peak seasons defined as:** `Eid`, `Hajj`, `Winter Holidays`
**Non-peak season:** `Regular`

| KPI Column | Formula | Purpose |
|---|---|---|
| avg_fare_bdt | `MEAN(total_fare_bdt)` GROUP BY seasonality | Average fare per season |
| min_fare_bdt | `MIN(total_fare_bdt)` GROUP BY seasonality | Minimum fare per season |
| max_fare_bdt | `MAX(total_fare_bdt)` GROUP BY seasonality | Maximum fare per season |
| flight_count | `COUNT(*)` GROUP BY seasonality | Number of flights per season |
| avg_days_before_dep | `MEAN(days_before_departure)` GROUP BY seasonality | Average booking lead time |

**Peak vs non-peak comparison computed inline:**
```python
peak     = df[df["is_peak_season"]]["total_fare_bdt"].mean()
non_peak = df[~df["is_peak_season"]]["total_fare_bdt"].mean()
premium  = ((peak - non_peak) / non_peak) * 100
```

---

### KPI 3 — Most Popular Routes (`kpi_route_summary`)

**Business question:** Which source-destination pairs have the highest booking volume and what are their fare characteristics?

| KPI Column | Formula | Purpose |
|---|---|---|
| route | `source + " → " + destination` | Route identifier |
| flight_count | `COUNT(*)` GROUP BY route | Booking count — identifies popular routes |
| avg_fare_bdt | `MEAN(total_fare_bdt)` GROUP BY route | Average route fare |
| min_fare_bdt | `MIN(total_fare_bdt)` GROUP BY route | Cheapest available on route |
| max_fare_bdt | `MAX(total_fare_bdt)` GROUP BY route | Most expensive on route |
| avg_duration_hrs | `MEAN(duration_hrs)` GROUP BY route | Average flight time |
| direct_flight_pct | `(stopovers == "Direct").sum() / COUNT(*) × 100` | % of direct flights on route |

Results are sorted by `flight_count DESC` to surface the most popular routes at the top.

---

### KPI 4 — Booking Lead Time Analysis (`kpi_booking_leadtime`)

**Business question:** Does booking earlier result in lower fares? How do fares vary by how far in advance a booking is made?

**Lead time buckets:**

| Bucket | Range |
|---|---|
| 1–7 days | Last-minute bookings |
| 8–14 days | Short lead time |
| 15–30 days | Medium lead time |
| 31–90 days | Advance bookings |

**Implementation:**
```python
bins   = [0, 7, 14, 30, 90]
labels = ["1-7 days", "8-14 days", "15-30 days", "31-90 days"]
df["days_bucket"] = pd.cut(df["days_before_departure"], bins=bins, labels=labels, right=True)
```

| KPI Column | Formula | Purpose |
|---|---|---|
| avg_fare_bdt | `MEAN(total_fare_bdt)` GROUP BY days_bucket | Average fare per lead time bucket |
| min_fare_bdt | `MIN(total_fare_bdt)` GROUP BY days_bucket | Minimum fare per bucket |
| max_fare_bdt | `MAX(total_fare_bdt)` GROUP BY days_bucket | Maximum fare per bucket |
| flight_count | `COUNT(*)` GROUP BY days_bucket | Number of bookings per bucket |

---

## 8. Data Validation Rules

### Required Columns

The pipeline will raise a `ValueError` and halt if any of these are missing:

- `airline`, `source`, `destination`, `base_fare_bdt`, `tax_surcharge_bdt`, `total_fare_bdt`

### Allowed Values

| Field | Valid Values |
|---|---|
| class | Economy, Business, First Class |
| stopovers | Direct, 1 Stop, 2 Stops |
| seasonality | Regular, Eid, Hajj, Winter Holidays |
| booking_source | Direct Booking, Travel Agency, Online Website |
| days_before_departure | 1 to 90 (inclusive) |

### Business Rules

| Rule | Action |
|---|---|
| `total_fare_bdt <= 0` | Drop row |
| `base_fare_bdt < 0` | Drop row |
| `duration_hrs <= 0` | Drop row |
| `arrival_datetime <= departure_datetime` | Drop row |
| `\|base + tax - total\| > 1 BDT` | Recalculate: `total = base + tax` |
| Empty strings in airline, source, destination | Drop row |
| Duplicate rows | Drop duplicates |

---

## 9. Challenges & Resolutions

### Challenge 1 — Python Version Incompatibility

**Problem:** The default `apache/airflow:2.8.1` image runs Python 3.8. Installing `pandas==2.1.4` and `numpy==1.26.3` failed because both require Python 3.9+.

**Resolution:** Switched to the `apache/airflow:2.8.1-python3.9` variant which ships Python 3.9 while keeping the same Airflow version. Updated `requirements.txt` to use the target package versions rather than downgrading.

---

### Challenge 2 — PyMySQL Not Importable Despite Being Installed

**Problem:** `pip install PyMySQL` succeeded but `import pymysql` raised `ModuleNotFoundError`. The package installed to the user home directory (`~/.local/lib/python3.9`) rather than the system site-packages, which SQLAlchemy's dialect loader could not find.

**Resolution:** Created a custom `Dockerfile` that runs `pip install -r requirements.txt` during image build time as the `airflow` user, ensuring packages land in the correct location before the container starts.

```dockerfile
FROM apache/airflow:2.8.1-python3.9
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
```

---

### Challenge 3 — Docker Volume Preventing Postgres Init Scripts from Running

**Problem:** The `docker-entrypoint-initdb.d/` scripts (which create the additional databases) only execute on first boot when the data volume is empty. Since the Postgres container had already been started, subsequent `docker-compose up` runs skipped the init scripts entirely.

**Resolution:** Ran `docker-compose down -v` to remove the volume, forcing a full re-initialization on the next startup. The `init_postgres.sql` uses `WHERE NOT EXISTS` guards to make the script idempotent on any future fresh deployment.

---

### Challenge 4 — Init Script Treated as Directory by Docker

**Problem:** When `postgres_analytics_schema.sql` did not exist on the host, Docker created a directory at that path instead of a file when processing the volume mount. The container then failed with `error: could not read from input file: Is a directory`.

**Resolution:** Created all SQL files on the host before running `docker-compose up`. Renamed files with `01_` and `02_` prefixes to enforce deterministic execution order in `docker-entrypoint-initdb.d`.

---

### Challenge 5 — Windows Line Endings Breaking Shell Scripts

**Problem:** The `init_postgres.sh` shell script was created on Windows with CRLF line endings. Inside the Linux container, bash returned `cannot execute: required file not found` because it could not parse the script.

**Resolution:** Replaced the shell script entirely with a pure SQL file (`init_postgres.sql`). SQL files are not sensitive to line endings, eliminating the cross-platform issue entirely.

---

### Challenge 6 — SQLAlchemy `Connection.commit()` Removed in 1.4

**Problem:** The `ingest.py` script called `conn.commit()` directly on a `Connection` object inside a `with engine.connect()` block. SQLAlchemy 1.4 removed this method from `Connection`, causing `AttributeError: 'Connection' object has no attribute 'commit'`.

**Resolution:** Replaced `engine.connect()` with `engine.begin()`. The `begin()` context manager opens an explicit transaction that automatically commits on clean exit and rolls back on exception, removing the need for manual commit calls.

```python
# Before (broken)
with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE raw_flights"))
    conn.commit()

# After (correct)
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE raw_flights"))
```

---

### Challenge 7 — Generated Columns Rejecting Explicit Values

**Problem:** The original `flight_facts` schema included `GENERATED ALWAYS AS` columns for `route`, `departure_date`, and `departure_hour`. When `pandas.to_sql` tried to insert these columns, PostgreSQL raised `ERROR: column "route" can only be updated to DEFAULT`.

**Resolution:** Removed all generated columns from the schema. The derived values (`route`, `departure_date`, `departure_hour`) are now computed in Python during the transform step and either stored as regular columns or computed at query time using SQL expressions.

---

### Challenge 8 — Schema Mismatch Between Script and Database

**Problem:** After iterating on the schema design, the `flight_facts` table in PostgreSQL still had the old column names from a previous version. The insert failed with `column "source" of relation "flight_facts" does not exist` even though the Python script was correct.

**Resolution:** Connected directly to PostgreSQL and ran `DROP TABLE IF EXISTS flight_facts` followed by the updated `CREATE TABLE` DDL to rebuild with the correct column definitions. Updated `postgres_analytics_schema.sql` to always `DROP TABLE IF EXISTS` before creation to prevent stale schema issues.

---

### Challenge 9 — Categorical Value Mismatches

**Problem:** Validation rules were initially defined based on expected values (`Economy`, `Business`, `First`), but the actual dataset contained different strings (`First Class`, `Winter Holidays`, `Direct Booking`, `Travel Agency`). All rows with these values were being silently dropped.

**Resolution:** Inspected the raw data and updated the `VALID_*` sets in the script to match exactly what the dataset contains rather than normalizing values. This preserves data fidelity and avoids any unintended semantic changes.

```python
VALID_CLASSES        = {"Economy", "Business", "First Class"}
VALID_SEASONALITY    = {"Regular", "Eid", "Hajj", "Winter Holidays"}
VALID_BOOKING_SOURCE = {"Direct Booking", "Travel Agency", "Online Website"}
```

---

### Challenge 10 — Airflow Fernet Key Format Error

**Problem:** On first `docker-compose up`, Airflow crashed with `AirflowException: Could not create Fernet object: Fernet key must be 32 url-safe base64-encoded bytes`. The key had been manually typed rather than generated.

**Resolution:** Generated a valid key programmatically using Python's `cryptography` library:

```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

The output is a properly formatted 44-character base64 string that Airflow accepts.

---

## 10. Running the Pipeline

### First-Time Setup

```bash
# 1. Clone the project and navigate to it
cd "Airflow Project Flight Price Analysis"

# 2. Place the CSV dataset
cp Flight_Price_Dataset_of_Bangladesh.csv data/raw/

# 3. Generate a Fernet key and update docker-compose.yml
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 4. Build the custom Airflow image
docker-compose build

# 5. Start databases first
docker-compose up -d postgres mysql-staging

# 6. Wait for healthy status
docker-compose ps

# 7. Bootstrap Airflow
docker-compose up airflow-init

# 8. Start all services
docker-compose up -d
```

### Triggering the Pipeline

Navigate to `http://localhost:8080`, log in with `admin / admin`, find the `bangladesh_flight_price_pipeline` DAG and click the play button to trigger a manual run.

Or trigger via CLI:

```bash
docker exec -it airflow-webserver airflow dags trigger bangladesh_flight_price_pipeline
```

### Verifying Results

```bash
# Check flight facts loaded
docker exec -it my-postgres-container psql -U postgres -d flight_analytics_bd \
  -c "SELECT COUNT(*) FROM flight_facts;"

# Check KPI tables
docker exec -it my-postgres-container psql -U postgres -d flight_analytics_bd -c "
  SELECT airline, total_flights, ROUND(avg_fare_bdt,0) AS avg_fare
  FROM kpi_airline_summary
  ORDER BY avg_fare_bdt DESC
  LIMIT 10;
"

# Check seasonal variation
docker exec -it my-postgres-container psql -U postgres -d flight_analytics_bd -c "
  SELECT seasonality, flight_count, ROUND(avg_fare_bdt,0) AS avg_fare
  FROM kpi_seasonality_summary
  ORDER BY avg_fare_bdt DESC;
"

# Check most popular routes
docker exec -it my-postgres-container psql -U postgres -d flight_analytics_bd -c "
  SELECT route, flight_count, ROUND(avg_fare_bdt,0) AS avg_fare
  FROM kpi_route_summary
  ORDER BY flight_count DESC
  LIMIT 10;
"

# Check booking lead time effect on fares
docker exec -it my-postgres-container psql -U postgres -d flight_analytics_bd -c "
  SELECT days_bucket, flight_count, ROUND(avg_fare_bdt,0) AS avg_fare
  FROM kpi_booking_leadtime
  ORDER BY avg_fare_bdt DESC;
"
```

### Rerunning a Fresh Pipeline

```bash
# Tear down everything including volumes (resets all databases)
docker-compose down -v

# Start fresh
docker-compose up -d postgres mysql-staging
docker-compose up airflow-init
docker-compose up -d
```

---
