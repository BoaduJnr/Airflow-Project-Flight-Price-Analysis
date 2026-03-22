import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────
MYSQL_CONN = (
    "mysql+pymysql://staging_user:staging_pass"
    "@mysql-staging:3306/flight_staging"
)

CSV_PATH = os.getenv(
    "CSV_PATH",
    "/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv"
)


# ── Column mapping — CSV name → DB column name ────────────────────────────────
COLUMN_MAP = {
    "Airline":                  "airline",
    "Source":                   "source",
    "Source Name":              "source_name",
    "Destination":              "destination",
    "Destination Name":         "destination_name",
    "Departure Date & Time":    "departure_datetime",
    "Arrival Date & Time":      "arrival_datetime",
    "Duration (hrs)":           "duration_hrs",
    "Stopovers":                "stopovers",
    "Aircraft Type":            "aircraft_type",
    "Class":                    "class",
    "Booking Source":           "booking_source",
    "Days Before Departure":    "days_before_departure",
    "Base Fare (BDT)":          "base_fare_bdt",
    "Tax & Surcharge (BDT)":    "tax_surcharge_bdt",
    "Total Fare (BDT)":         "total_fare_bdt",
    "Seasonality":              "seasonality",
}

# ── Validation rules ──────────────────────────────────────────────────────────
VALID_CLASSES        = {"Economy", "Business", "First Class"}
VALID_STOPOVERS      = {"Direct", "1 Stop", "2 Stops"}
VALID_SEASONALITY    = {"Regular", "Eid", "Hajj", "Winter Holidays"}
VALID_BOOKING_SOURCE = {"Direct Booking", "Travel Agency", "Online Website"}
CRITICAL_COLUMNS     = [
    "airline", "source", "destination",
    "departure_datetime", "arrival_datetime",
    "total_fare_bdt"
]



def load_csv(path: str) -> pd.DataFrame:
    """Load CSV and rename columns to match DB schema."""
    log.info(f"Loading CSV from: {path}")
    df = pd.read_csv(path)

    # Drop unnamed index columns Kaggle datasets often include
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    log.info(f"Columns found: {list(df.columns)}")

    # Check all expected columns are present before renaming
    missing = set(COLUMN_MAP.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in CSV: {missing}")

    # Rename to DB column names
    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)
    log.info("✔ All expected columns present and renamed")
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Validate types, nulls, ranges and allowed values."""
    log.info("Running validation...")
    initial_count = len(df)

    # ── Cast datetime columns ─────────────────────────────────────────────────
    df["departure_datetime"] = pd.to_datetime(df["departure_datetime"], errors="coerce")
    df["arrival_datetime"]   = pd.to_datetime(df["arrival_datetime"],   errors="coerce")
    log.info("✔ Datetime columns parsed")

    # ── Cast numeric columns ──────────────────────────────────────────────────
    df["duration_hrs"]          = pd.to_numeric(df["duration_hrs"],          errors="coerce")
    df["days_before_departure"] = pd.to_numeric(df["days_before_departure"],  errors="coerce")
    df["base_fare_bdt"]         = pd.to_numeric(df["base_fare_bdt"],          errors="coerce")
    df["tax_surcharge_bdt"]     = pd.to_numeric(df["tax_surcharge_bdt"],      errors="coerce")
    df["total_fare_bdt"]        = pd.to_numeric(df["total_fare_bdt"],          errors="coerce")
    log.info("✔ Numeric columns cast")

    # ── Drop rows with nulls in critical columns ──────────────────────────────
    null_counts = df[CRITICAL_COLUMNS].isnull().sum()
    if null_counts.any():
        log.warning(f"Null values found:\n{null_counts[null_counts > 0]}")
        df = df.dropna(subset=CRITICAL_COLUMNS)
        log.warning(f"Dropped {initial_count - len(df)} rows with critical nulls")
    else:
        log.info("✔ No nulls in critical columns")

    # ── Validate allowed values ───────────────────────────────────────────────
    invalid_class = ~df["class"].isin(VALID_CLASSES)
    if invalid_class.any():
        log.warning(f"Invalid class values: {df.loc[invalid_class, 'class'].unique()}")
        df = df[~invalid_class]

    invalid_stops = ~df["stopovers"].isin(VALID_STOPOVERS)
    if invalid_stops.any():
        log.warning(f"Invalid stopover values: {df.loc[invalid_stops, 'stopovers'].unique()}")
        df = df[~invalid_stops]

    invalid_season = ~df["seasonality"].isin(VALID_SEASONALITY)
    if invalid_season.any():
        log.warning(f"Invalid seasonality values: {df.loc[invalid_season, 'seasonality'].unique()}")
        df = df[~invalid_season]
    log.info(f"Rows remained {len(df)} rows, {len(df.columns)} columns")

    invalid_booking = ~df["booking_source"].isin(VALID_BOOKING_SOURCE)
    if invalid_booking.any():
        log.warning(f"Invalid booking source values: {df.loc[invalid_booking, 'booking_source'].unique()}")
        df = df[~invalid_booking]

    log.info("✔ Allowed value validation complete")

    # ── Validate ranges ───────────────────────────────────────────────────────
    invalid_days = (df["days_before_departure"] < 1) | (df["days_before_departure"] > 90)
    if invalid_days.any():
        log.warning(f"days_before_departure out of range (1-90): {invalid_days.sum()} rows")
        df = df[~invalid_days]

    invalid_fare = df["total_fare_bdt"] <= 0
    if invalid_fare.any():
        log.warning(f"Non-positive total_fare_bdt: {invalid_fare.sum()} rows")
        df = df[~invalid_fare]

    invalid_duration = df["duration_hrs"] <= 0
    if invalid_duration.any():
        log.warning(f"Non-positive duration_hrs: {invalid_duration.sum()} rows")
        df = df[~invalid_duration]

    log.info("✔ Range validation complete")
    log.info(f"Rows remained {len(df)} rows, {len(df.columns)} columns")

    # ── Check fare consistency: base + tax should equal total ─────────────────
    fare_mismatch = abs(
        (df["base_fare_bdt"] + df["tax_surcharge_bdt"]) - df["total_fare_bdt"]
    ) > 1.0   # allow 1 BDT rounding tolerance
    if fare_mismatch.any():
        log.warning(f"Fare mismatch (base + tax ≠ total): {fare_mismatch.sum()} rows flagged")

    # ── Arrival must be after departure ──────────────────────────────────────
    invalid_times = df["arrival_datetime"] <= df["departure_datetime"]
    if invalid_times.any():
        log.warning(f"arrival_datetime ≤ departure_datetime: {invalid_times.sum()} rows — dropping")
        df = df[~invalid_times]

    # ── Remove duplicates ─────────────────────────────────────────────────────
    dupes = df.duplicated().sum()
    if dupes > 0:
        log.warning(f"Removing {dupes} duplicate rows")
        df = df.drop_duplicates()
    else:
        log.info("✔ No duplicate rows")

    log.info(f"✔ Validation complete — {len(df)}/{initial_count} rows passed")
    return df


def ingest(df: pd.DataFrame, engine) -> int:
    """Truncate staging table and bulk insert validated data."""
    log.info("Inserting into MySQL raw_flights...")

    # Add pipeline metadata
    df["ingested_at"] = datetime.utcnow()
    df["source_file"] = os.path.basename(CSV_PATH)

    # Truncate staging table for a clean load
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw_flights"))
        log.info("✔ Staging table truncated")

    # Bulk insert in chunks
    df.to_sql(
        name="raw_flights",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )

    log.info(f"✔ Inserted {len(df)} rows into raw_flights")
    return len(df)


def verify(engine, expected_count: int):
    """Confirm row count in MySQL matches what was inserted."""
    with engine.begin() as conn:
        db_count = conn.execute(
            text("SELECT COUNT(*) FROM raw_flights")
        ).scalar()

    if db_count != expected_count:
        raise ValueError(
            f"Row count mismatch! Inserted {expected_count}, MySQL has {db_count}"
        )
    log.info(f"✔ Verification passed — {db_count} rows confirmed in MySQL")


def run():
    engine = create_engine(MYSQL_CONN)
    df     = load_csv(CSV_PATH)
    df     = validate(df)
    count  = ingest(df, engine)
    verify(engine, count)
    log.info("🚀 Ingestion pipeline complete")


if __name__ == "__main__":
    run()