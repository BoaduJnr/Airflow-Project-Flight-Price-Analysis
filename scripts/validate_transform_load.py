import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MYSQL_CONN    = "mysql+pymysql://staging_user:staging_pass@mysql-staging:3306/flight_staging"
POSTGRES_CONN = "postgresql+psycopg2://postgres:pielly16@postgres:5432/flight_analytics_bd"

# ── Validation rules ──────────────────────────────────────────────────────────
REQUIRED_COLUMNS  = [
    "airline", "source", "destination",
    "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"
]
VALID_CLASSES        = {"Economy", "Business", "First Class"}
VALID_STOPOVERS      = {"Direct", "1 Stop", "2 Stops"}
VALID_SEASONALITY    = {"Regular", "Eid", "Hajj", "Winter Holidays"}
VALID_BOOKING_SOURCE = {"Direct Booking", "Travel Agency", "Online Website"}
PEAK_SEASONS         = {"Eid", "Hajj", "Winter Holidays"}


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — EXTRACT FROM MYSQL
# ══════════════════════════════════════════════════════════════════════════════
def extract(mysql_engine) -> pd.DataFrame:
    log.info("Extracting data from MySQL staging...")
    df = pd.read_sql("SELECT * FROM raw_flights", con=mysql_engine)
    log.info(f"✔ Extracted {len(df)} rows from MySQL")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — VALIDATE
# ══════════════════════════════════════════════════════════════════════════════
def validate(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Running validation...")
    initial_count = len(df)
    issues = []

    # ── Check required columns exist ──────────────────────────────────────────
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    log.info("✔ All required columns present")

    # ── Cast numeric columns ──────────────────────────────────────────────────
    for col in ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt", "duration_hrs"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["days_before_departure"] = pd.to_numeric(
        df["days_before_departure"], errors="coerce"
    ).astype("Int64")

    # ── Cast datetime columns ─────────────────────────────────────────────────
    df["departure_datetime"] = pd.to_datetime(df["departure_datetime"], errors="coerce")
    df["arrival_datetime"]   = pd.to_datetime(df["arrival_datetime"],   errors="coerce")
    log.info("✔ Column types cast")

    # ── Handle nulls in required columns ─────────────────────────────────────
    null_mask = df[REQUIRED_COLUMNS].isnull().any(axis=1)
    if null_mask.any():
        issues.append(f"{null_mask.sum()} rows dropped — nulls in required columns")
        df = df[~null_mask]

    # ── Handle nulls in non-critical columns — fill with defaults ─────────────
    df["stopovers"]      = df["stopovers"].fillna("Unknown")
    df["aircraft_type"]  = df["aircraft_type"].fillna("Unknown")
    df["booking_source"] = df["booking_source"].fillna("Unknown")
    df["seasonality"]    = df["seasonality"].fillna("Regular")
    log.info("✔ Null values handled")

    # ── Validate non-empty strings for categorical fields ─────────────────────
    for col in ["airline", "source", "destination", "source_name", "destination_name"]:
        empty_mask = df[col].str.strip().eq("") | df[col].isnull()
        if empty_mask.any():
            issues.append(f"{empty_mask.sum()} rows dropped — empty string in {col}")
            df = df[~empty_mask]

    # ── Validate allowed categorical values ───────────────────────────────────
    invalid_class = ~df["class"].isin(VALID_CLASSES)
    if invalid_class.any():
        issues.append(f"{invalid_class.sum()} rows dropped — invalid class value")
        df = df[~invalid_class]

    invalid_stops = ~df["stopovers"].isin(VALID_STOPOVERS | {"Unknown"})
    if invalid_stops.any():
        issues.append(f"{invalid_stops.sum()} rows dropped — invalid stopovers value")
        df = df[~invalid_stops]

    invalid_season = ~df["seasonality"].isin(VALID_SEASONALITY)
    if invalid_season.any():
        issues.append(f"{invalid_season.sum()} rows dropped — invalid seasonality value")
        df = df[~invalid_season]

    log.info("✔ Categorical value validation complete")

    # ── Flag and correct negative or zero fares ───────────────────────────────
    negative_fare = df["total_fare_bdt"] <= 0
    if negative_fare.any():
        issues.append(f"{negative_fare.sum()} rows dropped — negative or zero total fare")
        df = df[~negative_fare]

    negative_base = df["base_fare_bdt"] < 0
    if negative_base.any():
        issues.append(f"{negative_base.sum()} rows dropped — negative base fare")
        df = df[~negative_base]

    log.info("✔ Fare range validation complete")

    # ── Validate arrival is after departure ───────────────────────────────────
    invalid_times = df["arrival_datetime"] <= df["departure_datetime"]
    if invalid_times.any():
        issues.append(f"{invalid_times.sum()} rows dropped — arrival before departure")
        df = df[~invalid_times]

    # ── Validate days_before_departure range ──────────────────────────────────
    invalid_days = (df["days_before_departure"] < 1) | (df["days_before_departure"] > 90)
    if invalid_days.any():
        issues.append(f"{invalid_days.sum()} rows dropped — days_before_departure out of range")
        df = df[~invalid_days]

    # ── Remove duplicates ─────────────────────────────────────────────────────
    dupes = df.duplicated().sum()
    if dupes > 0:
        issues.append(f"{dupes} duplicate rows removed")
        df = df.drop_duplicates()

    # ── Print all issues found ────────────────────────────────────────────────
    if issues:
        log.warning("Validation issues found:")
        for issue in issues:
            log.warning(f"  ⚠ {issue}")

    log.info(f"✔ Validation complete — {len(df)}/{initial_count} rows passed")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Running transformations...")

    # ── Recalculate Total Fare if missing or inconsistent ─────────────────────
    fare_mismatch = abs(
        (df["base_fare_bdt"] + df["tax_surcharge_bdt"]) - df["total_fare_bdt"]
    ) > 1.0
    if fare_mismatch.any():
        log.warning(f"Correcting {fare_mismatch.sum()} fare mismatches — recalculating total")
        df.loc[fare_mismatch, "total_fare_bdt"] = (
            df.loc[fare_mismatch, "base_fare_bdt"] +
            df.loc[fare_mismatch, "tax_surcharge_bdt"]
        )
    log.info("✔ Total Fare = Base Fare + Tax & Surcharge verified")

    # ── Derive route column ───────────────────────────────────────────────────
    df["route"] = df["source"] + " → " + df["destination"]

    # ── Derive is_peak_season flag ────────────────────────────────────────────
    df["is_peak_season"] = df["seasonality"].isin(PEAK_SEASONS)

    # ── Derive departure date and hour ────────────────────────────────────────
    df["departure_date"] = df["departure_datetime"].dt.date
    df["departure_hour"] = df["departure_datetime"].dt.hour

    # ── Standardize text columns ──────────────────────────────────────────────
    df["airline"]      = df["airline"].str.strip().str.title()
    df["source"]       = df["source"].str.strip().str.upper()
    df["destination"]  = df["destination"].str.strip().str.upper()
    df["class"]        = df["class"].str.strip().str.title()
    df["stopovers"]    = df["stopovers"].str.strip().str.title()
    df["seasonality"]  = df["seasonality"].str.strip().str.title()

    log.info("✔ Transformations complete")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — COMPUTE KPIs
# ══════════════════════════════════════════════════════════════════════════════
def compute_kpis(df: pd.DataFrame) -> dict:
    log.info("Computing KPIs...")

    # ── KPI 1: Average Fare by Airline ────────────────────────────────────────
    kpi_airline = (
        df.groupby("airline")
        .agg(
            avg_fare_bdt      = ("total_fare_bdt", "mean"),
            min_fare_bdt      = ("total_fare_bdt", "min"),
            max_fare_bdt      = ("total_fare_bdt", "max"),
            total_flights     = ("total_fare_bdt", "count"),
            avg_duration_hrs  = ("duration_hrs",   "mean"),
            economy_pct       = ("class", lambda x: round((x == "Economy").sum()  / len(x) * 100, 2)),
            business_pct      = ("class", lambda x: round((x == "Business").sum() / len(x) * 100, 2)),
            first_class_pct         = ("class", lambda x: round((x == "First Class").sum()    / len(x) * 100, 2)),
        )
        .reset_index()
    )
    kpi_airline["computed_at"] = datetime.now(timezone.utc)
    log.info(f"✔ KPI 1 — Airline summary: {len(kpi_airline)} airlines")

    # ── KPI 2: Seasonal Fare Variation ────────────────────────────────────────
    kpi_season = (
        df.groupby("seasonality")
        .agg(
            avg_fare_bdt        = ("total_fare_bdt",        "mean"),
            min_fare_bdt        = ("total_fare_bdt",        "min"),
            max_fare_bdt        = ("total_fare_bdt",        "max"),
            flight_count        = ("total_fare_bdt",        "count"),
            avg_days_before_dep = ("days_before_departure", "mean"),
        )
        .reset_index()
    )
    kpi_season["computed_at"] = datetime.utcnow()

    # Peak vs non-peak comparison
    peak     = df[df["is_peak_season"]]["total_fare_bdt"].mean()
    non_peak = df[~df["is_peak_season"]]["total_fare_bdt"].mean()
    log.info(f"✔ KPI 2 — Seasonal variation: Peak avg BDT {peak:,.2f} vs Non-peak avg BDT {non_peak:,.2f}")

    # ── KPI 3: Booking Count by Airline ───────────────────────────────────────
    # Already computed in kpi_airline as total_flights
    log.info(f"✔ KPI 3 — Booking count by airline computed")

    # ── KPI 4: Most Popular Routes ────────────────────────────────────────────
    kpi_route = (
        df.groupby(["route", "source", "destination", "source_name", "destination_name"])
        .agg(
            avg_fare_bdt      = ("total_fare_bdt", "mean"),
            min_fare_bdt      = ("total_fare_bdt", "min"),
            max_fare_bdt      = ("total_fare_bdt", "max"),
            flight_count      = ("total_fare_bdt", "count"),
            avg_duration_hrs  = ("duration_hrs",   "mean"),
            direct_flight_pct = ("stopovers", lambda x: round((x == "Direct").sum() / len(x) * 100, 2)),
        )
        .reset_index()
        .sort_values("flight_count", ascending=False)
    )
    kpi_route["computed_at"] = datetime.utcnow()
    log.info(f"✔ KPI 4 — Route summary: {len(kpi_route)} routes, top route: {kpi_route.iloc[0]['route']}")

    # ── KPI 5: Booking Lead Time Buckets ──────────────────────────────────────
    bins   = [0, 7, 14, 30, 90]
    labels = ["1-7 days", "8-14 days", "15-30 days", "31-90 days"]
    df["days_bucket"] = pd.cut(
        df["days_before_departure"],
        bins=bins,
        labels=labels,
        right=True
    ).astype(str)

    kpi_leadtime = (
        df.groupby("days_bucket")
        .agg(
            avg_fare_bdt  = ("total_fare_bdt", "mean"),
            min_fare_bdt  = ("total_fare_bdt", "min"),
            max_fare_bdt  = ("total_fare_bdt", "max"),
            flight_count  = ("total_fare_bdt", "count"),
        )
        .reset_index()
    )
    kpi_leadtime["computed_at"] = datetime.utcnow()
    log.info(f"✔ KPI 5 — Booking lead time buckets computed")

    return {
        "kpi_airline_summary":     kpi_airline,
        "kpi_seasonality_summary": kpi_season,
        "kpi_route_summary":       kpi_route,
        "kpi_booking_leadtime":    kpi_leadtime,
    }


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — LOAD INTO POSTGRESQL
# ══════════════════════════════════════════════════════════════════════════════
def load(df: pd.DataFrame, kpis: dict, pg_engine) -> None:
    log.info("Loading data into PostgreSQL...")

    # ── Load flight facts ─────────────────────────────────────────────────────
    fact_columns = [
        "airline", "source", "source_name", "destination", "destination_name",
        "departure_datetime", "arrival_datetime", "duration_hrs",
        "stopovers", "aircraft_type", "class", "booking_source",
        "days_before_departure", "base_fare_bdt", "tax_surcharge_bdt",
        "total_fare_bdt", "seasonality",
    ]
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE flight_facts RESTART IDENTITY"))
    log.info("✔ flight_facts truncated")

    df[fact_columns].to_sql(
        name="flight_facts",
        con=pg_engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )
    log.info(f"✔ Loaded {len(df)} rows into flight_facts")
       

    # ── Load KPI tables ───────────────────────────────────────────────────────
    kpi_table_map = {
        "kpi_airline_summary":     "kpi_airline_summary",
        "kpi_seasonality_summary": "kpi_seasonality_summary",
        "kpi_route_summary":       "kpi_route_summary",
        "kpi_booking_leadtime":    "kpi_booking_leadtime",
    }

    for kpi_name, table_name in kpi_table_map.items():
        kpi_df = kpis[kpi_name]

        with pg_engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))

        kpi_df.to_sql(
            name=table_name,
            con=pg_engine,
            if_exists="append",
            index=False,
            chunksize=500,
            method="multi",
        )
        log.info(f"✔ Loaded {len(kpi_df)} rows into {table_name}")


def verify_load(pg_engine) -> None:
    """Confirm all tables have data in PostgreSQL."""
    tables = [
        "flight_facts",
        "kpi_airline_summary",
        "kpi_seasonality_summary",
        "kpi_route_summary",
        "kpi_booking_leadtime",
    ]
    with pg_engine.connect() as conn:
        for table in tables:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table}")
            ).scalar()
            log.info(f"  ✔ {table}: {count} rows")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def run():
    mysql_engine = create_engine(MYSQL_CONN)
    pg_engine    = create_engine(POSTGRES_CONN)

    df   = extract(mysql_engine)
    df   = validate(df)
    df   = transform(df)
    kpis = compute_kpis(df)
    load(df, kpis, pg_engine)
    # verify_load(pg_engine)

    log.info("🚀 Validation, transformation and loading complete")


if __name__ == "__main__":
    run()