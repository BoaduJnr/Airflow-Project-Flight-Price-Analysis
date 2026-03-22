-- Active: 1773951971635@@127.0.0.1@5432@flight_analytics_bd
-- ── KPI: airline summary ──────────────────────────────────────────────────
DROP TABLE IF EXISTS flight_facts;
CREATE TABLE flight_facts (
    id                      SERIAL          PRIMARY KEY,
    airline                 VARCHAR(100)    NOT NULL,
    source                  VARCHAR(10)     NOT NULL,
    source_name             VARCHAR(255)    NOT NULL,
    destination             VARCHAR(10)     NOT NULL,
    destination_name        VARCHAR(255)    NOT NULL,
    departure_datetime      TIMESTAMP       NOT NULL,
    arrival_datetime        TIMESTAMP       NOT NULL,
    duration_hrs            NUMERIC(6,2)    NOT NULL,
    stopovers               VARCHAR(50)     NOT NULL,
    aircraft_type           VARCHAR(100)    NOT NULL,
    class                   VARCHAR(50)     NOT NULL,
    booking_source          VARCHAR(100)    NOT NULL,
    days_before_departure   INT             NOT NULL,
    base_fare_bdt           NUMERIC(12,2)   NOT NULL,
    tax_surcharge_bdt       NUMERIC(12,2)   NOT NULL,
    total_fare_bdt          NUMERIC(12,2)   NOT NULL,
    seasonality             VARCHAR(50)     NOT NULL,
    loaded_at               TIMESTAMP       DEFAULT NOW()
);

-- ── KPI: airline summary ──────────────────────────────────────────────────
DROP TABLE IF EXISTS kpi_airline_summary;

CREATE TABLE kpi_airline_summary (
    airline                 VARCHAR(100)    PRIMARY KEY,
    avg_fare_bdt            NUMERIC(12,2),
    min_fare_bdt            NUMERIC(12,2),
    max_fare_bdt            NUMERIC(12,2),
    total_flights           INT,
    avg_duration_hrs        NUMERIC(6,2),
    economy_pct             NUMERIC(5,2),
    business_pct            NUMERIC(5,2),
    first_class_pct               NUMERIC(5,2),
    computed_at             TIMESTAMP       DEFAULT NOW()
);

-- ── KPI: seasonality summary ──────────────────────────────────────────────
DROP TABLE IF EXISTS kpi_seasonality_summary;

CREATE TABLE kpi_seasonality_summary (
    seasonality             VARCHAR(50)     PRIMARY KEY,
    avg_fare_bdt            NUMERIC(12,2),
    min_fare_bdt            NUMERIC(12,2),
    max_fare_bdt            NUMERIC(12,2),
    flight_count            INT,
    avg_days_before_dep     NUMERIC(6,2),
    computed_at             TIMESTAMP       DEFAULT NOW()
);

-- ── KPI: route summary ────────────────────────────────────────────────────
DROP TABLE IF EXISTS kpi_route_summary;

CREATE TABLE kpi_route_summary (
    route                   VARCHAR(50)     PRIMARY KEY,
    source                  VARCHAR(10),
    destination             VARCHAR(10),
    source_name             VARCHAR(255),
    destination_name        VARCHAR(255),
    avg_fare_bdt            NUMERIC(12,2),
    min_fare_bdt            NUMERIC(12,2),
    max_fare_bdt            NUMERIC(12,2),
    flight_count            INT,
    avg_duration_hrs        NUMERIC(6,2),
    direct_flight_pct       NUMERIC(5,2),
    computed_at             TIMESTAMP       DEFAULT NOW()
);

-- ── KPI: booking lead time ────────────────────────────────────────────────
DROP TABLE IF EXISTS kpi_booking_leadtime;

CREATE TABLE kpi_booking_leadtime (
    days_bucket             VARCHAR(50)     PRIMARY KEY,
    avg_fare_bdt            NUMERIC(12,2),
    min_fare_bdt            NUMERIC(12,2),
    max_fare_bdt            NUMERIC(12,2),
    flight_count            INT,
    computed_at             TIMESTAMP       DEFAULT NOW()
);