-- Active: 1765810125651@@127.0.0.1@5432@flight_analytics_bd
CREATE DATABASE IF NOT EXISTS flight_staging;
USE flight_staging;

CREATE TABLE IF NOT EXISTS raw_flights (
    id                      INT AUTO_INCREMENT PRIMARY KEY,

    -- ── Airline & Route ───────────────────────────────────────────────────
    airline                 VARCHAR(100)        NOT NULL,
    source                  VARCHAR(10)         NOT NULL,       -- IATA code e.g DAC
    source_name             VARCHAR(255)        NOT NULL,       -- Full airport name
    destination             VARCHAR(10)         NOT NULL,       -- IATA code e.g LHR
    destination_name        VARCHAR(255)        NOT NULL,       -- Full airport name

    -- ── Schedule ──────────────────────────────────────────────────────────
    departure_datetime      DATETIME            NOT NULL,       -- 2025-03-15 14:30:00
    arrival_datetime        DATETIME            NOT NULL,       -- 2025-03-16 02:45:00
    duration_hrs            DECIMAL(6,2)        NOT NULL,       -- 12.25

    -- ── Flight Details ────────────────────────────────────────────────────
    stopovers               VARCHAR(50)         NOT NULL,       -- Direct, 1 Stop, 2 Stops
    aircraft_type           VARCHAR(100)        NOT NULL,       -- Boeing 777
    class                   VARCHAR(50)         NOT NULL,       -- Economy, Business, First

    -- ── Booking ───────────────────────────────────────────────────────────
    booking_source          VARCHAR(100)        NOT NULL,       -- Direct, Agency, Online
    days_before_departure   INT                 NOT NULL,       -- 1 - 90

    -- ── Pricing (BDT) ─────────────────────────────────────────────────────
    base_fare_bdt           DECIMAL(12,2)       NOT NULL,
    tax_surcharge_bdt       DECIMAL(12,2)       NOT NULL,
    total_fare_bdt          DECIMAL(12,2)       NOT NULL,

    -- ── Seasonality ───────────────────────────────────────────────────────
    seasonality             VARCHAR(50)         NOT NULL,       -- Regular, Eid, Hajj, Winter

    -- ── Pipeline Metadata ─────────────────────────────────────────────────
    ingested_at             TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
    source_file             VARCHAR(255)        DEFAULT NULL
);