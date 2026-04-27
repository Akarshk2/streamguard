-- =============================================================================
-- StreamGuard — Snowflake DDL
-- Run this entire script in your Snowflake worksheet before starting the project
-- =============================================================================

-- ─── DATABASE & WAREHOUSE ─────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS STREAMGUARD_DB;
USE DATABASE STREAMGUARD_DB;

CREATE WAREHOUSE IF NOT EXISTS STREAMGUARD_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
       AUTO_SUSPEND   = 60
       AUTO_RESUME    = TRUE
       INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE STREAMGUARD_WH;

-- ─── SCHEMAS ──────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS METRICS;
CREATE SCHEMA IF NOT EXISTS ALERTS;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- =============================================================================
-- RAW LAYER — Raw ingested events from PySpark consumers
-- =============================================================================

CREATE TABLE IF NOT EXISTS RAW.ORDERS_RAW (
    order_id        VARCHAR(100),
    customer_id     VARCHAR(50),
    product_id      VARCHAR(50),
    quantity        INTEGER,
    amount          FLOAT,
    currency        VARCHAR(10),
    status          VARCHAR(50),
    payment_method  VARCHAR(50),
    region          VARCHAR(10),
    event_time      VARCHAR(50),
    created_at      VARCHAR(50),
    ingested_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_topic    VARCHAR(100),
    ingest_date     DATE
);

CREATE TABLE IF NOT EXISTS RAW.CLICKSTREAM_RAW (
    session_id      VARCHAR(100),
    user_id         VARCHAR(50),
    page            VARCHAR(200),
    action          VARCHAR(50),
    device          VARCHAR(50),
    browser         VARCHAR(50),
    referrer        VARCHAR(100),
    duration_ms     INTEGER,
    region          VARCHAR(10),
    event_time      VARCHAR(50),
    ingested_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_topic    VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS RAW.INVENTORY_RAW (
    sku             VARCHAR(50),
    warehouse_id    VARCHAR(50),
    stock_level     INTEGER,
    reserved_qty    INTEGER,
    restock_flag    BOOLEAN,
    restock_qty     INTEGER,
    unit_cost       FLOAT,
    last_movement   VARCHAR(50),
    event_time      VARCHAR(50),
    ingested_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_topic    VARCHAR(100)
);

-- =============================================================================
-- METRICS LAYER — Stream profile statistics for ML anomaly scoring
-- =============================================================================

CREATE SEQUENCE IF NOT EXISTS METRICS.PROFILE_ID_SEQ START = 1 INCREMENT = 1;

CREATE TABLE IF NOT EXISTS METRICS.STREAM_PROFILES (
    profile_id          INTEGER DEFAULT METRICS.PROFILE_ID_SEQ.NEXTVAL PRIMARY KEY,
    topic               VARCHAR(100)    NOT NULL,
    batch_id            INTEGER         NOT NULL,
    record_count        INTEGER         NOT NULL,
    null_rate_avg       FLOAT           NOT NULL,
    null_rate_max       FLOAT           NOT NULL,
    schema_hash         VARCHAR(100)    NOT NULL,
    column_count        INTEGER         NOT NULL,
    expected_schema_hash VARCHAR(100),
    null_rates_json     VARCHAR(4000),
    profile_ts          VARCHAR(50),
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- ALERTS LAYER — ML anomaly log, quarantine log, schema change history
-- =============================================================================

CREATE SEQUENCE IF NOT EXISTS ALERTS.ANOMALY_ID_SEQ START = 1 INCREMENT = 1;

CREATE TABLE IF NOT EXISTS ALERTS.ANOMALY_LOG (
    anomaly_id      INTEGER DEFAULT ALERTS.ANOMALY_ID_SEQ.NEXTVAL PRIMARY KEY,
    profile_id      INTEGER,
    topic           VARCHAR(100),
    batch_id        INTEGER,
    anomaly_score   FLOAT,
    severity        VARCHAR(20),    -- NORMAL, MEDIUM, HIGH, CRITICAL
    is_schema_drift BOOLEAN DEFAULT FALSE,
    scored_at       VARCHAR(50),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS ALERTS.QUARANTINE_LOG (
    id              INTEGER AUTOINCREMENT PRIMARY KEY,
    topic           VARCHAR(100),
    batch_id        INTEGER,
    reject_reason   VARCHAR(100),
    record_json     VARIANT,
    quarantine_ts   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS ALERTS.SCHEMA_CHANGES (
    id              INTEGER AUTOINCREMENT PRIMARY KEY,
    topic           VARCHAR(100),
    table_name      VARCHAR(200),
    new_columns     VARCHAR(2000),
    batch_id        INTEGER,
    detected_at     VARCHAR(50),
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- GRANTS
-- =============================================================================
GRANT USAGE  ON DATABASE   STREAMGUARD_DB    TO ROLE SYSADMIN;
GRANT USAGE  ON ALL SCHEMAS IN DATABASE STREAMGUARD_DB TO ROLE SYSADMIN;
GRANT ALL    ON ALL TABLES  IN DATABASE STREAMGUARD_DB TO ROLE SYSADMIN;
GRANT ALL    ON ALL SEQUENCES IN DATABASE STREAMGUARD_DB TO ROLE SYSADMIN;

-- =============================================================================
-- VERIFY
-- =============================================================================
SHOW TABLES IN DATABASE STREAMGUARD_DB;
