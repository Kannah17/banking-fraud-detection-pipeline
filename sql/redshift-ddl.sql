-- =============================================================================
-- redshift_ddl.sql
-- Table creation scripts for the Amazon Redshift analytics layer that backs
-- the Power BI AML / fraud-alert dashboards.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS fraud_analytics;

-- -----------------------------------------------------------------------------
-- fraud_alerts: transaction-level records flagged by the Gold-layer
-- composite risk scoring model (gold_aggregation.py). Loaded daily.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_analytics.fraud_alerts (
    transaction_id       VARCHAR(64)     NOT NULL,
    account_id           VARCHAR(32)     NOT NULL,
    card_number_token    VARCHAR(64),
    amount               DECIMAL(18,2)   NOT NULL,
    currency             CHAR(3)         NOT NULL,
    country_code         CHAR(2),
    mcc_code             VARCHAR(8),
    channel              VARCHAR(16),
    transaction_ts        TIMESTAMP       NOT NULL,
    txn_date             DATE            NOT NULL,
    region                VARCHAR(8),
    fraud_risk_score      DECIMAL(6,4)    NOT NULL,
    fraud_alert           BOOLEAN         NOT NULL,
    velocity_flag         BOOLEAN,
    high_risk_mcc_flag    BOOLEAN,
    amount_outlier_flag   BOOLEAN,
    loaded_at             TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (account_id)
SORTKEY (txn_date, region);

-- -----------------------------------------------------------------------------
-- daily_fraud_kpis: pre-aggregated KPI rollups powering the Power BI
-- trend dashboards (volume, exposure, fraud rate by region/channel/day).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_analytics.daily_fraud_kpis (
    txn_date               DATE            NOT NULL,
    region                  VARCHAR(8),
    channel                 VARCHAR(16),
    txn_volume               BIGINT,
    txn_amount_total          DECIMAL(20,2),
    flagged_txn_count         BIGINT,
    flagged_exposure_amount   DECIMAL(20,2),
    fraud_rate_pct            DECIMAL(6,3),
    loaded_at                TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (txn_date, region);

-- -----------------------------------------------------------------------------
-- reconciliation_discrepancies: mismatches between the streamed pipeline
-- and the core-banking source-of-truth batch extract, flagged for the
-- compliance audit trail by the nightly Airflow reconciliation task.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_analytics.reconciliation_discrepancies (
    transaction_id      VARCHAR(64),
    discrepancy_type     VARCHAR(32)     NOT NULL,
    stream_amount         DECIMAL(18,2),
    batch_amount          DECIMAL(18,2),
    txn_date              DATE,
    detected_at            TIMESTAMP DEFAULT GETDATE(),
    resolved                BOOLEAN DEFAULT FALSE,
    resolution_notes         VARCHAR(512)
)
DISTSTYLE EVEN
SORTKEY (txn_date, discrepancy_type);

-- -----------------------------------------------------------------------------
-- reference dimension: merchant category / MCC risk classification
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_analytics.dim_merchant_category (
    mcc_code        VARCHAR(8)      NOT NULL PRIMARY KEY,
    mcc_description  VARCHAR(128),
    risk_tier         VARCHAR(16)     -- LOW / MEDIUM / HIGH
)
DISTSTYLE ALL;

-- -----------------------------------------------------------------------------
-- reference dimension: country / geo AML risk scoring
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fraud_analytics.dim_geo_risk (
    country_code     CHAR(2)         NOT NULL PRIMARY KEY,
    country_name      VARCHAR(64),
    geo_risk_score     DECIMAL(4,3),   -- normalized 0.000 - 1.000
    fatf_watchlist      BOOLEAN DEFAULT FALSE
)
DISTSTYLE ALL;
