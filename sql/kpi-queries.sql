-- =============================================================================
-- kpi_queries.sql
-- Analytics queries powering the AML trend / fraud-alert Power BI dashboards.
-- Written against both Amazon Athena (Gold S3 Parquet tables) and
-- Amazon Redshift (fraud_analytics schema) — noted per query.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Daily fraud rate trend (Athena — Gold layer, partition-pruned on txn_date)
-- -----------------------------------------------------------------------------
SELECT
    txn_date,
    region,
    COUNT(*)                                            AS txn_volume,
    SUM(CASE WHEN fraud_alert THEN 1 ELSE 0 END)         AS flagged_txn_count,
    ROUND(
        SUM(CASE WHEN fraud_alert THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 3
    )                                                    AS fraud_rate_pct
FROM fraud_analytics.gold_transactions
WHERE txn_date BETWEEN date_add('day', -30, current_date) AND current_date
GROUP BY txn_date, region
ORDER BY txn_date DESC, region;


-- -----------------------------------------------------------------------------
-- 2. Top flagged accounts by exposure amount, last 7 days (Redshift)
-- -----------------------------------------------------------------------------
SELECT
    account_id,
    COUNT(*)               AS flagged_txn_count,
    SUM(amount)              AS total_exposure_amount,
    MAX(fraud_risk_score)     AS max_risk_score
FROM fraud_analytics.fraud_alerts
WHERE txn_date >= DATEADD(day, -7, GETDATE())
  AND fraud_alert = TRUE
GROUP BY account_id
ORDER BY total_exposure_amount DESC
LIMIT 25;


-- -----------------------------------------------------------------------------
-- 3. AML geo-risk exposure — flagged transaction volume by high-risk country
--    (Redshift, joined to geo risk dimension)
-- -----------------------------------------------------------------------------
SELECT
    fa.country_code,
    g.country_name,
    g.fatf_watchlist,
    COUNT(*)          AS flagged_txn_count,
    SUM(fa.amount)      AS flagged_amount_total
FROM fraud_analytics.fraud_alerts fa
JOIN fraud_analytics.dim_geo_risk g
  ON fa.country_code = g.country_code
WHERE fa.fraud_alert = TRUE
GROUP BY fa.country_code, g.country_name, g.fatf_watchlist
ORDER BY flagged_amount_total DESC;


-- -----------------------------------------------------------------------------
-- 4. Channel breakdown of fraud rate (POS vs Online vs ATM vs Mobile vs Wire)
--    (Athena)
-- -----------------------------------------------------------------------------
SELECT
    channel,
    COUNT(*)                                          AS txn_volume,
    SUM(CASE WHEN fraud_alert THEN 1 ELSE 0 END)        AS flagged_count,
    ROUND(AVG(fraud_risk_score), 4)                      AS avg_risk_score
FROM fraud_analytics.gold_transactions
WHERE txn_date = current_date - interval '1' day
GROUP BY channel
ORDER BY flagged_count DESC;


-- -----------------------------------------------------------------------------
-- 5. Reconciliation discrepancy audit report — unresolved mismatches between
--    streamed and batch source-of-truth data (Redshift)
-- -----------------------------------------------------------------------------
SELECT
    discrepancy_type,
    COUNT(*)                          AS discrepancy_count,
    SUM(ABS(COALESCE(stream_amount, 0) - COALESCE(batch_amount, 0))) AS total_variance_amount
FROM fraud_analytics.reconciliation_discrepancies
WHERE resolved = FALSE
  AND txn_date >= DATEADD(day, -1, GETDATE())
GROUP BY discrepancy_type
ORDER BY discrepancy_count DESC;


-- -----------------------------------------------------------------------------
-- 6. Reconciliation accuracy rate — used to compute the 98.5% reconciliation
--    accuracy KPI reported in the compliance daily summary (Redshift)
-- -----------------------------------------------------------------------------
WITH totals AS (
    SELECT COUNT(*) AS total_txns
    FROM fraud_analytics.fraud_alerts
    WHERE txn_date = DATEADD(day, -1, CAST(GETDATE() AS DATE))
),
mismatches AS (
    SELECT COUNT(*) AS mismatch_count
    FROM fraud_analytics.reconciliation_discrepancies
    WHERE txn_date = DATEADD(day, -1, CAST(GETDATE() AS DATE))
)
SELECT
    t.total_txns,
    m.mismatch_count,
    ROUND(100.0 * (t.total_txns - m.mismatch_count) / NULLIF(t.total_txns, 0), 2) AS reconciliation_accuracy_pct
FROM totals t, mismatches m;


-- -----------------------------------------------------------------------------
-- 7. High-risk MCC transaction monitor (velocity + MCC combined signal)
--    (Athena)
-- -----------------------------------------------------------------------------
SELECT
    account_id,
    mcc_code,
    COUNT(*)              AS txn_count,
    SUM(amount)             AS total_amount,
    bool_or(velocity_flag)   AS any_velocity_flag
FROM fraud_analytics.gold_transactions
WHERE high_risk_mcc_flag = TRUE
  AND txn_date = current_date - interval '1' day
GROUP BY account_id, mcc_code
HAVING COUNT(*) > 3
ORDER BY total_amount DESC;
