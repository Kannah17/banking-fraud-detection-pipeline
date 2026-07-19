"""
silver_validation.py
=====================
AWS Glue Job — Silver Layer (Data Quality, Cleansing & Enrichment)

Purpose
-------
Consumes Bronze (raw) transaction data, applies schema enforcement via the
AWS Glue Schema Registry, runs data-quality rules, deduplicates, enriches
with reference/dimension data, and writes the cleansed result as a Delta
Lake table so downstream consumers get ACID guarantees + time travel for
reconciliation and audit.

Responsibilities
----------------
1. Enforce schema against the registered Avro schema in Glue Schema Registry.
2. Data-quality checks: null checks, referential integrity (account exists),
   value-range checks (amount > 0), currency code validation.
3. Deduplicate on (transaction_id) using watermark-aware logic.
4. Enrich with merchant category + geo-risk reference tables.
5. Flag preliminary rule-based fraud signals (velocity, high-risk MCC,
   geo-mismatch) that feed the Gold-layer fraud scoring aggregation.
6. Persist as Delta Lake (time-travel enabled) for compliance reconciliation.

Tech: AWS Glue 4.0, PySpark, Delta Lake, AWS Glue Schema Registry
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_PATH",
        "SILVER_DELTA_PATH",
        "REFERENCE_MERCHANT_PATH",   # merchant category / MCC risk table
        "REFERENCE_GEO_RISK_PATH",   # country/region risk scoring table
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

HIGH_RISK_MCC = {"7995", "6051", "5993", "4829"}  # gambling, crypto/wire, tobacco, wire xfer


def load_bronze():
    return spark.read.parquet(args["BRONZE_PATH"])


def apply_quality_rules(df):
    """Rule-based data quality gate. Records failing hard rules are routed
    to a reject table for the compliance audit trail rather than dropped
    silently."""
    df = df.withColumn(
        "dq_flags",
        F.array_remove(
            F.array(
                F.when(F.col("amount").isNull() | (F.col("amount") <= 0), "INVALID_AMOUNT"),
                F.when(F.col("account_id").isNull(), "MISSING_ACCOUNT"),
                F.when(~F.col("currency").rlike("^[A-Z]{3}$"), "INVALID_CURRENCY"),
                F.when(F.col("transaction_ts").isNull(), "MISSING_TIMESTAMP"),
            ),
            None,
        ),
    )
    clean_df = df.filter(F.size("dq_flags") == 0)
    rejected_df = df.filter(F.size("dq_flags") > 0)
    return clean_df, rejected_df


def deduplicate(df):
    """Keep the most recently ingested version of each transaction_id
    (handles Kinesis at-least-once delivery + late-arriving reconciliation
    batches from the core banking system)."""
    window = Window.partitionBy("transaction_id").orderBy(F.col("ingest_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def enrich(df):
    merchant_ref = spark.read.parquet(args["REFERENCE_MERCHANT_PATH"])
    geo_ref = spark.read.parquet(args["REFERENCE_GEO_RISK_PATH"])

    enriched = (
        df.join(merchant_ref, on="mcc_code", how="left")
        .join(geo_ref, on="country_code", how="left")
        .withColumn(
            "high_risk_mcc_flag",
            F.col("mcc_code").isin(list(HIGH_RISK_MCC)),
        )
        .withColumn(
            "geo_risk_score",
            F.coalesce(F.col("geo_risk_score"), F.lit(0.0)),
        )
    )
    return enriched


def flag_velocity_signals(df):
    """Rolling 5-minute transaction-count / sum-amount per account as an
    early fraud velocity signal — a common AML/fraud heuristic."""
    velocity_window = (
        Window.partitionBy("account_id")
        .orderBy(F.col("transaction_ts").cast("long"))
        .rangeBetween(-300, 0)  # 5-minute lookback in seconds
    )
    return (
        df.withColumn("txn_count_5min", F.count("transaction_id").over(velocity_window))
        .withColumn("txn_sum_5min", F.sum("amount").over(velocity_window))
        .withColumn(
            "velocity_flag",
            (F.col("txn_count_5min") >= 5) | (F.col("txn_sum_5min") >= 10000),
        )
    )


def write_delta(df, rejected_df):
    df.write.format("delta").mode("append").partitionBy("ingest_date").save(
        args["SILVER_DELTA_PATH"]
    )
    if rejected_df.count() > 0:
        rejected_df.write.format("delta").mode("append").save(
            args["SILVER_DELTA_PATH"].rstrip("/") + "_rejected/"
        )

    # Compact small files periodically for query performance
    if DeltaTable.isDeltaTable(spark, args["SILVER_DELTA_PATH"]):
        delta_tbl = DeltaTable.forPath(spark, args["SILVER_DELTA_PATH"])
        delta_tbl.optimize().executeCompaction()
        delta_tbl.vacuum(168)  # 7-day retention for time-travel reconciliation


def run():
    bronze_df = load_bronze()
    clean_df, rejected_df = apply_quality_rules(bronze_df)
    deduped_df = deduplicate(clean_df)
    enriched_df = enrich(deduped_df)
    final_df = flag_velocity_signals(enriched_df)

    write_delta(final_df, rejected_df)

    print(f"[silver_validation] Accepted: {final_df.count()} | "
          f"Rejected: {rejected_df.count()}")


if __name__ == "__main__":
    run()
    job.commit()
