"""
gold_aggregation.py
=====================
AWS Glue Job — Gold Layer (Analytics-Ready / BI Layer)

Purpose
-------
Transforms the cleansed Silver Delta Lake table into analytics-ready
aggregates consumed by Athena -> Power BI dashboards for AML trend
monitoring and fraud-alert reporting, and writes final scored fraud
alerts into Redshift for the compliance team's operational workflows.

Responsibilities
----------------
1. Compute a composite fraud risk score per transaction (rules + weighted
   signals from Silver: velocity, geo-risk, high-risk MCC).
2. Aggregate daily / hourly KPIs: transaction volume, flagged volume,
   fraud rate, exposure amount, by region/channel.
3. Write partitioned, compressed Parquet to the Gold S3 zone optimized for
   Athena (partition pruning on txn_date, region) — cuts scan volume.
4. Load top-line fraud alert + KPI tables into Amazon Redshift for the
   Power BI semantic layer.

Tech: AWS Glue 4.0, PySpark, Amazon Athena, Amazon Redshift
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_DELTA_PATH",
        "GOLD_TRANSACTIONS_PATH",
        "GOLD_KPI_PATH",
        "REDSHIFT_CONNECTION",     # Glue connection name for Redshift
        "REDSHIFT_TEMP_DIR",       # S3 staging dir for Redshift COPY
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Weighted composite risk score — weights tuned against a labeled
# validation set during model calibration (see README for methodology).
RISK_WEIGHTS = {
    "velocity_flag": 0.35,
    "high_risk_mcc_flag": 0.25,
    "geo_risk_score": 0.25,     # already normalized 0-1 upstream
    "amount_outlier_flag": 0.15,
}


def load_silver():
    return spark.read.format("delta").load(args["SILVER_DELTA_PATH"])


def score_fraud_risk(df):
    # Flag statistical amount outliers per account using z-score vs the
    # account's trailing 30-day average (approximated via account-level agg
    # joined back for this batch).
    account_stats = df.groupBy("account_id").agg(
        F.avg("amount").alias("avg_amount"),
        F.stddev("amount").alias("std_amount"),
    )
    df = df.join(account_stats, on="account_id", how="left")
    df = df.withColumn(
        "amount_outlier_flag",
        F.when(
            F.col("std_amount") > 0,
            F.abs(F.col("amount") - F.col("avg_amount")) / F.col("std_amount") > 3,
        ).otherwise(F.lit(False)),
    )

    df = df.withColumn(
        "fraud_risk_score",
        F.round(
            F.col("velocity_flag").cast("double") * RISK_WEIGHTS["velocity_flag"]
            + F.col("high_risk_mcc_flag").cast("double") * RISK_WEIGHTS["high_risk_mcc"]
            + F.col("geo_risk_score") * RISK_WEIGHTS["geo_risk_score"]
            + F.col("amount_outlier_flag").cast("double") * RISK_WEIGHTS["amount_outlier_flag"],
            4,
        ),
    ).withColumn(
        "fraud_alert",
        F.col("fraud_risk_score") >= 0.6,
    )
    return df


def build_gold_transactions(df):
    return df.select(
        "transaction_id",
        "account_id",
        "card_number_token",
        "amount",
        "currency",
        "country_code",
        "mcc_code",
        "channel",
        "transaction_ts",
        "ingest_date",
        F.col("ingest_date").alias("txn_date"),
        "region",
        "fraud_risk_score",
        "fraud_alert",
        "velocity_flag",
        "high_risk_mcc_flag",
        "amount_outlier_flag",
    )


def build_kpi_aggregates(gold_df):
    return (
        gold_df.groupBy("txn_date", "region", "channel")
        .agg(
            F.count("transaction_id").alias("txn_volume"),
            F.sum("amount").alias("txn_amount_total"),
            F.sum(F.col("fraud_alert").cast("int")).alias("flagged_txn_count"),
            F.sum(F.when(F.col("fraud_alert"), F.col("amount")).otherwise(0)).alias(
                "flagged_exposure_amount"
            ),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("flagged_txn_count") / F.col("txn_volume") * 100, 3),
        )
    )


def write_gold_s3(gold_df, kpi_df):
    (
        gold_df.repartition("txn_date", "region")
        .write.mode("overwrite")
        .partitionBy("txn_date", "region")
        .option("compression", "snappy")
        .parquet(args["GOLD_TRANSACTIONS_PATH"])
    )
    (
        kpi_df.write.mode("overwrite")
        .partitionBy("txn_date")
        .option("compression", "snappy")
        .parquet(args["GOLD_KPI_PATH"])
    )


def write_redshift(gold_df, kpi_df):
    """Load flagged alerts + KPI rollups into Redshift for the Power BI
    semantic model consumed by the compliance team."""
    alerts_df = gold_df.filter(F.col("fraud_alert") == True)  # noqa: E712

    for df, table in [(alerts_df, "fraud_alerts"), (kpi_df, "daily_fraud_kpis")]:
        dyf = glueContext.create_dynamic_frame.from_dataframe(df, glueContext, table)
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf,
            catalog_connection=args["REDSHIFT_CONNECTION"],
            connection_options={
                "dbtable": f"public.{table}",
                "database": "fraud_analytics",
            },
            redshift_tmp_dir=args["REDSHIFT_TEMP_DIR"],
        )


def run():
    silver_df = load_silver()
    scored_df = score_fraud_risk(silver_df)
    gold_df = build_gold_transactions(scored_df)
    kpi_df = build_kpi_aggregates(gold_df)

    write_gold_s3(gold_df, kpi_df)
    write_redshift(gold_df, kpi_df)

    print(f"[gold_aggregation] Gold rows: {gold_df.count()} | "
          f"Alerts: {gold_df.filter(F.col('fraud_alert')).count()}")


if __name__ == "__main__":
    run()
    job.commit()
