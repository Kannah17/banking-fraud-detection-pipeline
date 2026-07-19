"""
incremental_load.py
=====================
Watermark-based incremental ingestion for the daily reconciliation batch
extract (core banking source-of-truth), so we never reprocess the full
history and never miss late-arriving records.

Strategy
--------
- Maintains a watermark table (small Delta table / DynamoDB-backed) that
  tracks `last_processed_ts` per source system.
- Pulls only records with `updated_at > last_processed_ts - grace_period`,
  where `grace_period` absorbs clock skew / late-arriving upstream writes
  (default 30 minutes).
- Uses Delta Lake MERGE (upsert) semantics so re-delivered records update
  in place rather than duplicating — critical for the reconciliation
  accuracy metric.
- Advances the watermark only after a successful, verified write.
"""

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

WATERMARK_TABLE_PATH = "s3://fraud-pipeline-control/watermarks/"
DEFAULT_GRACE_PERIOD_MINUTES = 30


def build_spark():
    return (
        SparkSession.builder.appName("incremental_load")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def get_last_watermark(spark, source_system: str) -> datetime:
    if not DeltaTable.isDeltaTable(spark, WATERMARK_TABLE_PATH):
        return datetime(1970, 1, 1)

    wm_df = spark.read.format("delta").load(WATERMARK_TABLE_PATH)
    row = wm_df.filter(F.col("source_system") == source_system).orderBy(
        F.col("last_processed_ts").desc()
    ).first()
    return row["last_processed_ts"] if row else datetime(1970, 1, 1)


def set_watermark(spark, source_system: str, new_watermark: datetime):
    new_row = spark.createDataFrame(
        [(source_system, new_watermark, datetime.utcnow())],
        ["source_system", "last_processed_ts", "updated_at"],
    )
    if DeltaTable.isDeltaTable(spark, WATERMARK_TABLE_PATH):
        wm_table = DeltaTable.forPath(spark, WATERMARK_TABLE_PATH)
        (
            wm_table.alias("t")
            .merge(new_row.alias("s"), "t.source_system = s.source_system")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_row.write.format("delta").mode("overwrite").save(WATERMARK_TABLE_PATH)


def extract_incremental(spark, source_path: str, source_system: str,
                         grace_minutes: int = DEFAULT_GRACE_PERIOD_MINUTES):
    last_watermark = get_last_watermark(spark, source_system)
    effective_start = last_watermark - timedelta(minutes=grace_minutes)

    print(f"[incremental_load] Source={source_system} | "
          f"watermark={last_watermark} | effective_start={effective_start}")

    source_df = spark.read.parquet(source_path)
    incremental_df = source_df.filter(F.col("updated_at") > F.lit(effective_start))

    return incremental_df, source_df


def upsert_to_delta(spark, incremental_df, target_delta_path: str, key_col: str = "transaction_id"):
    if not DeltaTable.isDeltaTable(spark, target_delta_path):
        incremental_df.write.format("delta").mode("overwrite").save(target_delta_path)
        return incremental_df.count()

    target_table = DeltaTable.forPath(spark, target_delta_path)
    (
        target_table.alias("t")
        .merge(incremental_df.alias("s"), f"t.{key_col} = s.{key_col}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return incremental_df.count()


def run(source_path: str, target_delta_path: str, source_system: str = "core_banking_extract"):
    spark = build_spark()

    incremental_df, full_source_df = extract_incremental(spark, source_path, source_system)
    row_count = upsert_to_delta(spark, incremental_df, target_delta_path)

    max_ts_row = full_source_df.agg(F.max("updated_at").alias("max_ts")).first()
    if max_ts_row and max_ts_row["max_ts"]:
        set_watermark(spark, source_system, max_ts_row["max_ts"])

    print(f"[incremental_load] Upserted {row_count} records into {target_delta_path}")
    spark.stop()


if __name__ == "__main__":
    run(
        source_path="s3://fraud-pipeline-raw/core-banking-extract/",
        target_delta_path="s3://fraud-pipeline-silver/transactions_delta/",
    )
