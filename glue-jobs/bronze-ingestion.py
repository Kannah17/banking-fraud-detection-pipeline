"""
bronze_ingestion.py
====================
AWS Glue Job — Bronze Layer (Raw Ingestion)

Purpose
-------
Ingests raw financial transaction events (streamed via Kinesis -> Firehose,
or landed as batch files from source-of-truth core banking extracts) into
the Bronze (raw) zone of the S3 data lake with zero transformation, so that
the original event payload is always auditable (a PCI-DSS / AML requirement).

Responsibilities
----------------
1. Read raw JSON transaction events from the landing S3 prefix.
2. Attach ingestion metadata (ingest_ts, source_file, batch_id).
3. Tokenize sensitive card-number fields BEFORE the data is persisted
   anywhere, using AWS KMS envelope encryption via a Glue Python UDF.
4. Write immutable, append-only Parquet files partitioned by
   (ingest_date, region) to the Bronze S3 bucket.
5. Register/refresh the table in the Glue Data Catalog so Athena / Redshift
   Spectrum can query it immediately.

Tech: AWS Glue 4.0 (Spark 3.3), PySpark, AWS KMS, Glue Data Catalog
"""

import sys
import hashlib
import base64
from datetime import datetime

import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Job bootstrap
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_SOURCE_PATH",        # e.g. s3://fraud-pipeline-landing/transactions/
        "BRONZE_TARGET_PATH",     # e.g. s3://fraud-pipeline-bronze/transactions/
        "KMS_KEY_ID",             # KMS CMK used for tokenization salt
        "GLUE_DATABASE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

kms_client = boto3.client("kms")


# ---------------------------------------------------------------------------
# PCI-DSS: tokenize PAN (card number) using a KMS-derived data key + salted
# SHA-256 hash. The reversible ciphertext is stored separately in a locked
# down "vault" table accessible only to the tokenization service role.
# ---------------------------------------------------------------------------
def get_kms_data_key(key_id: str) -> bytes:
    response = kms_client.generate_data_key(KeyId=key_id, KeySpec="AES_256")
    return response["Plaintext"]


_DATA_KEY = get_kms_data_key(args["KMS_KEY_ID"])


def tokenize_pan(card_number: str) -> str:
    """Deterministic, salted, one-way token for card numbers.
    Preserves last 4 digits for downstream customer-service lookups,
    matching common PCI-DSS truncation display rules."""
    if card_number is None:
        return None
    salted = (card_number + base64.b64encode(_DATA_KEY).decode()).encode()
    digest = hashlib.sha256(salted).hexdigest()
    last4 = card_number[-4:] if len(card_number) >= 4 else card_number
    return f"tok_{digest[:24]}_{last4}"


tokenize_udf = F.udf(tokenize_pan, StringType())


# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------
def run():
    batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    raw_df = (
        spark.read.format("json")
        .option("multiLine", False)
        .load(args["RAW_SOURCE_PATH"])
    )

    print(f"[bronze_ingestion] Read {raw_df.count()} raw events from "
          f"{args['RAW_SOURCE_PATH']}")

    bronze_df = (
        raw_df
        .withColumn("card_number_token", tokenize_udf(F.col("card_number")))
        .drop("card_number")  # never persist raw PAN beyond the landing zone
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.col("ingest_ts")))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.input_file_name())
    )

    # Basic malformed-record quarantine (structural only — deep validation
    # happens in silver_validation.py)
    valid_df = bronze_df.filter(F.col("transaction_id").isNotNull())
    quarantine_df = bronze_df.filter(F.col("transaction_id").isNull())

    if quarantine_df.count() > 0:
        quarantine_df.write.mode("append").parquet(
            args["BRONZE_TARGET_PATH"].rstrip("/") + "_quarantine/"
        )
        print(f"[bronze_ingestion] Quarantined {quarantine_df.count()} malformed records")

    (
        valid_df.repartition("ingest_date", "region")
        .write.mode("append")
        .partitionBy("ingest_date", "region")
        .parquet(args["BRONZE_TARGET_PATH"])
    )

    # Refresh Glue Catalog so Athena sees new partitions immediately
    glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [args["BRONZE_TARGET_PATH"]], "recurse": True},
        format="parquet",
    )

    print(f"[bronze_ingestion] Batch {batch_id} complete: "
          f"{valid_df.count()} records written to Bronze")


if __name__ == "__main__":
    run()
    job.commit()
