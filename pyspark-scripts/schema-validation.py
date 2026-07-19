"""
schema_validation.py
======================
Schema enforcement & evolution handling for the transaction pipeline,
backed by the AWS Glue Schema Registry (Avro schemas, backward-compatible
evolution mode).

Why this exists
---------------
Upstream core-banking and card-network producers occasionally add new
optional fields (e.g. a new `wallet_provider` column) or deprecate old
ones. Without a governed schema-evolution strategy this silently breaks
downstream Delta/Athena consumers. This module:

1. Fetches the latest registered schema version from Glue Schema Registry.
2. Validates incoming batches against it (structural + type checks).
3. Applies a compatibility policy (BACKWARD) — new optional fields are
   allowed; required-field removal or type-narrowing fails the batch and
   raises a SchemaCompatibilityError for the Airflow DAG to catch and page
   the data-eng on-call.
4. Safely merges/aligns evolved schemas onto the canonical Delta table
   schema using Delta's schema-merge capability.
"""

import json
import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class SchemaCompatibilityError(Exception):
    pass


GLUE_SCHEMA_REGISTRY_NAME = "fraud-pipeline-registry"
GLUE_SCHEMA_NAME = "transaction-event"

REQUIRED_FIELDS = {
    "transaction_id",
    "account_id",
    "amount",
    "currency",
    "transaction_ts",
    "mcc_code",
    "country_code",
    "channel",
}


def get_glue_client(region: str = "us-east-1"):
    return boto3.client("glue", region_name=region)


def fetch_latest_schema(glue_client) -> dict:
    """Retrieve the latest Avro schema definition registered for the
    transaction-event schema in AWS Glue Schema Registry."""
    response = glue_client.get_schema_version(
        SchemaId={
            "SchemaName": GLUE_SCHEMA_NAME,
            "RegistryName": GLUE_SCHEMA_REGISTRY_NAME,
        },
        SchemaVersionNumber={"LatestVersion": True},
    )
    return json.loads(response["SchemaDefinition"])


def validate_required_fields(df: DataFrame):
    incoming_fields = set(df.columns)
    missing = REQUIRED_FIELDS - incoming_fields
    if missing:
        raise SchemaCompatibilityError(
            f"Batch is missing required fields (breaking change): {missing}"
        )


def validate_types(df: DataFrame, registered_schema: dict):
    """Compares the incoming Spark schema's field types against the types
    declared in the registered Avro schema; flags narrowing/incompatible
    type changes (e.g. amount: double -> string)."""
    avro_type_map = {f["name"]: f["type"] for f in registered_schema.get("fields", [])}
    spark_fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    incompatible = []
    for field_name, avro_type in avro_type_map.items():
        if field_name in spark_fields:
            expected = _normalize_type(avro_type)
            actual = spark_fields[field_name]
            if expected and expected != actual and not _is_widening(expected, actual):
                incompatible.append((field_name, expected, actual))

    if incompatible:
        raise SchemaCompatibilityError(
            f"Incompatible type changes detected (BACKWARD compatibility broken): "
            f"{incompatible}"
        )


def _normalize_type(avro_type) -> str:
    mapping = {"string": "string", "double": "double", "long": "bigint",
               "int": "int", "boolean": "boolean", "float": "float"}
    if isinstance(avro_type, list):  # nullable union e.g. ["null", "string"]
        avro_type = [t for t in avro_type if t != "null"]
        avro_type = avro_type[0] if avro_type else None
    return mapping.get(avro_type, avro_type)


def _is_widening(expected: str, actual: str) -> bool:
    """Permit safe widening conversions (int -> bigint -> double) under
    BACKWARD compatibility mode."""
    widen_chain = ["int", "bigint", "float", "double"]
    if expected in widen_chain and actual in widen_chain:
        return widen_chain.index(actual) >= widen_chain.index(expected)
    return False


def validate_batch(df: DataFrame, region: str = "us-east-1") -> DataFrame:
    """Entry point called by the Bronze/Silver Glue jobs and the Airflow
    DAG's pre-flight validation task."""
    glue_client = get_glue_client(region)
    registered_schema = fetch_latest_schema(glue_client)

    validate_required_fields(df)
    validate_types(df, registered_schema)

    print(f"[schema_validation] Batch validated OK against schema version "
          f"for '{GLUE_SCHEMA_NAME}' ({len(df.columns)} columns)")
    return df


def align_to_delta_schema(spark: SparkSession, df: DataFrame, delta_table_path: str) -> DataFrame:
    """Uses Delta Lake's mergeSchema to safely absorb new optional columns
    into the canonical Silver table schema without a full rewrite."""
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(delta_table_path)
    )
    return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("schema_validation").getOrCreate()
    sample_df = spark.read.parquet("s3://fraud-pipeline-bronze/transactions/")
    validate_batch(sample_df)
