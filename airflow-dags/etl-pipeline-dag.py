"""
etl_pipeline_dag.py
=====================
Apache Airflow DAG orchestrating the daily Banking Fraud Detection &
Transaction Analytics pipeline:

  1. Pre-flight schema validation against Glue Schema Registry
  2. Bronze ingestion (Glue job)
  3. Silver validation / cleansing / enrichment (Glue job)
  4. Incremental reconciliation against core-banking batch extract
  5. Gold aggregation + fraud scoring (Glue job)
  6. Athena partition refresh (MSCK REPAIR equivalent)
  7. Data-quality gate (Great-Expectations-style row-count / null checks)
  8. Compliance audit-trail notification (SNS) summarizing flagged
     discrepancies for the daily reconciliation cycle

Schedule: daily at 02:00 UTC, after the core-banking nightly batch extract
lands (05:30 UTC upstream SLA -> DAG scheduled with a sensor buffer).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.hooks.sns import SnsHook

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-eng-oncall@bank-example.com"],
}

S3_BUCKET_RAW = "fraud-pipeline-raw"
S3_BUCKET_BRONZE = "fraud-pipeline-bronze"
GLUE_CONNECTION = "aws_default"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:fraud-pipeline-alerts"


def validate_schema_preflight(**context):
    """Calls the schema_validation module against the latest landed batch
    before triggering any Glue jobs, failing fast on breaking changes."""
    from pyspark_scripts.schema_validation import validate_batch  # noqa
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("preflight_validation").getOrCreate()
    df = spark.read.parquet(f"s3://{S3_BUCKET_RAW}/transactions/")
    validate_batch(df)
    spark.stop()


def data_quality_gate(**context):
    """Row-count and null-rate checks on the Gold layer before it's
    considered safe for the Power BI dashboards to consume."""
    import boto3

    athena = boto3.client("athena")
    query = """
        SELECT COUNT(*) AS row_count,
               SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_ids
        FROM fraud_analytics.gold_transactions
        WHERE txn_date = current_date - interval '1' day
    """
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "fraud_analytics"},
        ResultConfiguration={"OutputLocation": f"s3://{S3_BUCKET_BRONZE}/athena-results/"},
    )
    context["ti"].xcom_push(key="dq_query_execution_id", value=response["QueryExecutionId"])


def branch_on_dq_result(**context):
    """Simple branch: in production this would poll Athena for the query
    result and compare against thresholds (e.g. row_count > 0, null rate
    < 0.1%). Routes to either the publish step or the alert step."""
    passed = True  # placeholder for actual threshold evaluation
    return "publish_success_notification" if passed else "publish_dq_failure_alert"


def publish_reconciliation_summary(**context):
    """Sends the daily reconciliation + fraud-alert summary to the
    compliance team's SNS topic, closing the audit-trail loop described
    in the README (discrepancy flagging for compliance review)."""
    sns = SnsHook(aws_conn_id="aws_default")
    execution_date = context["ds"]
    message = (
        f"Fraud pipeline run complete for {execution_date}.\n"
        f"Silver validation, gold aggregation, and reconciliation "
        f"discrepancy report are available in Athena/Redshift for "
        f"the compliance audit trail."
    )
    sns.publish(target_arn=SNS_TOPIC_ARN, message=message,
                subject=f"Fraud Pipeline Daily Run — {execution_date}")


def publish_dq_failure_alert(**context):
    sns = SnsHook(aws_conn_id="aws_default")
    sns.publish(
        target_arn=SNS_TOPIC_ARN,
        message="Data quality gate FAILED on gold_transactions. Pipeline halted.",
        subject="ALERT: Fraud Pipeline Data Quality Failure",
    )


with DAG(
    dag_id="banking_fraud_etl_pipeline",
    description="Daily fraud detection & transaction analytics ETL",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud-detection", "banking", "etl", "aml"],
) as dag:

    wait_for_core_banking_extract = S3KeySensor(
        task_id="wait_for_core_banking_extract",
        bucket_name=S3_BUCKET_RAW,
        bucket_key="core-banking-extract/{{ ds }}/_SUCCESS",
        timeout=60 * 60 * 2,
        poke_interval=300,
        mode="reschedule",
    )

    preflight_schema_check = PythonOperator(
        task_id="preflight_schema_validation",
        python_callable=validate_schema_preflight,
    )

    bronze_ingestion = GlueJobOperator(
        task_id="bronze_ingestion",
        job_name="bronze_ingestion",
        script_args={
            "--RAW_SOURCE_PATH": f"s3://{S3_BUCKET_RAW}/transactions/{{{{ ds }}}}/",
            "--BRONZE_TARGET_PATH": f"s3://{S3_BUCKET_BRONZE}/transactions/",
            "--KMS_KEY_ID": "alias/fraud-pipeline-tokenization-key",
            "--GLUE_DATABASE": "fraud_analytics",
        },
        aws_conn_id=GLUE_CONNECTION,
    )

    silver_validation = GlueJobOperator(
        task_id="silver_validation",
        job_name="silver_validation",
        script_args={
            "--BRONZE_PATH": f"s3://{S3_BUCKET_BRONZE}/transactions/",
            "--SILVER_DELTA_PATH": "s3://fraud-pipeline-silver/transactions_delta/",
            "--REFERENCE_MERCHANT_PATH": "s3://fraud-pipeline-reference/merchant_mcc/",
            "--REFERENCE_GEO_RISK_PATH": "s3://fraud-pipeline-reference/geo_risk/",
        },
        aws_conn_id=GLUE_CONNECTION,
    )

    incremental_reconciliation = GlueJobOperator(
        task_id="incremental_reconciliation",
        job_name="batch_transformation_reconcile",
        script_args={
            "--input": "s3://fraud-pipeline-silver/transactions_delta/",
            "--batch-extract": f"s3://{S3_BUCKET_RAW}/core-banking-extract/{{{{ ds }}}}/",
            "--output": "s3://fraud-pipeline-silver/reconciliation_discrepancies/",
            "--exec-mode": "reconcile",
        },
        aws_conn_id=GLUE_CONNECTION,
    )

    gold_aggregation = GlueJobOperator(
        task_id="gold_aggregation",
        job_name="gold_aggregation",
        script_args={
            "--SILVER_DELTA_PATH": "s3://fraud-pipeline-silver/transactions_delta/",
            "--GOLD_TRANSACTIONS_PATH": "s3://fraud-pipeline-gold/transactions/",
            "--GOLD_KPI_PATH": "s3://fraud-pipeline-gold/daily_kpis/",
            "--REDSHIFT_CONNECTION": "redshift-fraud-analytics",
            "--REDSHIFT_TEMP_DIR": "s3://fraud-pipeline-gold/redshift-staging/",
        },
        aws_conn_id=GLUE_CONNECTION,
    )

    refresh_athena_partitions = AthenaOperator(
        task_id="refresh_athena_partitions",
        query="MSCK REPAIR TABLE fraud_analytics.gold_transactions",
        database="fraud_analytics",
        output_location=f"s3://{S3_BUCKET_BRONZE}/athena-results/",
        aws_conn_id=GLUE_CONNECTION,
    )

    dq_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=data_quality_gate,
    )

    dq_branch = BranchPythonOperator(
        task_id="dq_branch",
        python_callable=branch_on_dq_result,
    )

    publish_success_notification = PythonOperator(
        task_id="publish_success_notification",
        python_callable=publish_reconciliation_summary,
    )

    publish_dq_failure_alert_task = PythonOperator(
        task_id="publish_dq_failure_alert",
        python_callable=publish_dq_failure_alert,
    )

    end = DummyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    (
        wait_for_core_banking_extract
        >> preflight_schema_check
        >> bronze_ingestion
        >> silver_validation
        >> incremental_reconciliation
        >> gold_aggregation
        >> refresh_athena_partitions
        >> dq_gate
        >> dq_branch
    )
    dq_branch >> publish_success_notification >> end
    dq_branch >> publish_dq_failure_alert_task >> end
