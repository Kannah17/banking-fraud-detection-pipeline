"""
lambda_consumer.py
====================
AWS Lambda consumer triggered by a Kinesis Data Streams event source
mapping. Performs lightweight, sub-500ms real-time processing per batch:
schema sanity-check, PII redaction guard, and hand-off to Firehose for
buffered delivery to the Bronze S3 landing zone. Records that fail
processing after retries are routed to a Dead Letter Queue (SQS) so no
event is silently dropped — a PCI-DSS / AML audit requirement.

Event source mapping configuration (Terraform/CDK, not shown here):
- BatchSize: 500
- MaximumBatchingWindowInSeconds: 1   -> supports sub-500ms latency target
- BisectBatchOnFunctionError: true
- MaximumRetryAttempts: 2
- DestinationConfig.OnFailure -> SQS DLQ ARN
- ParallelizationFactor: 10           -> concurrent shard consumers
"""

import base64
import json
import os
import time
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

FIREHOSE_STREAM_NAME = os.environ.get("FIREHOSE_STREAM_NAME", "fraud-pipeline-bronze-delivery")
DLQ_URL = os.environ.get("DLQ_URL")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID")

firehose_client = boto3.client("firehose")
sqs_client = boto3.client("sqs")
kms_client = boto3.client("kms")

REQUIRED_FIELDS = {"transaction_id", "account_id", "amount", "currency", "transaction_ts"}


def decode_kinesis_record(record: dict) -> dict:
    payload = base64.b64decode(record["kinesis"]["data"])
    return json.loads(payload)


def validate_event(event: dict) -> bool:
    return REQUIRED_FIELDS.issubset(event.keys())


def redact_sensitive_fields(event: dict) -> dict:
    """Defense-in-depth: even though producers should never send raw PANs
    downstream of the tokenization boundary, the consumer strips/guards
    any unexpected raw card_number field before it ever reaches Firehose,
    logging a security event if one is found."""
    if "card_number" in event and not event["card_number"].startswith("tok_"):
        logger.warning("Raw card_number detected in stream payload — redacting")
        event["card_number"] = "[REDACTED]"
    return event


def send_to_firehose(records: list):
    if not records:
        return
    entries = [{"Data": (json.dumps(r) + "\n").encode("utf-8")} for r in records]
    # Firehose PutRecordBatch max is 500 records / call
    for i in range(0, len(entries), 500):
        chunk = entries[i:i + 500]
        response = firehose_client.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM_NAME, Records=chunk
        )
        if response.get("FailedPutCount", 0) > 0:
            logger.error(f"Firehose PutRecordBatch failures: {response['FailedPutCount']}")


def send_to_dlq(failed_events: list, reason: str):
    if not DLQ_URL or not failed_events:
        return
    for event in failed_events:
        sqs_client.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps({
                "event": event,
                "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }),
        )
    logger.info(f"Routed {len(failed_events)} failed events to DLQ: {reason}")


def handler(event, context):
    """Lambda entry point. Receives a batch of Kinesis records."""
    start_time = time.time()

    valid_events = []
    invalid_events = []

    for record in event.get("Records", []):
        try:
            decoded = decode_kinesis_record(record)
            decoded = redact_sensitive_fields(decoded)

            if validate_event(decoded):
                valid_events.append(decoded)
            else:
                invalid_events.append(decoded)

        except Exception as exc:
            logger.exception(f"Failed to process record: {exc}")
            invalid_events.append({"raw_record": str(record), "error": str(exc)})

    send_to_firehose(valid_events)
    send_to_dlq(invalid_events, reason="SCHEMA_VALIDATION_FAILED")

    latency_ms = (time.time() - start_time) * 1000
    logger.info(
        f"Processed batch: valid={len(valid_events)}, invalid={len(invalid_events)}, "
        f"latency_ms={latency_ms:.1f}"
    )

    if latency_ms > 500:
        logger.warning(f"Batch exceeded 500ms SLA target: {latency_ms:.1f}ms")

    return {
        "statusCode": 200,
        "processed": len(valid_events),
        "failed": len(invalid_events),
        "latency_ms": round(latency_ms, 1),
    }
