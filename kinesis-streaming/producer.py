"""
producer.py
============
Kinesis Data Streams producer that emits simulated banking transaction
events at high throughput, used both for local load-testing of the
pipeline (validating the 100K+ events/minute, sub-500ms latency targets)
and as a reference implementation for the real core-banking event
publisher integration.

Design notes
------------
- Batches PutRecords calls (up to 500 records / 5MB per call, the Kinesis
  API limit) instead of single PutRecord calls, which is the single
  biggest lever for sustaining high throughput.
- Partition key = account_id, so all events for a given account land on
  the same shard in order — required for the velocity-based fraud checks
  in silver_validation.py to see a correctly ordered sequence per account.
- Includes exponential backoff + retry on throttled/failed records, since
  Kinesis PutRecords can partially fail within a single batch.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import List, Dict

import boto3

STREAM_NAME = "fraud-pipeline-transactions"
REGION = "us-east-1"
BATCH_SIZE = 500  # Kinesis PutRecords max records per call

MCC_CODES = ["5411", "5812", "5999", "7995", "4829", "5732", "6051", "5941"]
CHANNELS = ["POS", "ONLINE", "ATM", "MOBILE", "WIRE"]
COUNTRIES = ["US", "GB", "DE", "SG", "AE", "NG", "RU", "CN"]
CURRENCIES = ["USD", "EUR", "GBP", "SGD", "AED"]


def get_kinesis_client(region: str = REGION):
    return boto3.client("kinesis", region_name=region)


def generate_transaction_event(account_pool_size: int = 50_000) -> Dict:
    account_id = f"acct_{random.randint(1, account_pool_size):08d}"
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "card_number": f"4{random.randint(100000000000000, 999999999999999)}",
        "amount": round(random.uniform(1, 15000), 2),
        "currency": random.choice(CURRENCIES),
        "mcc_code": random.choice(MCC_CODES),
        "country_code": random.choice(COUNTRIES),
        "channel": random.choice(CHANNELS),
        "region": random.choice(["NA", "EU", "APAC", "MEA"]),
        "transaction_ts": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "device_id": f"dev_{random.randint(1, 200000)}",
    }


def build_kinesis_records(events: List[Dict]) -> List[Dict]:
    return [
        {
            "Data": json.dumps(event).encode("utf-8"),
            "PartitionKey": event["account_id"],
        }
        for event in events
    ]


def put_records_with_retry(client, stream_name: str, records: List[Dict],
                            max_retries: int = 5):
    attempt = 0
    to_send = records

    while to_send and attempt < max_retries:
        response = client.put_records(StreamName=stream_name, Records=to_send)

        if response["FailedRecordCount"] == 0:
            return

        failed_records = [
            to_send[i]
            for i, r in enumerate(response["Records"])
            if "ErrorCode" in r
        ]
        to_send = failed_records
        attempt += 1
        backoff = min(2 ** attempt * 0.1, 5)
        print(f"[producer] Retry {attempt}: {len(failed_records)} throttled/failed "
              f"records, backing off {backoff:.2f}s")
        time.sleep(backoff)

    if to_send:
        print(f"[producer] WARNING: {len(to_send)} records dropped after {max_retries} retries")


def run_load_test(target_events_per_minute: int = 100_000, duration_seconds: int = 60):
    """Sustains ~100K+ events/minute by pacing PutRecords batches, matching
    the throughput benchmark referenced in the project README."""
    client = get_kinesis_client()
    batches_per_minute = target_events_per_minute / BATCH_SIZE
    sleep_between_batches = 60.0 / batches_per_minute

    start = time.time()
    total_sent = 0

    while time.time() - start < duration_seconds:
        batch_start = time.time()

        events = [generate_transaction_event() for _ in range(BATCH_SIZE)]
        records = build_kinesis_records(events)
        put_records_with_retry(client, STREAM_NAME, records)

        total_sent += len(events)
        elapsed_batch = time.time() - batch_start
        sleep_time = max(0, sleep_between_batches - elapsed_batch)
        time.sleep(sleep_time)

    elapsed_total = time.time() - start
    throughput = total_sent / (elapsed_total / 60)
    print(f"[producer] Sent {total_sent} events in {elapsed_total:.1f}s "
          f"(~{throughput:,.0f} events/minute)")


if __name__ == "__main__":
    run_load_test(target_events_per_minute=100_000, duration_seconds=60)
