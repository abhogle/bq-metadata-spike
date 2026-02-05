#!/usr/bin/env python3
"""
Stream-insert rows into BigQuery to populate the streaming_buffer metadata.

The streaming buffer shows data that has been inserted via the streaming API
but not yet flushed to permanent storage. This is visible in table metadata
for up to 90 minutes after insertion.

Usage:
    python3 sql/stream_insert.py

Prerequisites:
    pip install google-cloud-bigquery
"""

from google.cloud import bigquery
from datetime import datetime, timezone
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "bq-metadata-spike")


def stream_insert_shipping_events():
    """Stream insert new shipping events to populate streaming_buffer."""

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.raw_ecommerce.shipping_events"

    # Generate timestamps
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    rows = [
        {
            "event_id": "SE-STREAM-001",
            "order_id": "ORD-1005",
            "carrier": "FedEx",
            "tracking_number": "FX901234",
            "event_type": "delivered",
            "event_timestamp": now,
            "location": "Austin, TX",
            "source_system": "shipstation",
            "_ingested_at": now,
        },
        {
            "event_id": "SE-STREAM-002",
            "order_id": "ORD-1010",
            "carrier": "UPS",
            "tracking_number": "UP123456",
            "event_type": "picked_up",
            "event_timestamp": now,
            "location": "Warehouse, FL",
            "source_system": "shipstation",
            "_ingested_at": now,
        },
        {
            "event_id": "SE-STREAM-003",
            "order_id": "ORD-1010",
            "carrier": "UPS",
            "tracking_number": "UP123456",
            "event_type": "in_transit",
            "event_timestamp": now,
            "location": "Jacksonville, FL",
            "source_system": "shipstation",
            "_ingested_at": now,
        },
    ]

    print(f"Streaming {len(rows)} rows to {table_id}...")

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        print(f"Errors during streaming insert: {errors}")
        return False
    else:
        print("Streaming insert successful!")
        print(f"  Inserted {len(rows)} rows")
        print(f"  Table: {table_id}")
        print(f"  Timestamp: {now}")
        print("\nNote: Streaming buffer metadata will be visible for ~90 minutes.")
        print("Run the extraction script now to capture streaming_buffer stats.")
        return True


def check_streaming_buffer():
    """Check if the table has data in the streaming buffer."""

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.get_table(f"{PROJECT_ID}.raw_ecommerce.shipping_events")

    if table_ref.streaming_buffer:
        print("\nStreaming buffer stats:")
        print(f"  Estimated bytes: {table_ref.streaming_buffer.estimated_bytes}")
        print(f"  Estimated rows: {table_ref.streaming_buffer.estimated_rows}")
        print(f"  Oldest entry: {table_ref.streaming_buffer.oldest_entry_time}")
    else:
        print("\nNo streaming buffer present (data may have been flushed to storage).")


def stream_insert_payments():
    """Stream insert a payment to raw_ecommerce.payments."""

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.raw_ecommerce.payments"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    rows = [
        {
            "payment_id": "PAY-STREAM-001",
            "order_id": "ORD-1010",
            "payment_method": "credit_card",
            "payment_status": "success",
            "amount": "150.98",
            "currency": "USD",
            "processed_at": now,
            "gateway_reference": "ch_stream001",
            "card_last_four": "9999",
            "source_system": "stripe",
            "_ingested_at": now,
        }
    ]

    print(f"\nStreaming {len(rows)} rows to {table_id}...")

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        print(f"Errors: {errors}")
        return False
    else:
        print("Success!")
        return True


if __name__ == "__main__":
    print("BigQuery Streaming Insert Demo")
    print("=" * 60)
    print(f"Project: {PROJECT_ID}")
    print()

    # Stream to shipping_events
    stream_insert_shipping_events()

    # Stream to payments
    stream_insert_payments()

    # Check buffer status
    check_streaming_buffer()

    print("\n" + "=" * 60)
    print("Done! Run extraction script within 90 minutes to capture streaming_buffer.")
