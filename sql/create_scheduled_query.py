#!/usr/bin/env python3
"""
Create a scheduled query in BigQuery using the Data Transfer API.
This populates the scheduled_queries metadata for extraction.

Usage:
    python3 sql/create_scheduled_query.py

Prerequisites:
    pip install google-cloud-bigquery-datatransfer

Note: The Data Transfer API must be enabled in your project:
    gcloud services enable bigquerydatatransfer.googleapis.com
"""

from google.cloud import bigquery_datatransfer_v1
from google.protobuf import struct_pb2
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "bq-metadata-spike")
LOCATION = "us"  # Multi-region


def create_scheduled_query():
    """Create a scheduled query that refreshes customer segment counts nightly."""

    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"

    # Build the params struct
    params = struct_pb2.Struct()
    # Use a simple SELECT query that writes to a destination table
    params.fields["query"].string_value = """
        SELECT
            CURRENT_DATE() AS snapshot_date,
            customer_segment,
            COUNT(*) AS customer_count,
            ROUND(AVG(lifetime_revenue), 2) AS avg_lifetime_revenue,
            ROUND(AVG(total_orders), 1) AS avg_orders,
            SUM(lifetime_revenue) AS total_segment_revenue
        FROM `bq-metadata-spike.analytics_ecommerce.customer_360`
        GROUP BY 1, 2
        ORDER BY customer_count DESC
    """
    params.fields["destination_table_name_template"].string_value = "customer_segment_daily"
    params.fields["write_disposition"].string_value = "WRITE_TRUNCATE"

    transfer_config = bigquery_datatransfer_v1.TransferConfig(
        destination_dataset_id="reporting_ecommerce",
        display_name="Nightly Customer Segment Refresh",
        data_source_id="scheduled_query",
        params=params,
        schedule="every day 02:00",
        notification_pubsub_topic="",
        disabled=False,
    )

    try:
        response = client.create_transfer_config(
            parent=parent,
            transfer_config=transfer_config,
        )
        print(f"Created scheduled query: {response.name}")
        print(f"  Display name: {response.display_name}")
        print(f"  Schedule: {response.schedule}")
        print(f"  Dataset: {response.destination_dataset_id}")
        print(f"  State: {response.state.name}")
        return response

    except Exception as e:
        print(f"Error creating scheduled query: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure Data Transfer API is enabled:")
        print("   gcloud services enable bigquerydatatransfer.googleapis.com")
        print("2. Ensure you have the required permissions:")
        print("   - bigquery.transfers.update")
        print("   - bigquery.datasets.get")
        print("3. Check if the scheduled query already exists")
        raise


def list_scheduled_queries():
    """List all scheduled queries in the project."""

    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"

    print(f"\nExisting scheduled queries in {PROJECT_ID}:")
    print("-" * 60)

    try:
        for config in client.list_transfer_configs(parent=parent):
            if config.data_source_id == "scheduled_query":
                print(f"  - {config.display_name}")
                print(f"    Name: {config.name}")
                print(f"    Schedule: {config.schedule}")
                print(f"    State: {config.state.name}")
                print()
    except Exception as e:
        print(f"Error listing scheduled queries: {e}")


if __name__ == "__main__":
    print(f"Creating scheduled query in project: {PROJECT_ID}")
    print("=" * 60)

    # List existing first
    list_scheduled_queries()

    # Create new scheduled query
    print("\nCreating new scheduled query...")
    create_scheduled_query()

    print("\nDone! Run the extraction script to see the scheduled query in metadata.")
