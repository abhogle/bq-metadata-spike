"""
BigQuery Metadata Extraction Script
====================================
Extracts ALL metadata that a metadata platform would consume:
1. Schema & table/column descriptions
2. Column-level lineage across views/tables
3. Data quality signals (nulls, distributions, stats)
4. Access/permissions metadata
5. Routines (procedures, functions)
6. Scheduled queries
7. Storage metadata
8. Constraints, policies, and more

Output: bigquery_metadata.json

Prerequisites:
  pip install google-cloud-bigquery google-cloud-bigquery-datatransfer
  export GOOGLE_APPLICATION_CREDENTIALS=~/bq-spike-sa-key.json
"""

import json
import os
from datetime import datetime
from google.cloud import bigquery

# Try to import datatransfer client (optional)
try:
    from google.cloud import bigquery_datatransfer_v1
    HAS_DATATRANSFER = True
except ImportError:
    HAS_DATATRANSFER = False
    print("  Info: bigquery_datatransfer not installed - scheduled queries will be skipped")

# -- Configuration ----------------------------------------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "bq-metadata-spike")
DATASETS = ["raw_ecommerce", "staging_ecommerce", "analytics_ecommerce", "reporting_ecommerce"]

client = bigquery.Client(project=PROJECT_ID)


def extract_dataset_metadata():
    """Extract dataset-level metadata: descriptions, labels, creation time, location, and more."""
    print("  Extracting dataset metadata...")
    datasets = []

    for dataset_id in DATASETS:
        try:
            ds = client.get_dataset(f"{PROJECT_ID}.{dataset_id}")
            datasets.append({
                "project_id": PROJECT_ID,
                "dataset_id": dataset_id,
                "description": ds.description,
                "friendly_name": ds.friendly_name,
                "location": ds.location,
                "created": ds.created.isoformat() if ds.created else None,
                "modified": ds.modified.isoformat() if ds.modified else None,
                "default_table_expiration_ms": ds.default_table_expiration_ms,
                "default_partition_expiration_ms": ds.default_partition_expiration_ms,
                "max_time_travel_hours": getattr(ds, 'max_time_travel_hours', None),
                "storage_billing_model": getattr(ds, 'storage_billing_model', None),
                "is_case_insensitive": getattr(ds, 'is_case_insensitive', None),
                "default_collation": getattr(ds, 'default_collation', None),
                "default_rounding_mode": getattr(ds, 'default_rounding_mode', None),
                "labels": dict(ds.labels) if ds.labels else {},
                "access_entries": [
                    {
                        "role": entry.role,
                        "entity_type": entry.entity_type,
                        "entity_id": entry.entity_id
                    }
                    for entry in ds.access_entries
                ] if ds.access_entries else []
            })
        except Exception as e:
            print(f"    Warning: Could not fetch dataset {dataset_id}: {e}")

    return datasets


def extract_table_metadata():
    """Extract table-level metadata: row counts, size, type, descriptions, labels, partitioning, and more."""
    print("  Extracting table metadata...")
    tables = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            creation_time,
            ddl
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        """

        try:
            results = client.query(query).result()
            for row in results:
                table_ref = f"{PROJECT_ID}.{dataset_id}.{row.table_name}"
                try:
                    table = client.get_table(table_ref)

                    # Build partitioning info
                    partitioning = None
                    if table.time_partitioning:
                        partitioning = {
                            "type": table.time_partitioning.type_,
                            "field": table.time_partitioning.field,
                            "expiration_ms": table.time_partitioning.expiration_ms,
                            "require_partition_filter": table.require_partition_filter
                        }
                    elif table.range_partitioning:
                        partitioning = {
                            "type": "RANGE",
                            "field": table.range_partitioning.field,
                            "range_start": table.range_partitioning.range_.start if table.range_partitioning.range_ else None,
                            "range_end": table.range_partitioning.range_.end if table.range_partitioning.range_ else None,
                            "range_interval": table.range_partitioning.range_.interval if table.range_partitioning.range_ else None
                        }

                    # Build streaming buffer info
                    streaming_buffer = None
                    if table.streaming_buffer:
                        streaming_buffer = {
                            "estimated_rows": table.streaming_buffer.estimated_rows,
                            "estimated_bytes": table.streaming_buffer.estimated_bytes,
                            "oldest_entry_time": table.streaming_buffer.oldest_entry_time.isoformat() if table.streaming_buffer.oldest_entry_time else None
                        }

                    # Build encryption info
                    encryption = None
                    if table.encryption_configuration:
                        encryption = {
                            "kms_key_name": table.encryption_configuration.kms_key_name
                        }

                    tables.append({
                        "project_id": PROJECT_ID,
                        "dataset_id": dataset_id,
                        "table_name": row.table_name,
                        "table_type": row.table_type,
                        "description": table.description,
                        "friendly_name": table.friendly_name,
                        "created": table.created.isoformat() if table.created else None,
                        "modified": table.modified.isoformat() if table.modified else None,
                        "expires": table.expires.isoformat() if table.expires else None,
                        "num_rows": table.num_rows,
                        "num_bytes": table.num_bytes,
                        "num_long_term_bytes": getattr(table, 'num_long_term_bytes', None),
                        "num_active_logical_bytes": getattr(table, 'num_active_logical_bytes', None),
                        "num_long_term_logical_bytes": getattr(table, 'num_long_term_logical_bytes', None),
                        "num_active_physical_bytes": getattr(table, 'num_active_physical_bytes', None),
                        "num_long_term_physical_bytes": getattr(table, 'num_long_term_physical_bytes', None),
                        "num_time_travel_physical_bytes": getattr(table, 'num_time_travel_physical_bytes', None),
                        "num_total_logical_bytes": getattr(table, 'num_total_logical_bytes', None),
                        "num_total_physical_bytes": getattr(table, 'num_total_physical_bytes', None),
                        "size_mb": round(table.num_bytes / (1024 * 1024), 4) if table.num_bytes else 0,
                        "labels": dict(table.labels) if table.labels else {},
                        "partitioning": partitioning,
                        "clustering_fields": list(table.clustering_fields) if table.clustering_fields else [],
                        "streaming_buffer": streaming_buffer,
                        "encryption_configuration": encryption,
                        "require_partition_filter": table.require_partition_filter,
                        "schema_fields": [
                            {
                                "name": field.name,
                                "field_type": field.field_type,
                                "mode": field.mode,
                                "description": field.description,
                                "max_length": field.max_length,
                                "precision": field.precision,
                                "scale": field.scale,
                                "default_value_expression": getattr(field, 'default_value_expression', None),
                                "fields": [
                                    {"name": f.name, "field_type": f.field_type, "mode": f.mode, "description": f.description}
                                    for f in field.fields
                                ] if field.fields else []
                            }
                            for field in table.schema
                        ]
                    })
                except Exception as e:
                    print(f"    Warning: Could not fetch table details for {table_ref}: {e}")
        except Exception as e:
            print(f"    Warning: Could not query tables in {dataset_id}: {e}")

    return tables


def extract_column_metadata():
    """Extract column-level metadata: types, descriptions, nullability, ordinal position, and more."""
    print("  Extracting column metadata...")
    columns = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            column_name,
            ordinal_position,
            is_nullable,
            data_type,
            is_partitioning_column,
            clustering_ordinal_position,
            is_hidden,
            is_system_defined,
            is_generated,
            generation_expression,
            column_default,
            collation_name,
            rounding_mode
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        ORDER BY table_name, ordinal_position
        """

        try:
            results = client.query(query).result()
            for row in results:
                columns.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "column_name": row.column_name,
                    "ordinal_position": row.ordinal_position,
                    "is_nullable": row.is_nullable,
                    "data_type": row.data_type,
                    "is_partitioning_column": row.is_partitioning_column,
                    "clustering_ordinal_position": row.clustering_ordinal_position,
                    "is_hidden": getattr(row, 'is_hidden', None),
                    "is_system_defined": getattr(row, 'is_system_defined', None),
                    "is_generated": getattr(row, 'is_generated', None),
                    "generation_expression": getattr(row, 'generation_expression', None),
                    "column_default": getattr(row, 'column_default', None),
                    "collation_name": getattr(row, 'collation_name', None),
                    "rounding_mode": getattr(row, 'rounding_mode', None)
                })
        except Exception as e:
            print(f"    Warning: Could not query columns in {dataset_id}: {e}")

    return columns


def extract_column_field_paths():
    """Extract column field paths for nested/repeated fields and policy tags."""
    print("  Extracting column field paths...")
    field_paths = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            column_name,
            field_path,
            data_type,
            description,
            collation_name
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
        ORDER BY table_name, field_path
        """

        try:
            results = client.query(query).result()
            for row in results:
                field_paths.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "column_name": row.column_name,
                    "field_path": row.field_path,
                    "data_type": row.data_type,
                    "description": row.description,
                    "collation_name": getattr(row, 'collation_name', None)
                })
        except Exception as e:
            print(f"    Warning: Could not query column field paths in {dataset_id}: {e}")

    return field_paths


def extract_view_definitions():
    """Extract view SQL definitions -- critical for lineage parsing."""
    print("  Extracting view definitions...")
    views = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_name,
            view_definition,
            check_option,
            use_standard_sql
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.VIEWS`
        """

        try:
            results = client.query(query).result()
            for row in results:
                views.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "view_name": row.table_name,
                    "view_definition": row.view_definition,
                    "check_option": getattr(row, 'check_option', None),
                    "use_standard_sql": getattr(row, 'use_standard_sql', None)
                })
        except Exception as e:
            # Not all datasets have views -- that's fine
            pass

    return views


def extract_routines():
    """Extract routines (procedures, functions) metadata."""
    print("  Extracting routines...")
    routines = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            routine_catalog,
            routine_schema,
            routine_name,
            routine_type,
            routine_body,
            routine_definition,
            data_type,
            external_language,
            is_deterministic,
            security_type,
            created,
            last_modified
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.ROUTINES`
        """

        try:
            results = client.query(query).result()
            for row in results:
                routines.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "routine_name": row.routine_name,
                    "routine_type": row.routine_type,
                    "routine_body": row.routine_body,
                    "routine_definition": row.routine_definition,
                    "return_type": row.data_type,
                    "external_language": row.external_language,
                    "is_deterministic": row.is_deterministic,
                    "security_type": row.security_type,
                    "created": row.created.isoformat() if row.created else None,
                    "last_modified": row.last_modified.isoformat() if row.last_modified else None
                })
        except Exception as e:
            # No routines in dataset is fine
            pass

    # Also get routine parameters
    routine_params = []
    for dataset_id in DATASETS:
        query = f"""
        SELECT
            specific_catalog,
            specific_schema,
            specific_name,
            ordinal_position,
            parameter_mode,
            parameter_name,
            data_type,
            is_result
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.PARAMETERS`
        """

        try:
            results = client.query(query).result()
            for row in results:
                routine_params.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "routine_name": row.specific_name,
                    "ordinal_position": row.ordinal_position,
                    "parameter_mode": row.parameter_mode,
                    "parameter_name": row.parameter_name,
                    "data_type": row.data_type,
                    "is_result": row.is_result
                })
        except:
            pass

    return {"routines": routines, "parameters": routine_params}


def extract_scheduled_queries():
    """Extract scheduled queries using BigQuery Data Transfer API."""
    print("  Extracting scheduled queries...")
    scheduled_queries = []

    if not HAS_DATATRANSFER:
        print("    Warning: bigquery_datatransfer not installed - skipping scheduled queries")
        return scheduled_queries

    try:
        transfer_client = bigquery_datatransfer_v1.DataTransferServiceClient()
        parent = f"projects/{PROJECT_ID}/locations/us"

        # List all transfer configs (scheduled queries are a type of transfer)
        request = bigquery_datatransfer_v1.ListTransferConfigsRequest(
            parent=parent,
            data_source_ids=["scheduled_query"]
        )

        for config in transfer_client.list_transfer_configs(request=request):
            scheduled_queries.append({
                "name": config.display_name,
                "config_id": config.name.split("/")[-1],
                "destination_dataset": config.destination_dataset_id,
                "schedule": config.schedule,
                "disabled": config.disabled,
                "state": config.state.name if config.state else None,
                "update_time": config.update_time.isoformat() if config.update_time else None,
                "next_run_time": config.next_run_time.isoformat() if config.next_run_time else None,
                "schedule_options_disable_auto_scheduling": config.schedule_options.disable_auto_scheduling if config.schedule_options else None,
                "params": dict(config.params) if config.params else {}
            })
    except Exception as e:
        print(f"    Warning: Could not fetch scheduled queries: {e}")
        print("    (This requires the BigQuery Data Transfer API to be enabled)")

    return scheduled_queries


def extract_storage_metadata():
    """Extract storage metadata from INFORMATION_SCHEMA.TABLE_STORAGE.

    Tries multiple approaches:
    1. Project-level query with region-US (for US multi-region datasets)
    2. Dataset-level INFORMATION_SCHEMA queries
    3. Falls back to Python API for table storage info
    """
    print("  Extracting storage metadata...")
    storage_data = []

    # Approach 1: Try project-level TABLE_STORAGE with region-US
    query = f"""
    SELECT
        project_id,
        project_number,
        table_catalog,
        table_schema,
        table_name,
        creation_time,
        total_rows,
        total_partitions,
        total_logical_bytes,
        active_logical_bytes,
        long_term_logical_bytes,
        total_physical_bytes,
        active_physical_bytes,
        long_term_physical_bytes,
        time_travel_physical_bytes,
        storage_last_modified_time,
        deleted,
        table_type
    FROM `{PROJECT_ID}.region-US.INFORMATION_SCHEMA.TABLE_STORAGE`
    WHERE table_schema IN ('raw_ecommerce', 'staging_ecommerce', 'analytics_ecommerce', 'reporting_ecommerce')
    """

    try:
        results = client.query(query).result()
        for row in results:
            storage_data.append({
                "project_id": row.project_id,
                "dataset_id": row.table_schema,
                "table_name": row.table_name,
                "table_type": row.table_type,
                "creation_time": row.creation_time.isoformat() if row.creation_time else None,
                "total_rows": row.total_rows,
                "total_partitions": row.total_partitions,
                "total_logical_bytes": row.total_logical_bytes,
                "active_logical_bytes": row.active_logical_bytes,
                "long_term_logical_bytes": row.long_term_logical_bytes,
                "total_physical_bytes": row.total_physical_bytes,
                "active_physical_bytes": row.active_physical_bytes,
                "long_term_physical_bytes": row.long_term_physical_bytes,
                "time_travel_physical_bytes": row.time_travel_physical_bytes,
                "storage_last_modified_time": row.storage_last_modified_time.isoformat() if row.storage_last_modified_time else None,
                "deleted": row.deleted
            })
        print(f"    Found {len(storage_data)} table storage records via region-US")
        return storage_data
    except Exception as e:
        print(f"    Project-level TABLE_STORAGE not available: {e}")

    # Approach 2: Try per-dataset INFORMATION_SCHEMA.TABLE_STORAGE
    for dataset_id in DATASETS:
        query = f"""
        SELECT
            project_id,
            project_number,
            table_catalog,
            table_schema,
            table_name,
            creation_time,
            total_rows,
            total_partitions,
            total_logical_bytes,
            active_logical_bytes,
            long_term_logical_bytes,
            total_physical_bytes,
            active_physical_bytes,
            long_term_physical_bytes,
            time_travel_physical_bytes,
            storage_last_modified_time,
            deleted,
            table_type
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.TABLE_STORAGE`
        """

        try:
            results = client.query(query).result()
            for row in results:
                storage_data.append({
                    "project_id": row.project_id,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "table_type": row.table_type,
                    "creation_time": row.creation_time.isoformat() if row.creation_time else None,
                    "total_rows": row.total_rows,
                    "total_partitions": row.total_partitions,
                    "total_logical_bytes": row.total_logical_bytes,
                    "active_logical_bytes": row.active_logical_bytes,
                    "long_term_logical_bytes": row.long_term_logical_bytes,
                    "total_physical_bytes": row.total_physical_bytes,
                    "active_physical_bytes": row.active_physical_bytes,
                    "long_term_physical_bytes": row.long_term_physical_bytes,
                    "time_travel_physical_bytes": row.time_travel_physical_bytes,
                    "storage_last_modified_time": row.storage_last_modified_time.isoformat() if row.storage_last_modified_time else None,
                    "deleted": row.deleted
                })
        except Exception as e:
            print(f"    Warning: TABLE_STORAGE not available in {dataset_id}: {e}")

    if storage_data:
        print(f"    Found {len(storage_data)} table storage records via dataset-level queries")
    else:
        print("    TABLE_STORAGE not available, storage breakdown will be limited to API data")

    return storage_data


def extract_table_constraints():
    """Extract table constraints (primary keys, foreign keys)."""
    print("  Extracting table constraints...")
    constraints = []
    key_usage = []

    for dataset_id in DATASETS:
        # Get constraints
        query = f"""
        SELECT
            constraint_catalog,
            constraint_schema,
            constraint_name,
            table_catalog,
            table_schema,
            table_name,
            constraint_type,
            is_deferrable,
            initially_deferred,
            enforced
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS`
        """

        try:
            results = client.query(query).result()
            for row in results:
                constraints.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "constraint_name": row.constraint_name,
                    "constraint_type": row.constraint_type,
                    "is_deferrable": row.is_deferrable,
                    "initially_deferred": row.initially_deferred,
                    "enforced": row.enforced
                })
        except Exception as e:
            # Table constraints may not exist
            pass

        # Get key column usage
        query = f"""
        SELECT
            constraint_catalog,
            constraint_schema,
            constraint_name,
            table_catalog,
            table_schema,
            table_name,
            column_name,
            ordinal_position,
            position_in_unique_constraint
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
        """

        try:
            results = client.query(query).result()
            for row in results:
                key_usage.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "constraint_name": row.constraint_name,
                    "column_name": row.column_name,
                    "ordinal_position": row.ordinal_position,
                    "position_in_unique_constraint": row.position_in_unique_constraint
                })
        except:
            pass

    return {"constraints": constraints, "key_column_usage": key_usage}


def extract_table_options():
    """Extract table options (expiration, description, labels, etc.)."""
    print("  Extracting table options...")
    options = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            option_name,
            option_type,
            option_value
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.TABLE_OPTIONS`
        """

        try:
            results = client.query(query).result()
            for row in results:
                options.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "option_name": row.option_name,
                    "option_type": row.option_type,
                    "option_value": row.option_value
                })
        except Exception as e:
            print(f"    Warning: Could not query table options in {dataset_id}: {e}")

    return options


def extract_row_level_security():
    """Extract row-level security policies from INFORMATION_SCHEMA.ROW_ACCESS_POLICIES."""
    print("  Extracting row-level security policies...")
    policies = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            row_access_policy_name,
            filter_predicate,
            creation_time,
            last_modified_time
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES`
        """

        try:
            results = client.query(query).result()
            for row in results:
                policies.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "policy_name": row.row_access_policy_name,
                    "filter_predicate": row.filter_predicate,
                    "created": row.creation_time.isoformat() if row.creation_time else None,
                    "last_modified": row.last_modified_time.isoformat() if row.last_modified_time else None
                })
        except Exception as e:
            # No RLS policies is fine
            pass

    return policies


def extract_snapshots_and_clones():
    """Extract table snapshots metadata."""
    print("  Extracting snapshots and clones...")
    snapshots = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            base_table_catalog,
            base_table_schema,
            base_table_name,
            snapshot_time
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.TABLE_SNAPSHOTS`
        """

        try:
            results = client.query(query).result()
            for row in results:
                snapshots.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "snapshot_table_name": row.table_name,
                    "base_table_project": row.base_table_catalog,
                    "base_table_dataset": row.base_table_schema,
                    "base_table_name": row.base_table_name,
                    "snapshot_time": row.snapshot_time.isoformat() if row.snapshot_time else None
                })
        except Exception as e:
            # No snapshots is fine
            pass

    return snapshots


def extract_partitions_metadata():
    """Extract partition-level metadata from INFORMATION_SCHEMA.PARTITIONS."""
    print("  Extracting partitions metadata...")
    partitions = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            partition_id,
            total_rows,
            total_logical_bytes,
            total_billable_bytes,
            last_modified_time,
            storage_tier
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE partition_id IS NOT NULL
        """

        try:
            results = client.query(query).result()
            for row in results:
                partitions.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "partition_id": row.partition_id,
                    "total_rows": row.total_rows,
                    "total_logical_bytes": row.total_logical_bytes,
                    "total_billable_bytes": row.total_billable_bytes,
                    "last_modified_time": row.last_modified_time.isoformat() if row.last_modified_time else None,
                    "storage_tier": row.storage_tier
                })
        except Exception as e:
            # No partitions is fine
            pass

    return partitions


def extract_streaming_buffer_stats():
    """Extract streaming buffer statistics for all tables."""
    print("  Extracting streaming buffer stats...")
    buffers = []

    for dataset_id in DATASETS:
        try:
            tables = client.list_tables(f"{PROJECT_ID}.{dataset_id}")
            for table_item in tables:
                try:
                    table = client.get_table(f"{PROJECT_ID}.{dataset_id}.{table_item.table_id}")
                    if table.streaming_buffer:
                        buffers.append({
                            "project_id": PROJECT_ID,
                            "dataset_id": dataset_id,
                            "table_name": table_item.table_id,
                            "estimated_rows": table.streaming_buffer.estimated_rows,
                            "estimated_bytes": table.streaming_buffer.estimated_bytes,
                            "oldest_entry_time": table.streaming_buffer.oldest_entry_time.isoformat() if table.streaming_buffer.oldest_entry_time else None
                        })
                except:
                    pass
        except Exception as e:
            print(f"    Warning: Could not list tables in {dataset_id}: {e}")

    return buffers


def extract_column_level_lineage():
    """
    Extract column-level lineage by parsing view definitions and CTAS query patterns.

    In production, metadata platforms do this by:
    1. Parsing SQL from INFORMATION_SCHEMA.VIEWS
    2. Parsing SQL from INFORMATION_SCHEMA.JOBS (query history)
    3. Using BigQuery's native lineage API (if available)

    For this spike, we derive lineage from our known data architecture.
    """
    print("  Building column-level lineage...")

    lineage = {
        "table_lineage": [],
        "column_lineage": []
    }

    # Table-level lineage (which tables feed which)
    table_lineage_map = [
        # Raw -> Staging
        {"upstream": "raw_ecommerce.customers", "downstream": "staging_ecommerce.stg_customers", "transform": "CTAS with dedup + type casting"},
        {"upstream": "raw_ecommerce.products", "downstream": "staging_ecommerce.stg_products", "transform": "CTAS with type casting"},
        {"upstream": "raw_ecommerce.orders", "downstream": "staging_ecommerce.stg_orders", "transform": "CTAS with type casting"},
        {"upstream": "raw_ecommerce.order_items", "downstream": "staging_ecommerce.stg_order_items", "transform": "CTAS with type casting"},
        {"upstream": "raw_ecommerce.payments", "downstream": "staging_ecommerce.stg_payments", "transform": "CTAS with type casting"},
        {"upstream": "raw_ecommerce.shipping_events", "downstream": "staging_ecommerce.stg_shipping_events", "transform": "CTAS with type casting"},

        # Staging -> Analytics
        {"upstream": "staging_ecommerce.stg_customers", "downstream": "analytics_ecommerce.customer_360", "transform": "JOIN + aggregation"},
        {"upstream": "staging_ecommerce.stg_orders", "downstream": "analytics_ecommerce.customer_360", "transform": "JOIN + aggregation"},
        {"upstream": "staging_ecommerce.stg_products", "downstream": "analytics_ecommerce.product_performance", "transform": "JOIN + aggregation"},
        {"upstream": "staging_ecommerce.stg_order_items", "downstream": "analytics_ecommerce.product_performance", "transform": "JOIN + aggregation"},
        {"upstream": "staging_ecommerce.stg_orders", "downstream": "analytics_ecommerce.product_performance", "transform": "JOIN for customer_id"},
        {"upstream": "staging_ecommerce.stg_orders", "downstream": "analytics_ecommerce.revenue_daily", "transform": "Aggregation by date"},
        {"upstream": "staging_ecommerce.stg_orders", "downstream": "analytics_ecommerce.order_fulfillment", "transform": "JOIN + pivot"},
        {"upstream": "staging_ecommerce.stg_payments", "downstream": "analytics_ecommerce.order_fulfillment", "transform": "JOIN on order_id"},
        {"upstream": "staging_ecommerce.stg_shipping_events", "downstream": "analytics_ecommerce.order_fulfillment", "transform": "JOIN + pivot"},

        # Analytics -> Reporting
        {"upstream": "analytics_ecommerce.revenue_daily", "downstream": "reporting_ecommerce.v_executive_kpis", "transform": "VIEW aggregation"},
        {"upstream": "analytics_ecommerce.customer_360", "downstream": "reporting_ecommerce.v_executive_kpis", "transform": "VIEW aggregation"},
        {"upstream": "analytics_ecommerce.customer_360", "downstream": "reporting_ecommerce.v_customer_segments", "transform": "VIEW grouping"},
        {"upstream": "analytics_ecommerce.product_performance", "downstream": "reporting_ecommerce.v_category_performance", "transform": "VIEW grouping"},
        {"upstream": "analytics_ecommerce.order_fulfillment", "downstream": "reporting_ecommerce.v_fulfillment_sla", "transform": "VIEW grouping"},
    ]
    lineage["table_lineage"] = table_lineage_map

    # Column-level lineage (selected key columns to demonstrate)
    column_lineage_entries = [
        # customer_id flows from raw -> staging -> analytics -> reporting
        {"upstream_col": "raw_ecommerce.customers.customer_id", "downstream_col": "staging_ecommerce.stg_customers.customer_id", "transform": "pass-through"},
        {"upstream_col": "staging_ecommerce.stg_customers.customer_id", "downstream_col": "analytics_ecommerce.customer_360.customer_id", "transform": "pass-through"},

        # email flows through and is PII-tagged
        {"upstream_col": "raw_ecommerce.customers.email", "downstream_col": "staging_ecommerce.stg_customers.email", "transform": "LOWER()"},
        {"upstream_col": "staging_ecommerce.stg_customers.email", "downstream_col": "analytics_ecommerce.customer_360.email", "transform": "pass-through"},

        # name transformation
        {"upstream_col": "raw_ecommerce.customers.first_name", "downstream_col": "staging_ecommerce.stg_customers.first_name", "transform": "INITCAP()"},
        {"upstream_col": "staging_ecommerce.stg_customers.first_name", "downstream_col": "analytics_ecommerce.customer_360.first_name", "transform": "pass-through"},

        # date_of_birth type casting
        {"upstream_col": "raw_ecommerce.customers.date_of_birth", "downstream_col": "staging_ecommerce.stg_customers.date_of_birth", "transform": "SAFE.PARSE_DATE()"},

        # order_date flows through to revenue_daily
        {"upstream_col": "raw_ecommerce.orders.order_date", "downstream_col": "staging_ecommerce.stg_orders.order_date", "transform": "SAFE.PARSE_DATE()"},
        {"upstream_col": "staging_ecommerce.stg_orders.order_date", "downstream_col": "analytics_ecommerce.revenue_daily.order_date", "transform": "pass-through"},

        # total_amount flows to revenue metrics
        {"upstream_col": "raw_ecommerce.orders.total_amount", "downstream_col": "staging_ecommerce.stg_orders.total_amount", "transform": "SAFE_CAST to NUMERIC"},
        {"upstream_col": "staging_ecommerce.stg_orders.total_amount", "downstream_col": "analytics_ecommerce.revenue_daily.gross_revenue", "transform": "SUM()"},
        {"upstream_col": "staging_ecommerce.stg_orders.total_amount", "downstream_col": "analytics_ecommerce.customer_360.lifetime_revenue", "transform": "SUM()"},
        {"upstream_col": "staging_ecommerce.stg_orders.total_amount", "downstream_col": "analytics_ecommerce.customer_360.avg_order_value", "transform": "AVG()"},

        # Derived: customer_segment is computed
        {"upstream_col": "staging_ecommerce.stg_orders.order_id", "downstream_col": "analytics_ecommerce.customer_360.customer_segment", "transform": "COUNT DISTINCT -> CASE (derived)"},

        # product price -> margin calculations
        {"upstream_col": "raw_ecommerce.products.unit_price", "downstream_col": "staging_ecommerce.stg_products.unit_price", "transform": "SAFE_CAST to NUMERIC"},
        {"upstream_col": "staging_ecommerce.stg_products.unit_price", "downstream_col": "analytics_ecommerce.product_performance.current_price", "transform": "pass-through"},
        {"upstream_col": "staging_ecommerce.stg_products.cost_price", "downstream_col": "analytics_ecommerce.product_performance.gross_margin", "transform": "SUM(subtotal) - SUM(qty * cost_price)"},
        {"upstream_col": "staging_ecommerce.stg_products.cost_price", "downstream_col": "analytics_ecommerce.product_performance.margin_pct", "transform": "SAFE_DIVIDE(margin, revenue)"},

        # payment card_last_four is PII
        {"upstream_col": "raw_ecommerce.payments.card_last_four", "downstream_col": "staging_ecommerce.stg_payments.card_last_four", "transform": "pass-through (PII)"},

        # shipping -> fulfillment
        {"upstream_col": "staging_ecommerce.stg_shipping_events.event_type", "downstream_col": "analytics_ecommerce.order_fulfillment.delivered_at", "transform": "CASE WHEN pivot"},
        {"upstream_col": "staging_ecommerce.stg_shipping_events.event_timestamp", "downstream_col": "analytics_ecommerce.order_fulfillment.delivery_hours", "transform": "TIMESTAMP_DIFF()"},

        # Reporting views
        {"upstream_col": "analytics_ecommerce.revenue_daily.net_revenue", "downstream_col": "reporting_ecommerce.v_executive_kpis.kpi_value", "transform": "SUM -> CAST to STRING"},
        {"upstream_col": "analytics_ecommerce.customer_360.customer_segment", "downstream_col": "reporting_ecommerce.v_customer_segments.customer_segment", "transform": "GROUP BY"},
        {"upstream_col": "analytics_ecommerce.customer_360.lifetime_revenue", "downstream_col": "reporting_ecommerce.v_customer_segments.avg_lifetime_revenue", "transform": "AVG -> ROUND"},
        {"upstream_col": "analytics_ecommerce.product_performance.total_revenue", "downstream_col": "reporting_ecommerce.v_category_performance.revenue", "transform": "SUM by category"},
        {"upstream_col": "analytics_ecommerce.order_fulfillment.delivery_hours", "downstream_col": "reporting_ecommerce.v_fulfillment_sla.avg_delivery_hours", "transform": "AVG -> ROUND"},
    ]
    lineage["column_lineage"] = column_lineage_entries

    return lineage


def extract_data_quality_signals(tables_metadata):
    """
    Generate data quality signals: null rates, distinct counts, basic statistics.

    In production, this would use:
    - INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    - Profiling queries (SELECT COUNT(*), COUNT(DISTINCT col), etc.)
    - BigQuery's auto-generated table profiles

    For this spike, we run profiling queries on each table.
    """
    print("  Extracting data quality signals...")
    quality_signals = []

    for table_meta in tables_metadata:
        dataset_id = table_meta["dataset_id"]
        table_name = table_meta["table_name"]
        table_type = table_meta["table_type"]

        # Skip views for profiling (we'd need to query through them)
        if table_type == "VIEW":
            quality_signals.append({
                "dataset_id": dataset_id,
                "table_name": table_name,
                "table_type": "VIEW",
                "row_count": table_meta.get("num_rows"),
                "columns": [],
                "note": "View -- profile by querying through view"
            })
            continue

        num_rows = table_meta.get("num_rows", 0)
        if num_rows == 0:
            continue

        # Build profiling query for each column
        columns = table_meta.get("schema_fields", [])
        col_profiles = []

        for col in columns:
            col_name = col["name"]
            col_type = col["field_type"]

            try:
                # Basic profiling query
                profile_parts = [
                    f"COUNT(*) AS total_rows",
                    f"COUNTIF(`{col_name}` IS NULL) AS null_count",
                    f"COUNT(DISTINCT `{col_name}`) AS distinct_count",
                ]

                # Add type-specific stats
                if col_type in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
                    profile_parts.extend([
                        f"MIN(`{col_name}`) AS min_val",
                        f"MAX(`{col_name}`) AS max_val",
                        f"AVG(CAST(`{col_name}` AS FLOAT64)) AS avg_val",
                        f"STDDEV(CAST(`{col_name}` AS FLOAT64)) AS stddev_val",
                    ])
                elif col_type in ("STRING",):
                    profile_parts.extend([
                        f"MIN(LENGTH(`{col_name}`)) AS min_length",
                        f"MAX(LENGTH(`{col_name}`)) AS max_length",
                        f"AVG(LENGTH(`{col_name}`)) AS avg_length",
                    ])
                elif col_type in ("TIMESTAMP", "DATE", "DATETIME"):
                    profile_parts.extend([
                        f"MIN(`{col_name}`) AS min_val",
                        f"MAX(`{col_name}`) AS max_val",
                    ])

                profile_query = f"""
                SELECT {', '.join(profile_parts)}
                FROM `{PROJECT_ID}.{dataset_id}.{table_name}`
                """

                result = list(client.query(profile_query).result())[0]

                profile = {
                    "column_name": col_name,
                    "data_type": col_type,
                    "total_rows": result.total_rows,
                    "null_count": result.null_count,
                    "null_pct": round(result.null_count / result.total_rows * 100, 2) if result.total_rows > 0 else 0,
                    "distinct_count": result.distinct_count,
                    "uniqueness_pct": round(result.distinct_count / result.total_rows * 100, 2) if result.total_rows > 0 else 0,
                }

                # Add type-specific results
                if col_type in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
                    profile["min"] = float(result.min_val) if result.min_val is not None else None
                    profile["max"] = float(result.max_val) if result.max_val is not None else None
                    profile["mean"] = round(float(result.avg_val), 4) if result.avg_val is not None else None
                    profile["stddev"] = round(float(result.stddev_val), 4) if result.stddev_val is not None else None
                elif col_type in ("STRING",):
                    profile["min_length"] = result.min_length
                    profile["max_length"] = result.max_length
                    profile["avg_length"] = round(float(result.avg_length), 1) if result.avg_length is not None else None
                elif col_type in ("TIMESTAMP", "DATE", "DATETIME"):
                    profile["min"] = str(result.min_val) if result.min_val else None
                    profile["max"] = str(result.max_val) if result.max_val else None

                col_profiles.append(profile)

            except Exception as e:
                col_profiles.append({
                    "column_name": col_name,
                    "data_type": col_type,
                    "error": str(e)
                })

        quality_signals.append({
            "dataset_id": dataset_id,
            "table_name": table_name,
            "table_type": table_type,
            "row_count": num_rows,
            "columns": col_profiles
        })

    return quality_signals


def extract_access_metadata():
    """
    Extract access/permissions metadata from BigQuery.

    Sources:
    - Dataset ACLs (via client.get_dataset)
    - Table ACLs (via IAM)
    - Authorized views
    - Row-level security policies
    """
    print("  Extracting access/permissions metadata...")
    access_metadata = {
        "dataset_permissions": [],
        "iam_policies": [],
        "authorized_views": []
    }

    for dataset_id in DATASETS:
        try:
            ds = client.get_dataset(f"{PROJECT_ID}.{dataset_id}")

            entries = []
            for entry in ds.access_entries:
                entries.append({
                    "role": entry.role,
                    "entity_type": entry.entity_type,
                    "entity_id": entry.entity_id,
                    "dataset_id": dataset_id
                })

            access_metadata["dataset_permissions"].append({
                "dataset_id": dataset_id,
                "access_entries": entries
            })
        except Exception as e:
            print(f"    Warning: Could not fetch access for {dataset_id}: {e}")

    # Check for authorized views (views granted cross-dataset access)
    for dataset_id in DATASETS:
        try:
            ds = client.get_dataset(f"{PROJECT_ID}.{dataset_id}")
            for entry in ds.access_entries:
                if entry.entity_type == "view":
                    access_metadata["authorized_views"].append({
                        "dataset_id": dataset_id,
                        "authorized_view": str(entry.entity_id)
                    })
        except:
            pass

    return access_metadata


def extract_job_history():
    """
    Extract recent query/job history -- this is how metadata platforms build usage-based lineage.

    Uses INFORMATION_SCHEMA.JOBS (requires sufficient permissions).
    """
    print("  Extracting job/query history...")
    jobs = []

    try:
        query = f"""
        SELECT
            job_id,
            user_email,
            job_type,
            state,
            creation_time,
            end_time,
            total_bytes_processed,
            total_bytes_billed,
            query,
            referenced_tables,
            destination_table,
            cache_hit,
            statement_type,
            priority,
            total_slot_ms,
            error_result
        FROM `{PROJECT_ID}.region-us.INFORMATION_SCHEMA.JOBS`
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
          AND job_type = 'QUERY'
        ORDER BY creation_time DESC
        LIMIT 50
        """

        results = client.query(query).result()
        for row in results:
            ref_tables = []
            if row.referenced_tables:
                for rt in row.referenced_tables:
                    ref_tables.append({
                        "project_id": rt.get("project_id", ""),
                        "dataset_id": rt.get("dataset_id", ""),
                        "table_id": rt.get("table_id", "")
                    })

            dest = None
            if row.destination_table:
                dest = {
                    "project_id": row.destination_table.get("project_id", ""),
                    "dataset_id": row.destination_table.get("dataset_id", ""),
                    "table_id": row.destination_table.get("table_id", "")
                }

            jobs.append({
                "job_id": row.job_id,
                "user_email": row.user_email,
                "job_type": row.job_type,
                "state": row.state,
                "creation_time": row.creation_time.isoformat() if row.creation_time else None,
                "end_time": row.end_time.isoformat() if row.end_time else None,
                "bytes_processed": row.total_bytes_processed,
                "bytes_billed": row.total_bytes_billed,
                "cache_hit": row.cache_hit,
                "statement_type": row.statement_type,
                "priority": row.priority,
                "total_slot_ms": row.total_slot_ms,
                "error_result": str(row.error_result) if row.error_result else None,
                "query_preview": (row.query or "")[:500],
                "referenced_tables": ref_tables,
                "destination_table": dest
            })
    except Exception as e:
        print(f"    Warning: Could not fetch job history: {e}")
        print("    (This requires bigquery.jobs.listAll permission -- may not work with basic roles)")

    return jobs


def extract_pii_classification():
    """
    Identify columns likely containing PII -- metadata platforms do this via auto-classification.
    We look for column names + labels that suggest PII.
    """
    print("  Classifying PII columns...")

    pii_keywords = {
        "email": "Email Address",
        "phone": "Phone Number",
        "date_of_birth": "Date of Birth",
        "address": "Physical Address",
        "postal_code": "Postal Code",
        "card_last_four": "Partial Card Number",
        "first_name": "Person Name",
        "last_name": "Person Name",
        "ssn": "Social Security Number",
        "ip_address": "IP Address",
    }

    pii_columns = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT table_name, column_name
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        """
        try:
            results = client.query(query).result()
            for row in results:
                col_lower = row.column_name.lower()
                for keyword, classification in pii_keywords.items():
                    if keyword in col_lower:
                        pii_columns.append({
                            "dataset_id": dataset_id,
                            "table_name": row.table_name,
                            "column_name": row.column_name,
                            "pii_classification": classification,
                            "confidence": "high" if col_lower == keyword else "medium"
                        })
                        break
        except:
            pass

    return pii_columns


def extract_search_indexes():
    """Extract search indexes from INFORMATION_SCHEMA.SEARCH_INDEXES."""
    print("  Extracting search indexes...")
    indexes = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            index_catalog,
            index_schema,
            table_name,
            index_name,
            index_creation_time,
            index_status,
            coverage_percentage,
            analyzer,
            index_columns
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.SEARCH_INDEXES`
        """

        try:
            results = client.query(query).result()
            for row in results:
                indexes.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "index_name": row.index_name,
                    "index_creation_time": row.index_creation_time.isoformat() if row.index_creation_time else None,
                    "index_status": row.index_status,
                    "coverage_percentage": row.coverage_percentage,
                    "analyzer": row.analyzer,
                    "index_columns": row.index_columns
                })
        except Exception as e:
            # No search indexes is fine
            pass

    return indexes


def extract_vector_indexes():
    """Extract vector indexes from INFORMATION_SCHEMA.VECTOR_INDEXES."""
    print("  Extracting vector indexes...")
    indexes = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            index_catalog,
            index_schema,
            table_name,
            index_name,
            index_creation_time,
            index_status,
            coverage_percentage,
            index_columns,
            distance_type,
            tree_type,
            num_leaves
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.VECTOR_INDEXES`
        """

        try:
            results = client.query(query).result()
            for row in results:
                indexes.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "table_name": row.table_name,
                    "index_name": row.index_name,
                    "index_creation_time": row.index_creation_time.isoformat() if row.index_creation_time else None,
                    "index_status": row.index_status,
                    "coverage_percentage": row.coverage_percentage,
                    "index_columns": row.index_columns,
                    "distance_type": row.distance_type,
                    "tree_type": row.tree_type,
                    "num_leaves": row.num_leaves
                })
        except:
            # No vector indexes is fine
            pass

    return indexes


def extract_materialized_views():
    """Extract materialized view metadata from INFORMATION_SCHEMA.MATERIALIZED_VIEWS."""
    print("  Extracting materialized views...")
    mv_list = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            last_refresh_time,
            refresh_watermark
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.MATERIALIZED_VIEWS`
        """

        try:
            results = client.query(query).result()
            for row in results:
                mv_list.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "view_name": row.table_name,
                    "last_refresh_time": row.last_refresh_time.isoformat() if row.last_refresh_time else None,
                    "refresh_watermark": row.refresh_watermark.isoformat() if row.refresh_watermark else None
                })
        except:
            # No materialized views is fine
            pass

    return mv_list


def extract_bi_engine_statistics():
    """Extract BI Engine statistics from INFORMATION_SCHEMA.BI_CAPACITIES."""
    print("  Extracting BI Engine statistics...")
    bi_stats = []

    try:
        query = f"""
        SELECT
            project_id,
            project_number,
            size,
            preferred_tables
        FROM `{PROJECT_ID}.region-us.INFORMATION_SCHEMA.BI_CAPACITIES`
        """

        results = client.query(query).result()
        for row in results:
            bi_stats.append({
                "project_id": row.project_id,
                "project_number": row.project_number,
                "size_bytes": row.size,
                "preferred_tables": list(row.preferred_tables) if row.preferred_tables else []
            })
    except Exception as e:
        # BI Engine may not be enabled
        pass

    return bi_stats


# -- Main Execution ---------------------------------------------------

def main():
    print("=" * 60)
    print(" BigQuery Metadata Extraction -- Starting")
    print(f"   Project: {PROJECT_ID}")
    print(f"   Datasets: {', '.join(DATASETS)}")
    print("=" * 60)

    metadata = {
        "extraction_metadata": {
            "project_id": PROJECT_ID,
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "datasets_scanned": DATASETS,
            "extractor_version": "2.0.0-comprehensive"
        }
    }

    # 1. Dataset metadata
    metadata["datasets"] = extract_dataset_metadata()
    print(f"   [ok] {len(metadata['datasets'])} datasets extracted")

    # 2. Table metadata (includes column schemas, partitioning, clustering, streaming buffer, encryption)
    metadata["tables"] = extract_table_metadata()
    print(f"   [ok] {len(metadata['tables'])} tables/views extracted")

    # 3. Detailed column metadata
    metadata["columns"] = extract_column_metadata()
    print(f"   [ok] {len(metadata['columns'])} columns extracted")

    # 4. Column field paths (for nested/repeated fields)
    metadata["column_field_paths"] = extract_column_field_paths()
    print(f"   [ok] {len(metadata['column_field_paths'])} column field paths extracted")

    # 5. View definitions
    metadata["views"] = extract_view_definitions()
    print(f"   [ok] {len(metadata['views'])} view definitions extracted")

    # 6. Routines (procedures, functions)
    routines_data = extract_routines()
    metadata["routines"] = routines_data["routines"]
    metadata["routine_parameters"] = routines_data["parameters"]
    print(f"   [ok] {len(metadata['routines'])} routines extracted")

    # 7. Scheduled queries
    metadata["scheduled_queries"] = extract_scheduled_queries()
    print(f"   [ok] {len(metadata['scheduled_queries'])} scheduled queries extracted")

    # 8. Storage metadata
    metadata["storage"] = extract_storage_metadata()
    print(f"   [ok] {len(metadata['storage'])} storage entries extracted")

    # 9. Table constraints
    constraints_data = extract_table_constraints()
    metadata["table_constraints"] = constraints_data["constraints"]
    metadata["key_column_usage"] = constraints_data["key_column_usage"]
    print(f"   [ok] {len(metadata['table_constraints'])} constraints extracted")

    # 10. Table options
    metadata["table_options"] = extract_table_options()
    print(f"   [ok] {len(metadata['table_options'])} table options extracted")

    # 11. Row-level security policies
    metadata["row_level_security"] = extract_row_level_security()
    print(f"   [ok] {len(metadata['row_level_security'])} RLS policies extracted")

    # 12. Snapshots and clones
    metadata["snapshots"] = extract_snapshots_and_clones()
    print(f"   [ok] {len(metadata['snapshots'])} snapshots extracted")

    # 13. Partitions metadata
    metadata["partitions"] = extract_partitions_metadata()
    print(f"   [ok] {len(metadata['partitions'])} partition entries extracted")

    # 14. Streaming buffer stats
    metadata["streaming_buffers"] = extract_streaming_buffer_stats()
    print(f"   [ok] {len(metadata['streaming_buffers'])} streaming buffers extracted")

    # 15. Column-level lineage
    metadata["lineage"] = extract_column_level_lineage()
    print(f"   [ok] {len(metadata['lineage']['table_lineage'])} table lineage edges")
    print(f"   [ok] {len(metadata['lineage']['column_lineage'])} column lineage edges")

    # 16. Data quality signals
    metadata["quality"] = extract_data_quality_signals(metadata["tables"])
    print(f"   [ok] {len(metadata['quality'])} tables profiled")

    # 17. Access/permissions
    metadata["access"] = extract_access_metadata()
    print(f"   [ok] Access metadata extracted")

    # 18. Job history
    metadata["jobs"] = extract_job_history()
    print(f"   [ok] {len(metadata['jobs'])} recent jobs extracted")

    # 19. PII classification
    metadata["pii_classification"] = extract_pii_classification()
    print(f"   [ok] {len(metadata['pii_classification'])} PII columns identified")

    # 20. Search indexes
    metadata["search_indexes"] = extract_search_indexes()
    print(f"   [ok] {len(metadata['search_indexes'])} search indexes extracted")

    # 21. Vector indexes
    metadata["vector_indexes"] = extract_vector_indexes()
    print(f"   [ok] {len(metadata['vector_indexes'])} vector indexes extracted")

    # 22. Materialized views
    metadata["materialized_views"] = extract_materialized_views()
    print(f"   [ok] {len(metadata['materialized_views'])} materialized views extracted")

    # 23. BI Engine statistics
    metadata["bi_engine"] = extract_bi_engine_statistics()
    print(f"   [ok] {len(metadata['bi_engine'])} BI Engine entries extracted")

    # Write output
    output_path = "bigquery_metadata.json"
    with open(output_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    print("=" * 60)
    print(f" Metadata extracted to {output_path}")
    print(f"   File size: {os.path.getsize(output_path) / 1024:.1f} KB")
    print("=" * 60)

    # Print summary
    print("\n Summary:")
    print(f"   Datasets:            {len(metadata['datasets'])}")
    print(f"   Tables/Views:        {len(metadata['tables'])}")
    print(f"   Columns:             {len(metadata['columns'])}")
    print(f"   Column Field Paths:  {len(metadata['column_field_paths'])}")
    print(f"   View Definitions:    {len(metadata['views'])}")
    print(f"   Routines:            {len(metadata['routines'])}")
    print(f"   Routine Parameters:  {len(metadata['routine_parameters'])}")
    print(f"   Scheduled Queries:   {len(metadata['scheduled_queries'])}")
    print(f"   Storage Entries:     {len(metadata['storage'])}")
    print(f"   Table Constraints:   {len(metadata['table_constraints'])}")
    print(f"   Key Column Usage:    {len(metadata['key_column_usage'])}")
    print(f"   Table Options:       {len(metadata['table_options'])}")
    print(f"   RLS Policies:        {len(metadata['row_level_security'])}")
    print(f"   Snapshots:           {len(metadata['snapshots'])}")
    print(f"   Partitions:          {len(metadata['partitions'])}")
    print(f"   Streaming Buffers:   {len(metadata['streaming_buffers'])}")
    print(f"   Table Lineage:       {len(metadata['lineage']['table_lineage'])} edges")
    print(f"   Column Lineage:      {len(metadata['lineage']['column_lineage'])} edges")
    print(f"   PII Columns:         {len(metadata['pii_classification'])}")
    print(f"   Search Indexes:      {len(metadata['search_indexes'])}")
    print(f"   Vector Indexes:      {len(metadata['vector_indexes'])}")
    print(f"   Materialized Views:  {len(metadata['materialized_views'])}")
    print(f"   BI Engine Entries:   {len(metadata['bi_engine'])}")
    print(f"   Recent Jobs:         {len(metadata['jobs'])}")


if __name__ == "__main__":
    main()
