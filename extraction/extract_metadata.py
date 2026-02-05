"""
BigQuery Metadata Extraction Script
====================================
Extracts all metadata that a metadata platform would consume:
1. Schema & table/column descriptions
2. Column-level lineage across views/tables
3. Data quality signals (nulls, distributions, stats)
4. Access/permissions metadata

Output: bigquery_metadata.json

Prerequisites:
  pip install google-cloud-bigquery
  export GOOGLE_APPLICATION_CREDENTIALS=~/bq-spike-sa-key.json
"""

import json
import os
from datetime import datetime
from google.cloud import bigquery

# ── Configuration ──────────────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "bq-metadata-spike")
DATASETS = ["raw_ecommerce", "staging_ecommerce", "analytics_ecommerce", "reporting_ecommerce"]

client = bigquery.Client(project=PROJECT_ID)


def extract_dataset_metadata():
    """Extract dataset-level metadata: descriptions, labels, creation time, location."""
    print("📦 Extracting dataset metadata...")
    datasets = []

    query = f"""
    SELECT
        catalog_name AS project_id,
        schema_name AS dataset_id,
        location,
        creation_time,
        last_modified_time
    FROM `{PROJECT_ID}.INFORMATION_SCHEMA.SCHEMATA`
    WHERE schema_name IN UNNEST(@datasets)
    """

    # Use the Python client for dataset details (INFORMATION_SCHEMA.SCHEMATA
    # doesn't expose labels/descriptions well)
    for dataset_id in DATASETS:
        try:
            ds = client.get_dataset(f"{PROJECT_ID}.{dataset_id}")
            datasets.append({
                "project_id": PROJECT_ID,
                "dataset_id": dataset_id,
                "description": ds.description,
                "location": ds.location,
                "created": ds.created.isoformat() if ds.created else None,
                "modified": ds.modified.isoformat() if ds.modified else None,
                "default_table_expiration_ms": ds.default_table_expiration_ms,
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
            print(f"  ⚠️  Could not fetch dataset {dataset_id}: {e}")

    return datasets


def extract_table_metadata():
    """Extract table-level metadata: row counts, size, type, descriptions, labels, partitioning."""
    print("📋 Extracting table metadata...")
    tables = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            creation_time,
            -- DDL contains column descriptions if set
            ddl
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        """

        try:
            results = client.query(query).result()
            for row in results:
                # Get detailed table info from API for row count, size, labels
                table_ref = f"{PROJECT_ID}.{dataset_id}.{row.table_name}"
                try:
                    table = client.get_table(table_ref)
                    tables.append({
                        "project_id": PROJECT_ID,
                        "dataset_id": dataset_id,
                        "table_name": row.table_name,
                        "table_type": row.table_type,
                        "description": table.description,
                        "created": table.created.isoformat() if table.created else None,
                        "modified": table.modified.isoformat() if table.modified else None,
                        "num_rows": table.num_rows,
                        "num_bytes": table.num_bytes,
                        "size_mb": round(table.num_bytes / (1024 * 1024), 4) if table.num_bytes else 0,
                        "labels": dict(table.labels) if table.labels else {},
                        "partitioning": {
                            "type": table.time_partitioning.type_ if table.time_partitioning else None,
                            "field": table.time_partitioning.field if table.time_partitioning else None
                        },
                        "clustering_fields": list(table.clustering_fields) if table.clustering_fields else [],
                        "schema_fields": [
                            {
                                "name": field.name,
                                "field_type": field.field_type,
                                "mode": field.mode,
                                "description": field.description
                            }
                            for field in table.schema
                        ]
                    })
                except Exception as e:
                    print(f"  ⚠️  Could not fetch table details for {table_ref}: {e}")
        except Exception as e:
            print(f"  ⚠️  Could not query tables in {dataset_id}: {e}")

    return tables


def extract_column_metadata():
    """Extract column-level metadata: types, descriptions, nullability, ordinal position."""
    print("🔤 Extracting column metadata...")
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
            clustering_ordinal_position
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
                    "clustering_ordinal_position": row.clustering_ordinal_position
                })
        except Exception as e:
            print(f"  ⚠️  Could not query columns in {dataset_id}: {e}")

    return columns


def extract_view_definitions():
    """Extract view SQL definitions — critical for lineage parsing."""
    print("👁️  Extracting view definitions...")
    views = []

    for dataset_id in DATASETS:
        query = f"""
        SELECT
            table_name,
            view_definition
        FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.VIEWS`
        """

        try:
            results = client.query(query).result()
            for row in results:
                views.append({
                    "project_id": PROJECT_ID,
                    "dataset_id": dataset_id,
                    "view_name": row.table_name,
                    "view_definition": row.view_definition
                })
        except Exception as e:
            # Not all datasets have views — that's fine
            pass

    return views


def extract_column_level_lineage():
    """
    Extract column-level lineage by parsing view definitions and CTAS query patterns.

    In production, metadata platforms do this by:
    1. Parsing SQL from INFORMATION_SCHEMA.VIEWS
    2. Parsing SQL from INFORMATION_SCHEMA.JOBS (query history)
    3. Using BigQuery's native lineage API (if available)

    For this spike, we derive lineage from our known data architecture.
    """
    print("🔗 Building column-level lineage...")

    lineage = {
        "table_lineage": [],
        "column_lineage": []
    }

    # Table-level lineage (which tables feed which)
    table_lineage_map = [
        # Raw → Staging
        {"upstream": "raw_ecommerce.customers", "downstream": "staging_ecommerce.stg_customers", "transform": "CTAS with dedup + type casting"},
        {"upstream": "raw_ecommerce.products", "downstream": "staging_ecommerce.stg_products", "transform": "CTAS with type casting"},
        {"upstream": "raw_ecommerce.orders", "downstream": "staging_ecommerce.stg_orders", "transform": "CTAS with type casting"},
        {"upstream": "raw_ecommerce.order_items", "downstream": "staging_ecommerce.stg_order_items", "transform": "CTAS with type casting"},
        {"upstream": "raw_ecommerce.payments", "downstream": "staging_ecommerce.stg_payments", "transform": "CTAS with type casting"},
        {"upstream": "raw_ecommerce.shipping_events", "downstream": "staging_ecommerce.stg_shipping_events", "transform": "CTAS with type casting"},

        # Staging → Analytics
        {"upstream": "staging_ecommerce.stg_customers", "downstream": "analytics_ecommerce.customer_360", "transform": "JOIN + aggregation"},
        {"upstream": "staging_ecommerce.stg_orders", "downstream": "analytics_ecommerce.customer_360", "transform": "JOIN + aggregation"},
        {"upstream": "staging_ecommerce.stg_products", "downstream": "analytics_ecommerce.product_performance", "transform": "JOIN + aggregation"},
        {"upstream": "staging_ecommerce.stg_order_items", "downstream": "analytics_ecommerce.product_performance", "transform": "JOIN + aggregation"},
        {"upstream": "staging_ecommerce.stg_orders", "downstream": "analytics_ecommerce.product_performance", "transform": "JOIN for customer_id"},
        {"upstream": "staging_ecommerce.stg_orders", "downstream": "analytics_ecommerce.revenue_daily", "transform": "Aggregation by date"},
        {"upstream": "staging_ecommerce.stg_orders", "downstream": "analytics_ecommerce.order_fulfillment", "transform": "JOIN + pivot"},
        {"upstream": "staging_ecommerce.stg_payments", "downstream": "analytics_ecommerce.order_fulfillment", "transform": "JOIN on order_id"},
        {"upstream": "staging_ecommerce.stg_shipping_events", "downstream": "analytics_ecommerce.order_fulfillment", "transform": "JOIN + pivot"},

        # Analytics → Reporting
        {"upstream": "analytics_ecommerce.revenue_daily", "downstream": "reporting_ecommerce.v_executive_kpis", "transform": "VIEW aggregation"},
        {"upstream": "analytics_ecommerce.customer_360", "downstream": "reporting_ecommerce.v_executive_kpis", "transform": "VIEW aggregation"},
        {"upstream": "analytics_ecommerce.customer_360", "downstream": "reporting_ecommerce.v_customer_segments", "transform": "VIEW grouping"},
        {"upstream": "analytics_ecommerce.product_performance", "downstream": "reporting_ecommerce.v_category_performance", "transform": "VIEW grouping"},
        {"upstream": "analytics_ecommerce.order_fulfillment", "downstream": "reporting_ecommerce.v_fulfillment_sla", "transform": "VIEW grouping"},
    ]
    lineage["table_lineage"] = table_lineage_map

    # Column-level lineage (selected key columns to demonstrate)
    column_lineage_entries = [
        # customer_id flows from raw → staging → analytics → reporting
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
        {"upstream_col": "staging_ecommerce.stg_orders.order_id", "downstream_col": "analytics_ecommerce.customer_360.customer_segment", "transform": "COUNT DISTINCT → CASE (derived)"},

        # product price → margin calculations
        {"upstream_col": "raw_ecommerce.products.unit_price", "downstream_col": "staging_ecommerce.stg_products.unit_price", "transform": "SAFE_CAST to NUMERIC"},
        {"upstream_col": "staging_ecommerce.stg_products.unit_price", "downstream_col": "analytics_ecommerce.product_performance.current_price", "transform": "pass-through"},
        {"upstream_col": "staging_ecommerce.stg_products.cost_price", "downstream_col": "analytics_ecommerce.product_performance.gross_margin", "transform": "SUM(subtotal) - SUM(qty * cost_price)"},
        {"upstream_col": "staging_ecommerce.stg_products.cost_price", "downstream_col": "analytics_ecommerce.product_performance.margin_pct", "transform": "SAFE_DIVIDE(margin, revenue)"},

        # payment card_last_four is PII
        {"upstream_col": "raw_ecommerce.payments.card_last_four", "downstream_col": "staging_ecommerce.stg_payments.card_last_four", "transform": "pass-through (PII)"},

        # shipping → fulfillment
        {"upstream_col": "staging_ecommerce.stg_shipping_events.event_type", "downstream_col": "analytics_ecommerce.order_fulfillment.delivered_at", "transform": "CASE WHEN pivot"},
        {"upstream_col": "staging_ecommerce.stg_shipping_events.event_timestamp", "downstream_col": "analytics_ecommerce.order_fulfillment.delivery_hours", "transform": "TIMESTAMP_DIFF()"},

        # Reporting views
        {"upstream_col": "analytics_ecommerce.revenue_daily.net_revenue", "downstream_col": "reporting_ecommerce.v_executive_kpis.kpi_value", "transform": "SUM → CAST to STRING"},
        {"upstream_col": "analytics_ecommerce.customer_360.customer_segment", "downstream_col": "reporting_ecommerce.v_customer_segments.customer_segment", "transform": "GROUP BY"},
        {"upstream_col": "analytics_ecommerce.customer_360.lifetime_revenue", "downstream_col": "reporting_ecommerce.v_customer_segments.avg_lifetime_revenue", "transform": "AVG → ROUND"},
        {"upstream_col": "analytics_ecommerce.product_performance.total_revenue", "downstream_col": "reporting_ecommerce.v_category_performance.revenue", "transform": "SUM by category"},
        {"upstream_col": "analytics_ecommerce.order_fulfillment.delivery_hours", "downstream_col": "reporting_ecommerce.v_fulfillment_sla.avg_delivery_hours", "transform": "AVG → ROUND"},
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
    print("📊 Extracting data quality signals...")
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
                "note": "View — profile by querying through view"
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
    print("🔐 Extracting access/permissions metadata...")
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
            print(f"  ⚠️  Could not fetch access for {dataset_id}: {e}")

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
    Extract recent query/job history — this is how metadata platforms build usage-based lineage.

    Uses INFORMATION_SCHEMA.JOBS (requires sufficient permissions).
    """
    print("📜 Extracting job/query history...")
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
            destination_table
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
                "query_preview": (row.query or "")[:500],
                "referenced_tables": ref_tables,
                "destination_table": dest
            })
    except Exception as e:
        print(f"  ⚠️  Could not fetch job history: {e}")
        print("  (This requires bigquery.jobs.listAll permission — may not work with basic roles)")

    return jobs


def extract_pii_classification():
    """
    Identify columns likely containing PII — metadata platforms do this via auto-classification.
    We look for column names + labels that suggest PII.
    """
    print("🏷️  Classifying PII columns...")

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


# ── Main Execution ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print("🚀 BigQuery Metadata Extraction — Starting")
    print(f"   Project: {PROJECT_ID}")
    print(f"   Datasets: {', '.join(DATASETS)}")
    print("=" * 60)

    metadata = {
        "extraction_metadata": {
            "project_id": PROJECT_ID,
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "datasets_scanned": DATASETS,
            "extractor_version": "1.0.0-spike"
        }
    }

    # 1. Dataset metadata
    metadata["datasets"] = extract_dataset_metadata()
    print(f"   ✅ {len(metadata['datasets'])} datasets extracted")

    # 2. Table metadata (includes column schemas)
    metadata["tables"] = extract_table_metadata()
    print(f"   ✅ {len(metadata['tables'])} tables/views extracted")

    # 3. Detailed column metadata
    metadata["columns"] = extract_column_metadata()
    print(f"   ✅ {len(metadata['columns'])} columns extracted")

    # 4. View definitions
    metadata["views"] = extract_view_definitions()
    print(f"   ✅ {len(metadata['views'])} view definitions extracted")

    # 5. Column-level lineage
    metadata["lineage"] = extract_column_level_lineage()
    print(f"   ✅ {len(metadata['lineage']['table_lineage'])} table lineage edges")
    print(f"   ✅ {len(metadata['lineage']['column_lineage'])} column lineage edges")

    # 6. Data quality signals
    metadata["quality"] = extract_data_quality_signals(metadata["tables"])
    print(f"   ✅ {len(metadata['quality'])} tables profiled")

    # 7. Access/permissions
    metadata["access"] = extract_access_metadata()
    print(f"   ✅ Access metadata extracted")

    # 8. Job history
    metadata["jobs"] = extract_job_history()
    print(f"   ✅ {len(metadata['jobs'])} recent jobs extracted")

    # 9. PII classification
    metadata["pii_classification"] = extract_pii_classification()
    print(f"   ✅ {len(metadata['pii_classification'])} PII columns identified")

    # Write output
    output_path = "bigquery_metadata.json"
    with open(output_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    print("=" * 60)
    print(f"✅ Metadata extracted to {output_path}")
    print(f"   File size: {os.path.getsize(output_path) / 1024:.1f} KB")
    print("=" * 60)

    # Print summary
    print("\n📈 Summary:")
    print(f"   Datasets:        {len(metadata['datasets'])}")
    print(f"   Tables/Views:    {len(metadata['tables'])}")
    print(f"   Columns:         {len(metadata['columns'])}")
    print(f"   View Defs:       {len(metadata['views'])}")
    print(f"   Table Lineage:   {len(metadata['lineage']['table_lineage'])} edges")
    print(f"   Column Lineage:  {len(metadata['lineage']['column_lineage'])} edges")
    print(f"   PII Columns:     {len(metadata['pii_classification'])}")
    print(f"   Recent Jobs:     {len(metadata['jobs'])}")


if __name__ == "__main__":
    main()
