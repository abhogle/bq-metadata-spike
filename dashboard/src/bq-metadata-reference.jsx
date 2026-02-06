import { useState, useMemo } from "react";

// BigQuery Metadata Reference Component
// Displays all 160+ metadata fields organized by category with search and filtering

const C = {
  bg: "#0B0E14",
  surface: "#151921",
  border: "#1E2430",
  text: "#E6EDF3",
  textMuted: "#8B949E",
  textDim: "#484F58",
  accent: "#58A6FF",
  raw: "#F97316",
  staging: "#FACC15",
  analytics: "#22C55E",
  reporting: "#8B5CF6",
};

// All BigQuery metadata fields organized by category
const METADATA_FIELDS = [
  {
    category: "Dataset Metadata",
    icon: "📁",
    description: "Dataset-level configuration and properties",
    fields: [
      { name: "project_id", type: "STRING", description: "GCP project containing the dataset", source: "INFORMATION_SCHEMA.SCHEMATA" },
      { name: "schema_name", type: "STRING", description: "Dataset name (schema_name in ANSI SQL terms)", source: "INFORMATION_SCHEMA.SCHEMATA" },
      { name: "location", type: "STRING", description: "Geographic location (US, EU, region)", source: "INFORMATION_SCHEMA.SCHEMATA" },
      { name: "creation_time", type: "TIMESTAMP", description: "When the dataset was created", source: "INFORMATION_SCHEMA.SCHEMATA" },
      { name: "last_modified_time", type: "TIMESTAMP", description: "Last metadata modification time", source: "INFORMATION_SCHEMA.SCHEMATA" },
      { name: "default_collation_name", type: "STRING", description: "Default collation for string comparisons", source: "INFORMATION_SCHEMA.SCHEMATA" },
      { name: "default_table_expiration_ms", type: "INT64", description: "Default table lifetime in milliseconds", source: "Dataset API" },
      { name: "description", type: "STRING", description: "Human-readable dataset description", source: "Dataset API" },
      { name: "labels", type: "RECORD", description: "Key-value labels for organization", source: "Dataset API" },
      { name: "access", type: "RECORD[]", description: "IAM access control entries", source: "Dataset API" },
      { name: "default_encryption_configuration", type: "RECORD", description: "Customer-managed encryption key", source: "Dataset API" },
    ],
  },
  {
    category: "Table Metadata",
    icon: "📋",
    description: "Core table properties and structure",
    fields: [
      { name: "table_catalog", type: "STRING", description: "Project ID (catalog in ANSI terms)", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "table_schema", type: "STRING", description: "Dataset ID (schema in ANSI terms)", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "table_name", type: "STRING", description: "Table name", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "table_type", type: "STRING", description: "BASE TABLE, VIEW, MATERIALIZED VIEW, EXTERNAL, SNAPSHOT", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "is_insertable_into", type: "STRING", description: "YES if data can be inserted", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "is_typed", type: "STRING", description: "YES if table has typed structure", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "creation_time", type: "TIMESTAMP", description: "When the table was created", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "ddl", type: "STRING", description: "DDL statement to recreate the table", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "base_table_catalog", type: "STRING", description: "For clones: source project", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "base_table_schema", type: "STRING", description: "For clones: source dataset", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "base_table_name", type: "STRING", description: "For clones: source table", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "snapshot_time_ms", type: "TIMESTAMP", description: "For snapshots: point-in-time", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "replica_source_catalog", type: "STRING", description: "For replicas: source project", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "replica_source_schema", type: "STRING", description: "For replicas: source dataset", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "replica_source_name", type: "STRING", description: "For replicas: source table", source: "INFORMATION_SCHEMA.TABLES" },
      { name: "upsert_stream_apply_watermark", type: "TIMESTAMP", description: "For CDC tables: watermark", source: "INFORMATION_SCHEMA.TABLES" },
    ],
  },
  {
    category: "Column Metadata",
    icon: "📊",
    description: "Column-level schema information",
    fields: [
      { name: "column_name", type: "STRING", description: "Column name", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "ordinal_position", type: "INT64", description: "1-based column position", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "is_nullable", type: "STRING", description: "YES or NO", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "data_type", type: "STRING", description: "Full data type with parameters", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "is_generated", type: "STRING", description: "ALWAYS if generated column", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "generation_expression", type: "STRING", description: "Expression for generated columns", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "is_stored", type: "STRING", description: "YES if generated column is stored", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "is_hidden", type: "STRING", description: "YES if column is hidden", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "is_updatable", type: "STRING", description: "YES if column can be updated", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "is_system_defined", type: "STRING", description: "YES for system columns (_PARTITIONTIME)", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "is_partitioning_column", type: "STRING", description: "YES if used for partitioning", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "clustering_ordinal_position", type: "INT64", description: "Position in clustering (NULL if not clustered)", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "collation_name", type: "STRING", description: "String collation setting", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "column_default", type: "STRING", description: "Default value expression", source: "INFORMATION_SCHEMA.COLUMNS" },
      { name: "rounding_mode", type: "STRING", description: "Numeric rounding mode", source: "INFORMATION_SCHEMA.COLUMNS" },
    ],
  },
  {
    category: "Column Field Paths",
    icon: "🌳",
    description: "Nested field paths for STRUCT/RECORD columns",
    fields: [
      { name: "field_path", type: "STRING", description: "Dot-notation path (e.g., 'address.city')", source: "INFORMATION_SCHEMA.COLUMN_FIELD_PATHS" },
      { name: "data_type", type: "STRING", description: "Data type of the nested field", source: "INFORMATION_SCHEMA.COLUMN_FIELD_PATHS" },
      { name: "description", type: "STRING", description: "Field description", source: "INFORMATION_SCHEMA.COLUMN_FIELD_PATHS" },
      { name: "collation_name", type: "STRING", description: "String collation for the field", source: "INFORMATION_SCHEMA.COLUMN_FIELD_PATHS" },
      { name: "rounding_mode", type: "STRING", description: "Numeric rounding mode", source: "INFORMATION_SCHEMA.COLUMN_FIELD_PATHS" },
    ],
  },
  {
    category: "Table Options",
    icon: "⚙️",
    description: "Table configuration options set via DDL",
    fields: [
      { name: "option_name", type: "STRING", description: "Option name (e.g., 'description', 'labels')", source: "INFORMATION_SCHEMA.TABLE_OPTIONS" },
      { name: "option_type", type: "STRING", description: "Data type of the option value", source: "INFORMATION_SCHEMA.TABLE_OPTIONS" },
      { name: "option_value", type: "STRING", description: "Option value as string", source: "INFORMATION_SCHEMA.TABLE_OPTIONS" },
      { name: "friendly_name", type: "STRING", description: "Human-readable table name", option: true },
      { name: "description", type: "STRING", description: "Table description", option: true },
      { name: "labels", type: "ARRAY<STRUCT>", description: "Key-value labels", option: true },
      { name: "expiration_timestamp", type: "TIMESTAMP", description: "When table will be auto-deleted", option: true },
      { name: "partition_expiration_days", type: "INT64", description: "Days before partitions expire", option: true },
      { name: "require_partition_filter", type: "BOOL", description: "Force partition filter in queries", option: true },
      { name: "kms_key_name", type: "STRING", description: "Customer-managed encryption key", option: true },
      { name: "enable_refresh", type: "BOOL", description: "For MV: enable auto-refresh", option: true },
      { name: "refresh_interval_minutes", type: "INT64", description: "For MV: refresh frequency", option: true },
      { name: "max_staleness", type: "INTERVAL", description: "For MV: max data staleness", option: true },
    ],
  },
  {
    category: "Partitioning & Clustering",
    icon: "📅",
    description: "Table partitioning and clustering configuration",
    fields: [
      { name: "partition_id", type: "STRING", description: "Partition identifier (__NULL__, __UNPARTITIONED__, date)", source: "INFORMATION_SCHEMA.PARTITIONS" },
      { name: "total_rows", type: "INT64", description: "Rows in the partition", source: "INFORMATION_SCHEMA.PARTITIONS" },
      { name: "total_logical_bytes", type: "INT64", description: "Logical bytes in partition", source: "INFORMATION_SCHEMA.PARTITIONS" },
      { name: "total_billable_bytes", type: "INT64", description: "Billable bytes in partition", source: "INFORMATION_SCHEMA.PARTITIONS" },
      { name: "last_modified_time", type: "TIMESTAMP", description: "When partition was last modified", source: "INFORMATION_SCHEMA.PARTITIONS" },
      { name: "storage_tier", type: "STRING", description: "ACTIVE or LONG_TERM storage", source: "INFORMATION_SCHEMA.PARTITIONS" },
      { name: "time_partitioning.type", type: "STRING", description: "DAY, HOUR, MONTH, YEAR", source: "Table API" },
      { name: "time_partitioning.field", type: "STRING", description: "Column used for partitioning", source: "Table API" },
      { name: "range_partitioning.field", type: "STRING", description: "Column for integer range partitioning", source: "Table API" },
      { name: "range_partitioning.range", type: "RECORD", description: "Start, end, interval for ranges", source: "Table API" },
      { name: "clustering_fields", type: "ARRAY<STRING>", description: "Columns for clustering (up to 4)", source: "Table API" },
    ],
  },
  {
    category: "Storage Metadata",
    icon: "💾",
    description: "Physical and logical storage statistics",
    fields: [
      { name: "total_rows", type: "INT64", description: "Total row count", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "total_partitions", type: "INT64", description: "Number of partitions", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "total_logical_bytes", type: "INT64", description: "Uncompressed logical size", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "active_logical_bytes", type: "INT64", description: "Logical bytes in active storage", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "long_term_logical_bytes", type: "INT64", description: "Logical bytes in long-term storage", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "total_physical_bytes", type: "INT64", description: "Compressed physical size", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "active_physical_bytes", type: "INT64", description: "Physical bytes in active storage", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "long_term_physical_bytes", type: "INT64", description: "Physical bytes in long-term storage", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "time_travel_physical_bytes", type: "INT64", description: "Bytes for time travel window", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "storage_last_modified_time", type: "TIMESTAMP", description: "Last storage modification", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
      { name: "deleted", type: "BOOL", description: "Whether table is soft-deleted", source: "INFORMATION_SCHEMA.TABLE_STORAGE" },
    ],
  },
  {
    category: "Streaming Buffer",
    icon: "🌊",
    description: "Real-time streaming insert buffer stats",
    fields: [
      { name: "estimated_bytes", type: "INT64", description: "Bytes in streaming buffer", source: "Table API (streaming_buffer)" },
      { name: "estimated_rows", type: "INT64", description: "Rows in streaming buffer", source: "Table API (streaming_buffer)" },
      { name: "oldest_entry_time", type: "TIMESTAMP", description: "Oldest row in buffer", source: "Table API (streaming_buffer)" },
    ],
  },
  {
    category: "Table Constraints",
    icon: "🔗",
    description: "Primary keys and foreign key relationships",
    fields: [
      { name: "constraint_catalog", type: "STRING", description: "Project containing constraint", source: "INFORMATION_SCHEMA.TABLE_CONSTRAINTS" },
      { name: "constraint_schema", type: "STRING", description: "Dataset containing constraint", source: "INFORMATION_SCHEMA.TABLE_CONSTRAINTS" },
      { name: "constraint_name", type: "STRING", description: "Constraint name", source: "INFORMATION_SCHEMA.TABLE_CONSTRAINTS" },
      { name: "constraint_type", type: "STRING", description: "PRIMARY KEY or FOREIGN KEY", source: "INFORMATION_SCHEMA.TABLE_CONSTRAINTS" },
      { name: "is_deferrable", type: "STRING", description: "Whether constraint is deferrable", source: "INFORMATION_SCHEMA.TABLE_CONSTRAINTS" },
      { name: "initially_deferred", type: "STRING", description: "Whether initially deferred", source: "INFORMATION_SCHEMA.TABLE_CONSTRAINTS" },
      { name: "enforced", type: "STRING", description: "YES or NO (BQ uses NOT ENFORCED)", source: "INFORMATION_SCHEMA.TABLE_CONSTRAINTS" },
      { name: "column_name", type: "STRING", description: "Column in constraint", source: "INFORMATION_SCHEMA.KEY_COLUMN_USAGE" },
      { name: "ordinal_position", type: "INT64", description: "Position in composite key", source: "INFORMATION_SCHEMA.KEY_COLUMN_USAGE" },
      { name: "position_in_unique_constraint", type: "INT64", description: "Position in referenced key", source: "INFORMATION_SCHEMA.KEY_COLUMN_USAGE" },
    ],
  },
  {
    category: "Views",
    icon: "👁️",
    description: "View definitions and properties",
    fields: [
      { name: "table_catalog", type: "STRING", description: "Project containing view", source: "INFORMATION_SCHEMA.VIEWS" },
      { name: "table_schema", type: "STRING", description: "Dataset containing view", source: "INFORMATION_SCHEMA.VIEWS" },
      { name: "table_name", type: "STRING", description: "View name", source: "INFORMATION_SCHEMA.VIEWS" },
      { name: "view_definition", type: "STRING", description: "SQL query defining the view", source: "INFORMATION_SCHEMA.VIEWS" },
      { name: "check_option", type: "STRING", description: "Check option for updatable views", source: "INFORMATION_SCHEMA.VIEWS" },
      { name: "use_standard_sql", type: "STRING", description: "YES for standard SQL, NO for legacy", source: "INFORMATION_SCHEMA.VIEWS" },
    ],
  },
  {
    category: "Materialized Views",
    icon: "🔄",
    description: "Materialized view configuration and refresh stats",
    fields: [
      { name: "table_name", type: "STRING", description: "Materialized view name", source: "INFORMATION_SCHEMA.MATERIALIZED_VIEWS" },
      { name: "last_refresh_time", type: "TIMESTAMP", description: "When last refreshed", source: "INFORMATION_SCHEMA.MATERIALIZED_VIEWS" },
      { name: "refresh_watermark", type: "TIMESTAMP", description: "Data freshness watermark", source: "INFORMATION_SCHEMA.MATERIALIZED_VIEWS" },
      { name: "last_refresh_status", type: "RECORD", description: "Status of last refresh job", source: "INFORMATION_SCHEMA.MATERIALIZED_VIEWS" },
      { name: "enable_refresh", type: "BOOL", description: "Auto-refresh enabled", source: "Table Options" },
      { name: "refresh_interval_minutes", type: "INT64", description: "Minutes between refreshes", source: "Table Options" },
    ],
  },
  {
    category: "Routines (UDFs & Procedures)",
    icon: "⚡",
    description: "User-defined functions and stored procedures",
    fields: [
      { name: "routine_catalog", type: "STRING", description: "Project containing routine", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "routine_schema", type: "STRING", description: "Dataset containing routine", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "routine_name", type: "STRING", description: "Routine name", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "routine_type", type: "STRING", description: "FUNCTION, PROCEDURE, TABLE FUNCTION", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "data_type", type: "STRING", description: "Return type for functions", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "routine_body", type: "STRING", description: "SQL or EXTERNAL", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "routine_definition", type: "STRING", description: "Function/procedure body", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "external_language", type: "STRING", description: "Language for external routines (JS)", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "is_deterministic", type: "STRING", description: "YES if deterministic", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "security_type", type: "STRING", description: "INVOKER or DEFINER security", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "created", type: "TIMESTAMP", description: "Creation time", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "last_modified", type: "TIMESTAMP", description: "Last modification time", source: "INFORMATION_SCHEMA.ROUTINES" },
      { name: "parameter_name", type: "STRING", description: "Parameter name", source: "INFORMATION_SCHEMA.PARAMETERS" },
      { name: "parameter_mode", type: "STRING", description: "IN, OUT, INOUT", source: "INFORMATION_SCHEMA.PARAMETERS" },
    ],
  },
  {
    category: "Row Access Policies",
    icon: "🔒",
    description: "Row-level security policies",
    fields: [
      { name: "row_access_policy_name", type: "STRING", description: "Policy name", source: "INFORMATION_SCHEMA.ROW_ACCESS_POLICIES" },
      { name: "grantees", type: "ARRAY<STRING>", description: "Users/groups granted access", source: "INFORMATION_SCHEMA.ROW_ACCESS_POLICIES" },
      { name: "filter_predicate", type: "STRING", description: "Row filter expression", source: "INFORMATION_SCHEMA.ROW_ACCESS_POLICIES" },
      { name: "creation_time", type: "TIMESTAMP", description: "When policy was created", source: "INFORMATION_SCHEMA.ROW_ACCESS_POLICIES" },
      { name: "last_modified_time", type: "TIMESTAMP", description: "Last modification time", source: "INFORMATION_SCHEMA.ROW_ACCESS_POLICIES" },
    ],
  },
  {
    category: "Search & Vector Indexes",
    icon: "🔍",
    description: "Full-text search and vector similarity indexes",
    fields: [
      { name: "index_name", type: "STRING", description: "Index name", source: "INFORMATION_SCHEMA.SEARCH_INDEXES" },
      { name: "index_status", type: "STRING", description: "ACTIVE, PENDING, DISABLED", source: "INFORMATION_SCHEMA.SEARCH_INDEXES" },
      { name: "creation_time", type: "TIMESTAMP", description: "When index was created", source: "INFORMATION_SCHEMA.SEARCH_INDEXES" },
      { name: "last_modification_time", type: "TIMESTAMP", description: "Last modification", source: "INFORMATION_SCHEMA.SEARCH_INDEXES" },
      { name: "covered_column_count", type: "INT64", description: "Columns in search index", source: "INFORMATION_SCHEMA.SEARCH_INDEXES" },
      { name: "analyzer", type: "STRING", description: "NO_OP_ANALYZER or LOG_ANALYZER", source: "INFORMATION_SCHEMA.SEARCH_INDEXES" },
      { name: "total_logical_bytes", type: "INT64", description: "Bytes consumed by index", source: "INFORMATION_SCHEMA.SEARCH_INDEXES" },
      { name: "vector_index_type", type: "STRING", description: "IVF, TREE_AH, etc.", source: "INFORMATION_SCHEMA.VECTOR_INDEXES" },
      { name: "distance_type", type: "STRING", description: "COSINE, EUCLIDEAN, DOT_PRODUCT", source: "INFORMATION_SCHEMA.VECTOR_INDEXES" },
    ],
  },
  {
    category: "Scheduled Queries",
    icon: "⏰",
    description: "Scheduled query transfers via Data Transfer Service",
    fields: [
      { name: "config_id", type: "STRING", description: "Transfer config ID", source: "Data Transfer API" },
      { name: "display_name", type: "STRING", description: "Human-readable name", source: "Data Transfer API" },
      { name: "destination_dataset_id", type: "STRING", description: "Target dataset", source: "Data Transfer API" },
      { name: "schedule", type: "STRING", description: "Cron-like schedule string", source: "Data Transfer API" },
      { name: "state", type: "STRING", description: "PENDING, RUNNING, SUCCEEDED, FAILED", source: "Data Transfer API" },
      { name: "next_run_time", type: "TIMESTAMP", description: "Next scheduled execution", source: "Data Transfer API" },
      { name: "params.query", type: "STRING", description: "SQL query to execute", source: "Data Transfer API" },
      { name: "update_time", type: "TIMESTAMP", description: "Last config update", source: "Data Transfer API" },
    ],
  },
  {
    category: "Job History",
    icon: "📜",
    description: "Query execution history and statistics",
    fields: [
      { name: "job_id", type: "STRING", description: "Unique job identifier", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "creation_time", type: "TIMESTAMP", description: "When job was created", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "start_time", type: "TIMESTAMP", description: "When job started executing", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "end_time", type: "TIMESTAMP", description: "When job completed", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "state", type: "STRING", description: "PENDING, RUNNING, DONE", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "query", type: "STRING", description: "SQL query text", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "user_email", type: "STRING", description: "User who ran the query", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "total_bytes_processed", type: "INT64", description: "Bytes scanned", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "total_bytes_billed", type: "INT64", description: "Bytes billed (min 10MB)", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "total_slot_ms", type: "INT64", description: "Slot milliseconds consumed", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "cache_hit", type: "BOOL", description: "Whether results were cached", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "destination_table", type: "RECORD", description: "Result destination", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "referenced_tables", type: "ARRAY<RECORD>", description: "Tables read by query", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "error_result", type: "RECORD", description: "Error details if failed", source: "INFORMATION_SCHEMA.JOBS" },
      { name: "priority", type: "STRING", description: "INTERACTIVE or BATCH", source: "INFORMATION_SCHEMA.JOBS" },
    ],
  },
];

const TOTAL_FIELDS = METADATA_FIELDS.reduce((sum, cat) => sum + cat.fields.length, 0);

// Export all fields to CSV
function exportToCSV() {
  const headers = ["Category", "Field Name", "Type", "Description", "Source"];
  const rows = [];

  METADATA_FIELDS.forEach(cat => {
    cat.fields.forEach(field => {
      rows.push([
        cat.category,
        field.name,
        field.type,
        // Escape quotes and commas in description
        `"${(field.description || "").replace(/"/g, '""')}"`,
        field.source || ""
      ]);
    });
  });

  const csvContent = [
    headers.join(","),
    ...rows.map(row => row.join(","))
  ].join("\n");

  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.setAttribute("href", url);
  link.setAttribute("download", "bigquery_metadata_reference.csv");
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export default function BQMetadataReference() {
  const [search, setSearch] = useState("");
  const [selectedCategory, setSelectedCategory] = useState(null);
  const [expandedCategories, setExpandedCategories] = useState(new Set(METADATA_FIELDS.map(c => c.category)));

  const filteredCategories = useMemo(() => {
    if (!search) return METADATA_FIELDS;
    const s = search.toLowerCase();
    return METADATA_FIELDS.map(cat => ({
      ...cat,
      fields: cat.fields.filter(f =>
        f.name.toLowerCase().includes(s) ||
        f.description.toLowerCase().includes(s) ||
        f.type.toLowerCase().includes(s) ||
        (f.source && f.source.toLowerCase().includes(s))
      ),
    })).filter(cat => cat.fields.length > 0);
  }, [search]);

  const toggleCategory = (cat) => {
    const newSet = new Set(expandedCategories);
    if (newSet.has(cat)) {
      newSet.delete(cat);
    } else {
      newSet.add(cat);
    }
    setExpandedCategories(newSet);
  };

  const filteredFieldCount = filteredCategories.reduce((sum, cat) => sum + cat.fields.length, 0);

  return (
    <div style={{ padding: 20, fontFamily: "'IBM Plex Sans', sans-serif" }}>
      {/* Header */}
      <div style={{ marginBottom: 20, display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 600, color: C.text }}>
            BigQuery Metadata Reference
          </h2>
          <p style={{ margin: "8px 0 0 0", color: C.textMuted, fontSize: 13 }}>
            {TOTAL_FIELDS} metadata fields across {METADATA_FIELDS.length} categories
          </p>
        </div>
        <button
          onClick={exportToCSV}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 6,
            padding: "8px 14px",
            background: C.surface,
            border: `1px solid ${C.border}`,
            borderRadius: 6,
            color: C.text,
            fontSize: 12,
            cursor: "pointer",
            transition: "all 0.15s",
          }}
          onMouseOver={(e) => e.currentTarget.style.borderColor = C.accent}
          onMouseOut={(e) => e.currentTarget.style.borderColor = C.border}
        >
          <span>📥</span> Export CSV
        </button>
      </div>

      {/* Search */}
      <div style={{ marginBottom: 20 }}>
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search fields, types, or sources..."
          style={{
            width: "100%",
            maxWidth: 400,
            padding: "10px 14px",
            background: C.surface,
            border: `1px solid ${C.border}`,
            borderRadius: 6,
            color: C.text,
            fontSize: 13,
            outline: "none",
          }}
        />
        {search && (
          <span style={{ marginLeft: 12, color: C.textMuted, fontSize: 12 }}>
            {filteredFieldCount} field{filteredFieldCount !== 1 ? "s" : ""} found
          </span>
        )}
      </div>

      {/* Categories */}
      <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        {filteredCategories.map((cat) => (
          <div
            key={cat.category}
            style={{
              background: C.surface,
              borderRadius: 8,
              border: `1px solid ${C.border}`,
              overflow: "hidden",
            }}
          >
            {/* Category Header */}
            <div
              onClick={() => toggleCategory(cat.category)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 12,
                padding: "14px 16px",
                cursor: "pointer",
                borderBottom: expandedCategories.has(cat.category) ? `1px solid ${C.border}` : "none",
              }}
            >
              <span style={{ fontSize: 18 }}>{cat.icon}</span>
              <div style={{ flex: 1 }}>
                <div style={{ fontWeight: 600, color: C.text, fontSize: 14 }}>{cat.category}</div>
                <div style={{ color: C.textMuted, fontSize: 11 }}>{cat.description}</div>
              </div>
              <span style={{
                background: C.bg,
                padding: "4px 10px",
                borderRadius: 12,
                fontSize: 11,
                color: C.textMuted,
              }}>
                {cat.fields.length} fields
              </span>
              <span style={{ color: C.textDim, fontSize: 12 }}>
                {expandedCategories.has(cat.category) ? "▼" : "▶"}
              </span>
            </div>

            {/* Fields Table */}
            {expandedCategories.has(cat.category) && (
              <div style={{ padding: "0 16px 16px" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr style={{ textAlign: "left", color: C.textDim, fontSize: 10, textTransform: "uppercase" }}>
                      <th style={{ padding: "10px 8px", borderBottom: `1px solid ${C.border}` }}>Field Name</th>
                      <th style={{ padding: "10px 8px", borderBottom: `1px solid ${C.border}` }}>Type</th>
                      <th style={{ padding: "10px 8px", borderBottom: `1px solid ${C.border}` }}>Description</th>
                      <th style={{ padding: "10px 8px", borderBottom: `1px solid ${C.border}` }}>Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cat.fields.map((field, i) => (
                      <tr key={field.name} style={{ background: i % 2 === 0 ? "transparent" : C.bg }}>
                        <td style={{ padding: "8px", fontFamily: "'JetBrains Mono', monospace", color: C.accent, fontSize: 11 }}>
                          {field.name}
                        </td>
                        <td style={{ padding: "8px", color: C.textMuted, fontFamily: "monospace", fontSize: 10 }}>
                          {field.type}
                        </td>
                        <td style={{ padding: "8px", color: C.text, fontSize: 11 }}>
                          {field.description}
                        </td>
                        <td style={{ padding: "8px", color: C.textDim, fontSize: 10 }}>
                          {field.source || "-"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Footer */}
      <div style={{ marginTop: 24, padding: 16, background: C.surface, borderRadius: 8, border: `1px solid ${C.border}` }}>
        <div style={{ fontSize: 12, color: C.textMuted }}>
          <strong style={{ color: C.text }}>Sources:</strong> Metadata is extracted from INFORMATION_SCHEMA views,
          BigQuery API (Python client), and Data Transfer API. Some fields are derived or computed during extraction.
        </div>
      </div>
    </div>
  );
}
