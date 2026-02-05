-- ============================================================
-- Layer: Metadata Enrichment
-- Purpose: Enrich all tables with comprehensive metadata for demo
-- Run: bq query --use_legacy_sql=false < sql/06-enrich-metadata.sql
-- ============================================================

-- ============================================================
-- 1. ADD FRIENDLY NAMES TO ALL TABLES
-- ============================================================

-- Raw tables (6)
ALTER TABLE `bq-metadata-spike.raw_ecommerce.customers`
SET OPTIONS (friendly_name = "Raw Customers (CRM Source)");

ALTER TABLE `bq-metadata-spike.raw_ecommerce.products`
SET OPTIONS (friendly_name = "Raw Products (Catalog Source)");

ALTER TABLE `bq-metadata-spike.raw_ecommerce.orders`
SET OPTIONS (friendly_name = "Raw Orders (OMS Source)");

ALTER TABLE `bq-metadata-spike.raw_ecommerce.order_items`
SET OPTIONS (friendly_name = "Raw Order Items (OMS Source)");

ALTER TABLE `bq-metadata-spike.raw_ecommerce.payments`
SET OPTIONS (friendly_name = "Raw Payments (Stripe Source)");

ALTER TABLE `bq-metadata-spike.raw_ecommerce.shipping_events`
SET OPTIONS (friendly_name = "Raw Shipping Events (Logistics Source)");

-- Staging tables (6)
ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_customers`
SET OPTIONS (friendly_name = "Staged Customers (Cleaned)");

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_products`
SET OPTIONS (friendly_name = "Staged Products (Cleaned)");

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_orders`
SET OPTIONS (friendly_name = "Staged Orders (Cleaned)");

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_order_items`
SET OPTIONS (friendly_name = "Staged Order Items (Cleaned)");

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_payments`
SET OPTIONS (friendly_name = "Staged Payments (Cleaned)");

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_shipping_events`
SET OPTIONS (friendly_name = "Staged Shipping Events (Cleaned)");

-- Analytics tables (4)
ALTER TABLE `bq-metadata-spike.analytics_ecommerce.customer_360`
SET OPTIONS (friendly_name = "Customer 360 Profile");

ALTER TABLE `bq-metadata-spike.analytics_ecommerce.product_performance`
SET OPTIONS (friendly_name = "Product Performance Metrics");

ALTER TABLE `bq-metadata-spike.analytics_ecommerce.revenue_daily`
SET OPTIONS (friendly_name = "Daily Revenue Analytics");

ALTER TABLE `bq-metadata-spike.analytics_ecommerce.order_fulfillment`
SET OPTIONS (friendly_name = "Order Fulfillment Funnel");


-- ============================================================
-- 2. SET TABLE EXPIRATION ON RAW TABLES (90 days)
-- ============================================================

ALTER TABLE `bq-metadata-spike.raw_ecommerce.customers`
SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 90 DAY));

ALTER TABLE `bq-metadata-spike.raw_ecommerce.products`
SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 90 DAY));

ALTER TABLE `bq-metadata-spike.raw_ecommerce.orders`
SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 90 DAY));

ALTER TABLE `bq-metadata-spike.raw_ecommerce.order_items`
SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 90 DAY));

ALTER TABLE `bq-metadata-spike.raw_ecommerce.payments`
SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 90 DAY));

ALTER TABLE `bq-metadata-spike.raw_ecommerce.shipping_events`
SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 90 DAY));


-- ============================================================
-- 3. RECREATE TABLES WITH PARTITIONING AND CLUSTERING
-- Note: Must DROP first because you can't change partitioning with CREATE OR REPLACE
-- ============================================================

-- 3a. Recreate analytics_ecommerce.revenue_daily with PARTITION BY order_date
DROP TABLE IF EXISTS `bq-metadata-spike.analytics_ecommerce.revenue_daily`;
CREATE TABLE `bq-metadata-spike.analytics_ecommerce.revenue_daily`
PARTITION BY order_date
OPTIONS(
  description = "Daily revenue aggregation with order counts and average order value. Source of truth for revenue reporting. Partitioned by order_date for efficient time-range queries.",
  labels = [("owner", "finance-analytics"), ("domain", "finance"), ("layer", "analytics"), ("certified", "true")],
  require_partition_filter = true,
  friendly_name = "Daily Revenue (Partitioned)"
)
AS
SELECT
  o.order_date,
  EXTRACT(YEAR FROM o.order_date) AS year,
  EXTRACT(MONTH FROM o.order_date) AS month,
  EXTRACT(DAYOFWEEK FROM o.order_date) AS day_of_week,
  COUNT(DISTINCT o.order_id) AS order_count,
  SUM(o.total_amount) AS gross_revenue,
  SUM(o.discount_amount) AS total_discounts,
  SUM(o.tax_amount) AS total_tax,
  SUM(o.total_amount) - SUM(o.discount_amount) AS net_revenue,
  AVG(o.total_amount) AS avg_order_value,
  COUNT(DISTINCT o.customer_id) AS unique_customers
FROM `bq-metadata-spike.staging_ecommerce.stg_orders` o
WHERE o.order_status IN ('completed', 'shipped')
GROUP BY 1, 2, 3, 4;


-- 3b. Recreate analytics_ecommerce.order_fulfillment with CLUSTER BY order_status, payment_method
DROP TABLE IF EXISTS `bq-metadata-spike.analytics_ecommerce.order_fulfillment`;
CREATE TABLE `bq-metadata-spike.analytics_ecommerce.order_fulfillment`
CLUSTER BY order_status, payment_method
OPTIONS(
  description = "Order-level fulfillment tracking: combines order, payment, and shipping data for ops metrics. Clustered by order_status and payment_method for efficient filtering.",
  labels = [("owner", "ops-analytics"), ("domain", "operations"), ("layer", "analytics"), ("certified", "true")],
  friendly_name = "Order Fulfillment Funnel (Clustered)"
)
AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status,
  o.shipping_method,
  o.total_amount,
  p.payment_method,
  p.payment_status,
  p.processed_at AS payment_processed_at,
  MIN(CASE WHEN se.event_type = 'picked_up' THEN se.event_timestamp END) AS picked_up_at,
  MIN(CASE WHEN se.event_type = 'delivered' THEN se.event_timestamp END) AS delivered_at,
  MIN(CASE WHEN se.event_type = 'returned' THEN se.event_timestamp END) AS returned_at,
  TIMESTAMP_DIFF(
    MIN(CASE WHEN se.event_type = 'delivered' THEN se.event_timestamp END),
    MIN(CASE WHEN se.event_type = 'picked_up' THEN se.event_timestamp END),
    HOUR
  ) AS delivery_hours
FROM `bq-metadata-spike.staging_ecommerce.stg_orders` o
LEFT JOIN `bq-metadata-spike.staging_ecommerce.stg_payments` p ON o.order_id = p.order_id
LEFT JOIN `bq-metadata-spike.staging_ecommerce.stg_shipping_events` se ON o.order_id = se.order_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;


-- 3c. Recreate staging_ecommerce.stg_orders with PARTITION BY order_date and CLUSTER BY customer_id
DROP TABLE IF EXISTS `bq-metadata-spike.staging_ecommerce.stg_orders`;
CREATE TABLE `bq-metadata-spike.staging_ecommerce.stg_orders`
PARTITION BY order_date
CLUSTER BY customer_id
OPTIONS(
  description = "Cleaned orders. Amounts cast to NUMERIC. Dates parsed. Partitioned by order_date and clustered by customer_id.",
  labels = [("pii", "false"), ("owner", "data-engineering"), ("layer", "staging"), ("certified", "true")],
  friendly_name = "Staged Orders (Partitioned & Clustered)"
)
AS
SELECT
  order_id,
  customer_id,
  SAFE.PARSE_DATE('%Y-%m-%d', order_date) AS order_date,
  order_status,
  shipping_method,
  shipping_address_city,
  shipping_address_state,
  shipping_address_country,
  SAFE_CAST(total_amount AS NUMERIC) AS total_amount,
  SAFE_CAST(discount_amount AS NUMERIC) AS discount_amount,
  SAFE_CAST(tax_amount AS NUMERIC) AS tax_amount,
  currency,
  source_system,
  _ingested_at
FROM `bq-metadata-spike.raw_ecommerce.orders`;


-- ============================================================
-- 4. ADD PRIMARY KEY CONSTRAINTS (NOT ENFORCED)
-- ============================================================

ALTER TABLE `bq-metadata-spike.raw_ecommerce.customers`
ADD PRIMARY KEY (customer_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.raw_ecommerce.products`
ADD PRIMARY KEY (product_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.raw_ecommerce.orders`
ADD PRIMARY KEY (order_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.raw_ecommerce.payments`
ADD PRIMARY KEY (payment_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_customers`
ADD PRIMARY KEY (customer_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_products`
ADD PRIMARY KEY (product_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_orders`
ADD PRIMARY KEY (order_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.analytics_ecommerce.customer_360`
ADD PRIMARY KEY (customer_id) NOT ENFORCED;


-- ============================================================
-- 5. ADD FOREIGN KEY CONSTRAINTS (NOT ENFORCED)
-- ============================================================

ALTER TABLE `bq-metadata-spike.raw_ecommerce.orders`
ADD CONSTRAINT fk_orders_customer
FOREIGN KEY (customer_id) REFERENCES `bq-metadata-spike.raw_ecommerce.customers`(customer_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.raw_ecommerce.order_items`
ADD CONSTRAINT fk_order_items_order
FOREIGN KEY (order_id) REFERENCES `bq-metadata-spike.raw_ecommerce.orders`(order_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.raw_ecommerce.order_items`
ADD CONSTRAINT fk_order_items_product
FOREIGN KEY (product_id) REFERENCES `bq-metadata-spike.raw_ecommerce.products`(product_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.raw_ecommerce.payments`
ADD CONSTRAINT fk_payments_order
FOREIGN KEY (order_id) REFERENCES `bq-metadata-spike.raw_ecommerce.orders`(order_id) NOT ENFORCED;

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_orders`
ADD CONSTRAINT fk_stg_orders_customer
FOREIGN KEY (customer_id) REFERENCES `bq-metadata-spike.staging_ecommerce.stg_customers`(customer_id) NOT ENFORCED;


-- ============================================================
-- 6. CREATE ROW-LEVEL SECURITY POLICIES
-- ============================================================

-- VIP-only policy: non-VIP users only see VIP customers
CREATE ROW ACCESS POLICY vip_only_policy
ON `bq-metadata-spike.analytics_ecommerce.customer_360`
GRANT TO ("allAuthenticatedUsers")
FILTER USING (customer_segment = 'VIP');

-- Full access for service account
CREATE ROW ACCESS POLICY data_team_full_access
ON `bq-metadata-spike.analytics_ecommerce.customer_360`
GRANT TO ("serviceAccount:bq-spike-sa@bq-metadata-spike.iam.gserviceaccount.com")
FILTER USING (TRUE);


-- ============================================================
-- 7. CREATE STORED PROCEDURES AND UDFs (ROUTINES)
-- ============================================================

-- UDF: Calculate customer lifetime value projection
CREATE OR REPLACE FUNCTION `bq-metadata-spike.analytics_ecommerce.calculate_ltv`(
  total_revenue NUMERIC, order_count INT64, months_active INT64
) RETURNS NUMERIC
OPTIONS (
  description = "Calculates projected annual customer lifetime value based on revenue and activity duration."
)
AS (
  CASE
    WHEN months_active > 0 THEN ROUND(total_revenue * (12.0 / months_active), 2)
    ELSE total_revenue
  END
);

-- UDF: Clean and normalize email addresses
CREATE OR REPLACE FUNCTION `bq-metadata-spike.staging_ecommerce.clean_email`(
  email STRING
) RETURNS STRING
OPTIONS (
  description = "Normalizes email addresses by trimming whitespace and converting to lowercase."
)
AS (
  LOWER(TRIM(email))
);

-- UDF: Format currency for display
CREATE OR REPLACE FUNCTION `bq-metadata-spike.analytics_ecommerce.format_currency`(
  amount NUMERIC
) RETURNS STRING
OPTIONS (
  description = "Formats a numeric amount as USD currency string with commas and 2 decimal places."
)
AS (
  CONCAT('$', FORMAT('%,.2f', amount))
);

-- UDF: Calculate SLA status for fulfillment
CREATE OR REPLACE FUNCTION `bq-metadata-spike.reporting_ecommerce.calculate_sla_status`(
  shipped_at TIMESTAMP, delivered_at TIMESTAMP, target_days INT64
) RETURNS STRING
OPTIONS (
  description = "Determines if delivery met SLA target. Returns 'On Time', 'Late', or 'Pending'."
)
AS (
  CASE
    WHEN delivered_at IS NULL THEN 'Pending'
    WHEN TIMESTAMP_DIFF(delivered_at, shipped_at, DAY) <= target_days THEN 'On Time'
    ELSE 'Late'
  END
);

-- Stored Procedure: Refresh customer segments
CREATE OR REPLACE PROCEDURE `bq-metadata-spike.analytics_ecommerce.refresh_customer_segments`()
OPTIONS (
  description = "Recalculates customer segments (VIP, Regular, Occasional) based on latest order counts."
)
BEGIN
  -- Recalculate customer segments based on latest order data
  CREATE OR REPLACE TABLE `bq-metadata-spike.analytics_ecommerce.customer_360`
  OPTIONS(
    description = "Unified customer profile combining CRM data with order behavior metrics. Core entity for segmentation and LTV analysis.",
    labels = [("pii", "true"), ("owner", "analytics-team"), ("domain", "customer"), ("layer", "analytics"), ("certified", "true")],
    friendly_name = "Customer 360 Profile"
  ) AS
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state,
    c.country,
    c.created_at AS account_created_at,
    DATE_DIFF(CURRENT_DATE(), DATE(c.created_at), DAY) AS account_age_days,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_revenue,
    COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
    MAX(o.order_date) AS last_order_date,
    MIN(o.order_date) AS first_order_date,
    COUNTIF(o.order_status = 'completed') AS completed_orders,
    COUNTIF(o.order_status = 'returned') AS returned_orders,
    COUNTIF(o.order_status = 'cancelled') AS cancelled_orders,
    CASE
      WHEN COUNT(DISTINCT o.order_id) >= 5 THEN 'VIP'
      WHEN COUNT(DISTINCT o.order_id) >= 3 THEN 'Regular'
      WHEN COUNT(DISTINCT o.order_id) >= 1 THEN 'Occasional'
      ELSE 'Prospect'
    END AS customer_segment
  FROM `bq-metadata-spike.staging_ecommerce.stg_customers` c
  LEFT JOIN `bq-metadata-spike.staging_ecommerce.stg_orders` o ON c.customer_id = o.customer_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;
END;


-- ============================================================
-- 8. CREATE MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW `bq-metadata-spike.reporting_ecommerce.mv_daily_revenue_summary`
OPTIONS (
  description = "Materialized: daily revenue with auto-refresh. Faster than querying revenue_daily directly.",
  friendly_name = "Daily Revenue Summary (Materialized)",
  labels = [("owner", "analytics-team"), ("layer", "reporting"), ("certified", "true")],
  enable_refresh = true,
  refresh_interval_minutes = 60
)
AS
SELECT
  order_date,
  gross_revenue AS total_revenue,
  order_count,
  unique_customers,
  net_revenue,
  avg_order_value
FROM `bq-metadata-spike.analytics_ecommerce.revenue_daily`;


-- ============================================================
-- 9. CREATE TABLE SNAPSHOT
-- ============================================================

CREATE SNAPSHOT TABLE `bq-metadata-spike.analytics_ecommerce.customer_360_snapshot_20260205`
CLONE `bq-metadata-spike.analytics_ecommerce.customer_360`
OPTIONS (
  description = "Point-in-time snapshot of customer_360 for Feb 2026 audit",
  friendly_name = "Customer 360 Audit Snapshot (Feb 2026)",
  labels = [("purpose", "audit"), ("layer", "analytics")]
);


-- ============================================================
-- 10. CREATE SEARCH INDEX
-- ============================================================

CREATE SEARCH INDEX customer_search_idx
ON `bq-metadata-spike.raw_ecommerce.customers`(ALL COLUMNS)
OPTIONS (analyzer = 'LOG_ANALYZER');


-- ============================================================
-- 11. ENSURE ALL LABELS ARE COMPREHENSIVE
-- ============================================================

-- Raw tables: ensure all have owner, domain, layer, certified, pii, source labels
ALTER TABLE `bq-metadata-spike.raw_ecommerce.customers`
SET OPTIONS (
  labels = [("owner", "data-engineering"), ("domain", "ecommerce"), ("layer", "raw"), ("certified", "false"), ("pii", "true"), ("source", "crm")]
);

ALTER TABLE `bq-metadata-spike.raw_ecommerce.products`
SET OPTIONS (
  labels = [("owner", "data-engineering"), ("domain", "ecommerce"), ("layer", "raw"), ("certified", "false"), ("pii", "false"), ("source", "shopify")]
);

ALTER TABLE `bq-metadata-spike.raw_ecommerce.orders`
SET OPTIONS (
  labels = [("owner", "data-engineering"), ("domain", "ecommerce"), ("layer", "raw"), ("certified", "false"), ("pii", "false"), ("source", "oms")]
);

ALTER TABLE `bq-metadata-spike.raw_ecommerce.order_items`
SET OPTIONS (
  labels = [("owner", "data-engineering"), ("domain", "ecommerce"), ("layer", "raw"), ("certified", "false"), ("pii", "false"), ("source", "oms")]
);

ALTER TABLE `bq-metadata-spike.raw_ecommerce.payments`
SET OPTIONS (
  labels = [("owner", "finance-engineering"), ("domain", "ecommerce"), ("layer", "raw"), ("certified", "false"), ("pii", "true"), ("source", "stripe")]
);

ALTER TABLE `bq-metadata-spike.raw_ecommerce.shipping_events`
SET OPTIONS (
  labels = [("owner", "ops-engineering"), ("domain", "ecommerce"), ("layer", "raw"), ("certified", "false"), ("pii", "false"), ("source", "logistics")]
);

-- Staging tables: owner, domain, layer, certified, pii
ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_customers`
SET OPTIONS (
  labels = [("owner", "data-engineering"), ("domain", "ecommerce"), ("layer", "staging"), ("certified", "true"), ("pii", "true")]
);

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_products`
SET OPTIONS (
  labels = [("owner", "data-engineering"), ("domain", "ecommerce"), ("layer", "staging"), ("certified", "true"), ("pii", "false")]
);

-- stg_orders already set via CREATE OR REPLACE above

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_order_items`
SET OPTIONS (
  labels = [("owner", "data-engineering"), ("domain", "ecommerce"), ("layer", "staging"), ("certified", "true"), ("pii", "false")]
);

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_payments`
SET OPTIONS (
  labels = [("owner", "finance-engineering"), ("domain", "ecommerce"), ("layer", "staging"), ("certified", "true"), ("pii", "true")]
);

ALTER TABLE `bq-metadata-spike.staging_ecommerce.stg_shipping_events`
SET OPTIONS (
  labels = [("owner", "ops-engineering"), ("domain", "ecommerce"), ("layer", "staging"), ("certified", "true"), ("pii", "false")]
);

-- Analytics tables: owner, domain, layer, certified, pii
ALTER TABLE `bq-metadata-spike.analytics_ecommerce.customer_360`
SET OPTIONS (
  labels = [("owner", "analytics-team"), ("domain", "ecommerce"), ("layer", "analytics"), ("certified", "true"), ("pii", "true")]
);

ALTER TABLE `bq-metadata-spike.analytics_ecommerce.product_performance`
SET OPTIONS (
  labels = [("owner", "analytics-team"), ("domain", "ecommerce"), ("layer", "analytics"), ("certified", "true"), ("pii", "false")]
);

-- revenue_daily and order_fulfillment already set via CREATE OR REPLACE above

-- ============================================================
-- END OF ENRICHMENT SCRIPT
-- ============================================================
