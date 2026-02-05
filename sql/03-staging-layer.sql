-- ============================================================
-- Layer: Staging
-- Purpose: Clean and type-cast raw data with deduplication and null handling
-- Run: bq query --use_legacy_sql=false < sql/03-staging-layer.sql
-- ============================================================

CREATE OR REPLACE TABLE `bq-metadata-spike.staging_ecommerce.stg_customers`
OPTIONS(
  description="Cleaned customer records. Deduplicated by customer_id (latest ingestion wins). Dates parsed, PII fields preserved for downstream masking.",
  labels=[("pii", "true"), ("owner", "data-engineering"), ("certified", "true")]
) AS
SELECT
  customer_id,
  INITCAP(first_name) AS first_name,
  INITCAP(last_name) AS last_name,
  LOWER(email) AS email,
  phone,
  SAFE.PARSE_DATE('%Y-%m-%d', date_of_birth) AS date_of_birth,
  gender,
  address_line1,
  city,
  state,
  country,
  postal_code,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', created_at) AS created_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', updated_at) AS updated_at,
  source_system,
  _ingested_at
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _ingested_at DESC) AS rn
  FROM `bq-metadata-spike.raw_ecommerce.customers`
)
WHERE rn = 1;


CREATE OR REPLACE TABLE `bq-metadata-spike.staging_ecommerce.stg_products`
OPTIONS(
  description="Cleaned product catalog. Prices cast to NUMERIC. Active flag as BOOL.",
  labels=[("owner", "data-engineering"), ("certified", "true")]
) AS
SELECT
  product_id,
  product_name,
  category,
  subcategory,
  brand,
  SAFE_CAST(unit_price AS NUMERIC) AS unit_price,
  SAFE_CAST(cost_price AS NUMERIC) AS cost_price,
  SAFE_CAST(weight_kg AS NUMERIC) AS weight_kg,
  CASE LOWER(is_active) WHEN 'true' THEN TRUE ELSE FALSE END AS is_active,
  SAFE.PARSE_DATE('%Y-%m-%d', created_at) AS created_at,
  source_system
FROM `bq-metadata-spike.raw_ecommerce.products`;


CREATE OR REPLACE TABLE `bq-metadata-spike.staging_ecommerce.stg_orders`
OPTIONS(
  description="Cleaned orders. Amounts cast to NUMERIC. Dates parsed.",
  labels=[("owner", "data-engineering"), ("certified", "true")]
) AS
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


CREATE OR REPLACE TABLE `bq-metadata-spike.staging_ecommerce.stg_order_items`
OPTIONS(
  description="Cleaned order line items. Quantities as INT64, prices as NUMERIC.",
  labels=[("owner", "data-engineering"), ("certified", "true")]
) AS
SELECT
  order_item_id,
  order_id,
  product_id,
  SAFE_CAST(quantity AS INT64) AS quantity,
  SAFE_CAST(unit_price AS NUMERIC) AS unit_price,
  SAFE_CAST(subtotal AS NUMERIC) AS subtotal,
  _ingested_at
FROM `bq-metadata-spike.raw_ecommerce.order_items`;


CREATE OR REPLACE TABLE `bq-metadata-spike.staging_ecommerce.stg_payments`
OPTIONS(
  description="Cleaned payment transactions. Amounts as NUMERIC. Timestamps parsed. PII: partial card number.",
  labels=[("pii", "true"), ("owner", "finance-engineering"), ("certified", "true")]
) AS
SELECT
  payment_id,
  order_id,
  payment_method,
  payment_status,
  SAFE_CAST(amount AS NUMERIC) AS amount,
  currency,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', processed_at) AS processed_at,
  gateway_reference,
  card_last_four,
  source_system,
  _ingested_at
FROM `bq-metadata-spike.raw_ecommerce.payments`;


CREATE OR REPLACE TABLE `bq-metadata-spike.staging_ecommerce.stg_shipping_events`
OPTIONS(
  description="Cleaned shipping events. Timestamps parsed.",
  labels=[("owner", "ops-engineering")]
) AS
SELECT
  event_id,
  order_id,
  carrier,
  tracking_number,
  event_type,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', event_timestamp) AS event_timestamp,
  location,
  source_system,
  _ingested_at
FROM `bq-metadata-spike.raw_ecommerce.shipping_events`;
