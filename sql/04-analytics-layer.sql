-- ============================================================
-- Layer: Analytics
-- Purpose: Business logic layer with joins, aggregations, and derived metrics
-- Run: bq query --use_legacy_sql=false < sql/04-analytics-layer.sql
-- ============================================================

-- Customer 360 — unified customer profile with order metrics
CREATE OR REPLACE TABLE `bq-metadata-spike.analytics_ecommerce.customer_360`
OPTIONS(
  description="Unified customer profile combining CRM data with order behavior metrics. Core entity for segmentation and LTV analysis.",
  labels=[("pii", "true"), ("owner", "analytics-team"), ("domain", "customer")]
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
GROUP BY 1,2,3,4,5,6,7,8,9;


-- Product Performance — sales metrics per product
CREATE OR REPLACE TABLE `bq-metadata-spike.analytics_ecommerce.product_performance`
OPTIONS(
  description="Product-level performance metrics: revenue, units sold, margin, return rate. Updated daily.",
  labels=[("owner", "analytics-team"), ("domain", "product")]
) AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  p.unit_price AS current_price,
  p.cost_price,
  p.is_active,
  COALESCE(SUM(oi.quantity), 0) AS total_units_sold,
  COALESCE(SUM(oi.subtotal), 0) AS total_revenue,
  COALESCE(SUM(oi.subtotal) - SUM(oi.quantity * p.cost_price), 0) AS gross_margin,
  SAFE_DIVIDE(SUM(oi.subtotal) - SUM(oi.quantity * p.cost_price), SUM(oi.subtotal)) AS margin_pct,
  COUNT(DISTINCT oi.order_id) AS order_count,
  COUNT(DISTINCT o.customer_id) AS unique_buyers
FROM `bq-metadata-spike.staging_ecommerce.stg_products` p
LEFT JOIN `bq-metadata-spike.staging_ecommerce.stg_order_items` oi ON p.product_id = oi.product_id
LEFT JOIN `bq-metadata-spike.staging_ecommerce.stg_orders` o ON oi.order_id = o.order_id
GROUP BY 1,2,3,4,5,6,7,8;


-- Revenue by period — time-series revenue analysis
CREATE OR REPLACE TABLE `bq-metadata-spike.analytics_ecommerce.revenue_daily`
OPTIONS(
  description="Daily revenue aggregation with order counts and average order value. Source of truth for revenue reporting.",
  labels=[("owner", "finance-analytics"), ("domain", "finance")]
) AS
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
  AVG(o.total_amount) AS avg_order_value
FROM `bq-metadata-spike.staging_ecommerce.stg_orders` o
WHERE o.order_status IN ('completed', 'shipped')
GROUP BY 1,2,3,4;


-- Order fulfillment funnel
CREATE OR REPLACE TABLE `bq-metadata-spike.analytics_ecommerce.order_fulfillment`
OPTIONS(
  description="Order-level fulfillment tracking: combines order, payment, and shipping data for ops metrics.",
  labels=[("owner", "ops-analytics"), ("domain", "operations")]
) AS
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
GROUP BY 1,2,3,4,5,6,7,8,9;
