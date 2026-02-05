-- ============================================================
-- Layer: Reporting
-- Purpose: BI-ready views optimized for dashboards (Looker, Tableau, Power BI)
-- Run: bq query --use_legacy_sql=false < sql/05-reporting-layer.sql
-- ============================================================

-- Executive KPI view
CREATE OR REPLACE VIEW `bq-metadata-spike.reporting_ecommerce.v_executive_kpis`
OPTIONS(
  description="Executive-level KPIs for C-suite dashboard. Refreshed from analytics tables.",
  labels=[("owner", "bi-team"), ("dashboard", "executive"), ("certified", "true")]
) AS
SELECT
  'Total Revenue' AS kpi_name,
  CAST(SUM(net_revenue) AS STRING) AS kpi_value,
  'USD' AS unit
FROM `bq-metadata-spike.analytics_ecommerce.revenue_daily`
UNION ALL
SELECT
  'Total Orders',
  CAST(SUM(order_count) AS STRING),
  'count'
FROM `bq-metadata-spike.analytics_ecommerce.revenue_daily`
UNION ALL
SELECT
  'Avg Order Value',
  CAST(ROUND(AVG(avg_order_value), 2) AS STRING),
  'USD'
FROM `bq-metadata-spike.analytics_ecommerce.revenue_daily`
UNION ALL
SELECT
  'VIP Customers',
  CAST(COUNTIF(customer_segment = 'VIP') AS STRING),
  'count'
FROM `bq-metadata-spike.analytics_ecommerce.customer_360`;


-- Customer segmentation view for marketing dashboard
CREATE OR REPLACE VIEW `bq-metadata-spike.reporting_ecommerce.v_customer_segments`
OPTIONS(
  description="Customer segmentation breakdown for marketing team. Segments: VIP, Regular, Occasional, Prospect.",
  labels=[("owner", "bi-team"), ("dashboard", "marketing"), ("certified", "true")]
) AS
SELECT
  customer_segment,
  COUNT(*) AS customer_count,
  ROUND(AVG(lifetime_revenue), 2) AS avg_lifetime_revenue,
  ROUND(AVG(avg_order_value), 2) AS avg_order_value,
  ROUND(AVG(total_orders), 1) AS avg_orders_per_customer,
  SUM(returned_orders) AS total_returns
FROM `bq-metadata-spike.analytics_ecommerce.customer_360`
GROUP BY 1;


-- Product category performance for merchandising
CREATE OR REPLACE VIEW `bq-metadata-spike.reporting_ecommerce.v_category_performance`
OPTIONS(
  description="Category and subcategory performance for merchandising dashboard.",
  labels=[("owner", "bi-team"), ("dashboard", "merchandising")]
) AS
SELECT
  category,
  subcategory,
  COUNT(*) AS product_count,
  SUM(total_units_sold) AS units_sold,
  SUM(total_revenue) AS revenue,
  ROUND(AVG(margin_pct) * 100, 1) AS avg_margin_pct,
  SUM(unique_buyers) AS total_buyers
FROM `bq-metadata-spike.analytics_ecommerce.product_performance`
GROUP BY 1, 2;


-- Fulfillment SLA tracking
CREATE OR REPLACE VIEW `bq-metadata-spike.reporting_ecommerce.v_fulfillment_sla`
OPTIONS(
  description="Fulfillment SLA metrics for operations dashboard. Shows delivery speed by shipping method.",
  labels=[("owner", "bi-team"), ("dashboard", "operations")]
) AS
SELECT
  shipping_method,
  order_status,
  COUNT(*) AS order_count,
  ROUND(AVG(delivery_hours), 1) AS avg_delivery_hours,
  COUNTIF(delivery_hours <= 24) AS delivered_within_24h,
  COUNTIF(payment_status = 'refunded') AS refunded_count
FROM `bq-metadata-spike.analytics_ecommerce.order_fulfillment`
GROUP BY 1, 2;
