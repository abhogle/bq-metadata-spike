# BigQuery Metadata Spike — Complete Setup Guide

> Prototype: Simulating a typical customer's BigQuery environment, extracting all metadata, and visualizing it.

## Prerequisites

1. A Google account (Gmail works)
2. `gcloud` CLI installed ([install guide](https://cloud.google.com/sdk/docs/install))
3. Python 3.10+ with `pip`

---

## Part 1: GCP Project Setup (From Scratch)

### 1.1 Create a GCP Project

```bash
# Authenticate
gcloud auth login

# Create project (pick a unique ID)
gcloud projects create bq-metadata-spike --name="BQ Metadata Spike"

# Set as active project
gcloud config set project bq-metadata-spike

# Enable billing (required for BigQuery — free tier covers this spike)
# Go to: https://console.cloud.google.com/billing
# Link project "bq-metadata-spike" to a billing account
# NOTE: BigQuery free tier = 1TB query/month + 10GB storage/month — this spike uses ~0.001% of that

# Enable BigQuery API
gcloud services enable bigquery.googleapis.com

# Enable BigQuery Data Transfer API (for audit log access)
gcloud services enable bigquerydatatransfer.googleapis.com
```

### 1.2 Create a Service Account (for Python scripts)

```bash
# Create service account
gcloud iam service-accounts create bq-spike-sa \
  --display-name="BQ Spike Service Account"

# Grant BigQuery Admin role (for this spike only — not for production)
gcloud projects add-iam-policy-binding bq-metadata-spike \
  --member="serviceAccount:bq-spike-sa@bq-metadata-spike.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"

# Generate key file
gcloud iam service-accounts keys create ~/bq-spike-sa-key.json \
  --iam-account=bq-spike-sa@bq-metadata-spike.iam.gserviceaccount.com

# Set environment variable for Python client
export GOOGLE_APPLICATION_CREDENTIALS=~/bq-spike-sa-key.json
```

---

## Part 2: Seed BigQuery with Realistic E-Commerce Data

We create a **4-layer architecture** that mirrors real customer setups:

```
raw_ecommerce          → Raw ingested data (as-is from source systems)
staging_ecommerce      → Cleaned & typed data
analytics_ecommerce    → Business logic, aggregations, derived metrics
reporting_ecommerce    → BI-ready views for dashboards
```

### 2.1 Create Datasets

Run this in the [BigQuery Console](https://console.cloud.google.com/bigquery) or via `bq` CLI:

```sql
-- Run each statement separately in BigQuery Console, or use bq CLI below
```

```bash
bq mk --dataset --description "Raw ingested e-commerce data from source systems. No transformations applied." \
  --label environment:spike --label layer:raw \
  bq-metadata-spike:raw_ecommerce

bq mk --dataset --description "Cleaned and typed e-commerce data. Deduplication, type casting, null handling applied." \
  --label environment:spike --label layer:staging \
  bq-metadata-spike:staging_ecommerce

bq mk --dataset --description "Business logic layer. Aggregations, derived metrics, joins across domains." \
  --label environment:spike --label layer:analytics \
  bq-metadata-spike:analytics_ecommerce

bq mk --dataset --description "BI-ready views for Looker/Tableau dashboards. Optimized for reporting queries." \
  --label environment:spike --label layer:reporting \
  bq-metadata-spike:reporting_ecommerce
```

### 2.2 Create & Populate Raw Layer Tables

```sql
-- ============================================================
-- RAW LAYER: raw_ecommerce
-- Simulates data as it arrives from source systems
-- ============================================================

-- Customers (from CRM system)
CREATE OR REPLACE TABLE `bq-metadata-spike.raw_ecommerce.customers` (
  customer_id STRING OPTIONS(description="Unique customer identifier from CRM"),
  first_name STRING OPTIONS(description="Customer first name"),
  last_name STRING OPTIONS(description="Customer last name"),
  email STRING OPTIONS(description="Customer email address — PII"),
  phone STRING OPTIONS(description="Customer phone number — PII"),
  date_of_birth STRING OPTIONS(description="Date of birth as string from source — PII"),
  gender STRING OPTIONS(description="Customer gender"),
  address_line1 STRING OPTIONS(description="Street address — PII"),
  city STRING OPTIONS(description="City"),
  state STRING OPTIONS(description="State/province code"),
  country STRING OPTIONS(description="ISO country code"),
  postal_code STRING OPTIONS(description="Postal/ZIP code — PII"),
  created_at STRING OPTIONS(description="Account creation timestamp from CRM"),
  updated_at STRING OPTIONS(description="Last update timestamp from CRM"),
  source_system STRING OPTIONS(description="Originating system identifier"),
  _ingested_at TIMESTAMP OPTIONS(description="Timestamp when row was ingested into BigQuery")
)
OPTIONS(
  description="Raw customer records ingested from CRM. Contains PII fields. No dedup or cleaning applied.",
  labels=[("pii", "true"), ("source", "crm"), ("owner", "data-engineering")]
);

INSERT INTO `bq-metadata-spike.raw_ecommerce.customers` VALUES
('C001','Alice','Johnson','alice.johnson@email.com','+1-555-0101','1990-03-15','F','123 Oak Street','San Francisco','CA','US','94102','2023-01-15T10:30:00','2024-06-01T14:22:00','salesforce','2024-01-01 00:00:00 UTC'),
('C002','Bob','Smith','bob.smith@email.com','+1-555-0102','1985-07-22','M','456 Pine Ave','New York','NY','US','10001','2023-02-20T09:15:00','2024-05-15T11:30:00','salesforce','2024-01-01 00:00:00 UTC'),
('C003','Carol','Williams','carol.w@email.com','+1-555-0103','1992-11-08','F','789 Elm Blvd','Chicago','IL','US','60601','2023-03-10T16:45:00','2024-07-20T09:10:00','salesforce','2024-01-01 00:00:00 UTC'),
('C004','David','Brown','david.brown@email.com','+1-555-0104','1988-01-30','M','321 Maple Dr','Austin','TX','US','73301','2023-04-05T08:00:00','2024-04-10T15:45:00','salesforce','2024-01-01 00:00:00 UTC'),
('C005','Eve','Davis','eve.davis@email.com','+1-555-0105','1995-09-12','F','654 Cedar Ln','Seattle','WA','US','98101','2023-05-18T12:30:00','2024-08-01T10:00:00','salesforce','2024-01-01 00:00:00 UTC'),
('C006','Frank','Miller','frank.m@email.com','+1-555-0106','1980-12-25','M','987 Birch Ct','Denver','CO','US','80201','2023-06-22T14:00:00','2024-03-15T08:30:00','salesforce','2024-01-01 00:00:00 UTC'),
('C007','Grace','Wilson','grace.wilson@email.com','+1-555-0107','1993-06-18','F','147 Walnut St','Portland','OR','US','97201','2023-07-30T11:15:00','2024-09-01T16:20:00','salesforce','2024-01-01 00:00:00 UTC'),
('C008','Henry','Taylor','henry.t@email.com','+1-555-0108','1987-04-03','M','258 Spruce Way','Miami','FL','US','33101','2023-08-14T09:45:00','2024-07-10T12:00:00','salesforce','2024-01-01 00:00:00 UTC'),
('C002','Bob','Smith','bob.smith@email.com','+1-555-0102','1985-07-22','M','456 Pine Ave','New York','NY','US','10001','2023-02-20T09:15:00','2024-05-15T11:30:00','salesforce','2024-01-02 00:00:00 UTC'),
('C009','Ivy','Anderson','ivy.a@email.com','+1-555-0109','1991-08-20','F','369 Ash Pl','Boston','MA','US','02101','2023-09-01T13:00:00','2024-06-25T14:15:00','salesforce','2024-01-01 00:00:00 UTC'),
('C010','Jack','Thomas','jack.thomas@email.com','+1-555-0110','1986-02-14','M','741 Poplar Rd','Nashville','TN','US','37201','2023-10-10T10:30:00','2024-08-15T11:45:00','salesforce','2024-01-01 00:00:00 UTC');


-- Products (from product catalog system)
CREATE OR REPLACE TABLE `bq-metadata-spike.raw_ecommerce.products` (
  product_id STRING OPTIONS(description="Unique product SKU"),
  product_name STRING OPTIONS(description="Product display name"),
  category STRING OPTIONS(description="Product category"),
  subcategory STRING OPTIONS(description="Product subcategory"),
  brand STRING OPTIONS(description="Brand name"),
  unit_price STRING OPTIONS(description="Price as string from source system"),
  cost_price STRING OPTIONS(description="Cost/wholesale price as string"),
  weight_kg STRING OPTIONS(description="Product weight in kg"),
  is_active STRING OPTIONS(description="Whether product is currently sold — 'true'/'false' string"),
  created_at STRING OPTIONS(description="Product creation date in catalog"),
  source_system STRING OPTIONS(description="Originating system"),
  _ingested_at TIMESTAMP OPTIONS(description="Ingestion timestamp")
)
OPTIONS(
  description="Raw product catalog data. Prices as strings, active flag as string. No cleaning.",
  labels=[("source", "catalog"), ("owner", "product-team")]
);

INSERT INTO `bq-metadata-spike.raw_ecommerce.products` VALUES
('P001','Wireless Mouse','Electronics','Peripherals','TechBrand','29.99','12.50','0.15','true','2023-01-01','shopify','2024-01-01 00:00:00 UTC'),
('P002','Mechanical Keyboard','Electronics','Peripherals','KeyCo','89.99','35.00','0.85','true','2023-01-01','shopify','2024-01-01 00:00:00 UTC'),
('P003','USB-C Hub','Electronics','Accessories','TechBrand','49.99','20.00','0.12','true','2023-02-15','shopify','2024-01-01 00:00:00 UTC'),
('P004','Laptop Stand','Office','Furniture','ErgoDesk','39.99','15.00','1.20','true','2023-03-01','shopify','2024-01-01 00:00:00 UTC'),
('P005','Noise Cancelling Headphones','Electronics','Audio','SoundMax','199.99','80.00','0.30','true','2023-01-01','shopify','2024-01-01 00:00:00 UTC'),
('P006','Webcam HD','Electronics','Peripherals','TechBrand','79.99','30.00','0.20','false','2023-01-01','shopify','2024-01-01 00:00:00 UTC'),
('P007','Desk Lamp','Office','Lighting','BrightCo','34.99','14.00','0.90','true','2023-04-10','shopify','2024-01-01 00:00:00 UTC'),
('P008','Monitor 27"','Electronics','Displays','ViewPro','349.99','150.00','5.50','true','2023-01-01','shopify','2024-01-01 00:00:00 UTC'),
('P009','Ergonomic Chair','Office','Furniture','ErgoDesk','499.99','200.00','15.00','true','2023-05-20','shopify','2024-01-01 00:00:00 UTC'),
('P010','Portable Charger','Electronics','Accessories','PowerUp','24.99','8.00','0.25','true','2023-06-01','shopify','2024-01-01 00:00:00 UTC'),
('P011','Notebook Set','Office','Supplies','PaperCraft','12.99','4.00','0.40','true','2023-07-15','shopify','2024-01-01 00:00:00 UTC'),
('P012','Wireless Earbuds','Electronics','Audio','SoundMax','129.99','50.00','0.08','true','2023-01-01','shopify','2024-01-01 00:00:00 UTC');


-- Orders (from order management system)
CREATE OR REPLACE TABLE `bq-metadata-spike.raw_ecommerce.orders` (
  order_id STRING OPTIONS(description="Unique order identifier"),
  customer_id STRING OPTIONS(description="FK to customers"),
  order_date STRING OPTIONS(description="Order placement date as string"),
  order_status STRING OPTIONS(description="Current order status"),
  shipping_method STRING OPTIONS(description="Shipping tier selected"),
  shipping_address_city STRING OPTIONS(description="Ship-to city"),
  shipping_address_state STRING OPTIONS(description="Ship-to state"),
  shipping_address_country STRING OPTIONS(description="Ship-to country"),
  total_amount STRING OPTIONS(description="Order total as string from source"),
  discount_amount STRING OPTIONS(description="Discount applied as string"),
  tax_amount STRING OPTIONS(description="Tax charged as string"),
  currency STRING OPTIONS(description="Currency code"),
  source_system STRING OPTIONS(description="Originating system"),
  _ingested_at TIMESTAMP OPTIONS(description="Ingestion timestamp")
)
OPTIONS(
  description="Raw order records from OMS. Amounts as strings. Includes all statuses.",
  labels=[("source", "oms"), ("owner", "data-engineering")]
);

INSERT INTO `bq-metadata-spike.raw_ecommerce.orders` VALUES
('ORD-1001','C001','2024-01-15','completed','express','San Francisco','CA','US','129.97','10.00','10.40','USD','shopify','2024-01-16 00:00:00 UTC'),
('ORD-1002','C002','2024-01-20','completed','standard','New York','NY','US','89.99','0.00','7.20','USD','shopify','2024-01-21 00:00:00 UTC'),
('ORD-1003','C003','2024-02-01','completed','express','Chicago','IL','US','249.98','25.00','18.00','USD','shopify','2024-02-02 00:00:00 UTC'),
('ORD-1004','C001','2024-02-14','completed','standard','San Francisco','CA','US','34.99','0.00','2.80','USD','shopify','2024-02-15 00:00:00 UTC'),
('ORD-1005','C004','2024-02-28','shipped','express','Austin','TX','US','549.98','50.00','40.00','USD','shopify','2024-03-01 00:00:00 UTC'),
('ORD-1006','C005','2024-03-10','completed','standard','Seattle','WA','US','199.99','20.00','14.40','USD','shopify','2024-03-11 00:00:00 UTC'),
('ORD-1007','C003','2024-03-15','cancelled','express','Chicago','IL','US','79.99','0.00','6.40','USD','shopify','2024-03-16 00:00:00 UTC'),
('ORD-1008','C006','2024-03-22','completed','standard','Denver','CO','US','64.98','5.00','4.80','USD','shopify','2024-03-23 00:00:00 UTC'),
('ORD-1009','C007','2024-04-01','completed','express','Portland','OR','US','349.99','30.00','25.60','USD','shopify','2024-04-02 00:00:00 UTC'),
('ORD-1010','C008','2024-04-10','pending','standard','Miami','FL','US','154.98','15.00','11.20','USD','shopify','2024-04-11 00:00:00 UTC'),
('ORD-1011','C002','2024-04-20','completed','express','New York','NY','US','629.98','60.00','45.60','USD','shopify','2024-04-21 00:00:00 UTC'),
('ORD-1012','C009','2024-05-01','completed','standard','Boston','MA','US','24.99','0.00','2.00','USD','shopify','2024-05-02 00:00:00 UTC'),
('ORD-1013','C010','2024-05-15','returned','express','Nashville','TN','US','129.99','10.00','9.60','USD','shopify','2024-05-16 00:00:00 UTC'),
('ORD-1014','C001','2024-06-01','completed','standard','San Francisco','CA','US','89.99','0.00','7.20','USD','shopify','2024-06-02 00:00:00 UTC'),
('ORD-1015','C005','2024-06-15','completed','express','Seattle','WA','US','499.99','40.00','36.80','USD','shopify','2024-06-16 00:00:00 UTC');


-- Order Items (line items)
CREATE OR REPLACE TABLE `bq-metadata-spike.raw_ecommerce.order_items` (
  order_item_id STRING OPTIONS(description="Unique line item ID"),
  order_id STRING OPTIONS(description="FK to orders"),
  product_id STRING OPTIONS(description="FK to products"),
  quantity STRING OPTIONS(description="Quantity as string from source"),
  unit_price STRING OPTIONS(description="Price at time of purchase as string"),
  subtotal STRING OPTIONS(description="Line item subtotal as string"),
  _ingested_at TIMESTAMP OPTIONS(description="Ingestion timestamp")
)
OPTIONS(
  description="Raw order line items. Quantities and prices as strings.",
  labels=[("source", "oms"), ("owner", "data-engineering")]
);

INSERT INTO `bq-metadata-spike.raw_ecommerce.order_items` VALUES
('OI-1','ORD-1001','P001','1','29.99','29.99','2024-01-16 00:00:00 UTC'),
('OI-2','ORD-1001','P003','2','49.99','99.98','2024-01-16 00:00:00 UTC'),
('OI-3','ORD-1002','P002','1','89.99','89.99','2024-01-21 00:00:00 UTC'),
('OI-4','ORD-1003','P005','1','199.99','199.99','2024-02-02 00:00:00 UTC'),
('OI-5','ORD-1003','P003','1','49.99','49.99','2024-02-02 00:00:00 UTC'),
('OI-6','ORD-1004','P007','1','34.99','34.99','2024-02-15 00:00:00 UTC'),
('OI-7','ORD-1005','P009','1','499.99','499.99','2024-03-01 00:00:00 UTC'),
('OI-8','ORD-1005','P003','1','49.99','49.99','2024-03-01 00:00:00 UTC'),
('OI-9','ORD-1006','P005','1','199.99','199.99','2024-03-11 00:00:00 UTC'),
('OI-10','ORD-1007','P006','1','79.99','79.99','2024-03-16 00:00:00 UTC'),
('OI-11','ORD-1008','P001','1','29.99','29.99','2024-03-23 00:00:00 UTC'),
('OI-12','ORD-1008','P007','1','34.99','34.99','2024-03-23 00:00:00 UTC'),
('OI-13','ORD-1009','P008','1','349.99','349.99','2024-04-02 00:00:00 UTC'),
('OI-14','ORD-1010','P010','2','24.99','49.98','2024-04-11 00:00:00 UTC'),
('OI-15','ORD-1010','P002','1','89.99','89.99','2024-04-11 00:00:00 UTC'),
('OI-16','ORD-1010','P011','1','14.99','14.99','2024-04-11 00:00:00 UTC'),
('OI-17','ORD-1011','P008','1','349.99','349.99','2024-04-21 00:00:00 UTC'),
('OI-18','ORD-1011','P002','1','89.99','89.99','2024-04-21 00:00:00 UTC'),
('OI-19','ORD-1011','P005','1','199.99','199.99','2024-04-21 00:00:00 UTC'),
('OI-20','ORD-1012','P010','1','24.99','24.99','2024-05-02 00:00:00 UTC'),
('OI-21','ORD-1013','P012','1','129.99','129.99','2024-05-16 00:00:00 UTC'),
('OI-22','ORD-1014','P002','1','89.99','89.99','2024-06-02 00:00:00 UTC'),
('OI-23','ORD-1015','P009','1','499.99','499.99','2024-06-16 00:00:00 UTC');


-- Payments (from payment gateway)
CREATE OR REPLACE TABLE `bq-metadata-spike.raw_ecommerce.payments` (
  payment_id STRING OPTIONS(description="Unique payment transaction ID"),
  order_id STRING OPTIONS(description="FK to orders"),
  payment_method STRING OPTIONS(description="Payment method used"),
  payment_status STRING OPTIONS(description="Transaction status"),
  amount STRING OPTIONS(description="Payment amount as string"),
  currency STRING OPTIONS(description="Currency code"),
  processed_at STRING OPTIONS(description="Processing timestamp as string"),
  gateway_reference STRING OPTIONS(description="External payment gateway reference ID"),
  card_last_four STRING OPTIONS(description="Last 4 digits of card — PII"),
  source_system STRING OPTIONS(description="Originating system"),
  _ingested_at TIMESTAMP OPTIONS(description="Ingestion timestamp")
)
OPTIONS(
  description="Raw payment transactions from Stripe gateway. Contains partial card info (PII).",
  labels=[("pii", "true"), ("source", "stripe"), ("owner", "finance-engineering")]
);

INSERT INTO `bq-metadata-spike.raw_ecommerce.payments` VALUES
('PAY-001','ORD-1001','credit_card','success','130.37','USD','2024-01-15T10:32:00','ch_abc123','4242','stripe','2024-01-16 00:00:00 UTC'),
('PAY-002','ORD-1002','credit_card','success','97.19','USD','2024-01-20T14:05:00','ch_def456','1234','stripe','2024-01-21 00:00:00 UTC'),
('PAY-003','ORD-1003','paypal','success','242.98','USD','2024-02-01T09:20:00','pp_ghi789',NULL,'stripe','2024-02-02 00:00:00 UTC'),
('PAY-004','ORD-1004','credit_card','success','37.79','USD','2024-02-14T11:00:00','ch_jkl012','5678','stripe','2024-02-15 00:00:00 UTC'),
('PAY-005','ORD-1005','credit_card','success','539.98','USD','2024-02-28T16:30:00','ch_mno345','9012','stripe','2024-03-01 00:00:00 UTC'),
('PAY-006','ORD-1006','credit_card','success','194.39','USD','2024-03-10T08:45:00','ch_pqr678','3456','stripe','2024-03-11 00:00:00 UTC'),
('PAY-007','ORD-1007','credit_card','refunded','86.39','USD','2024-03-15T13:15:00','ch_stu901','7890','stripe','2024-03-16 00:00:00 UTC'),
('PAY-008','ORD-1008','debit_card','success','64.78','USD','2024-03-22T10:00:00','ch_vwx234','2345','stripe','2024-03-23 00:00:00 UTC'),
('PAY-009','ORD-1009','credit_card','success','345.59','USD','2024-04-01T15:20:00','ch_yza567','6789','stripe','2024-04-02 00:00:00 UTC'),
('PAY-010','ORD-1010','paypal','pending','150.98','USD','2024-04-10T09:30:00','pp_bcd890',NULL,'stripe','2024-04-11 00:00:00 UTC'),
('PAY-011','ORD-1011','credit_card','success','615.58','USD','2024-04-20T12:00:00','ch_efg123','4242','stripe','2024-04-21 00:00:00 UTC'),
('PAY-012','ORD-1012','debit_card','success','26.99','USD','2024-05-01T14:30:00','ch_hij456','1111','stripe','2024-05-02 00:00:00 UTC'),
('PAY-013','ORD-1013','credit_card','refunded','129.59','USD','2024-05-15T11:45:00','ch_klm789','2222','stripe','2024-05-16 00:00:00 UTC'),
('PAY-014','ORD-1014','credit_card','success','97.19','USD','2024-06-01T08:15:00','ch_nop012','3333','stripe','2024-06-02 00:00:00 UTC'),
('PAY-015','ORD-1015','credit_card','success','496.79','USD','2024-06-15T16:00:00','ch_qrs345','4444','stripe','2024-06-16 00:00:00 UTC');


-- Shipping events (from logistics provider)
CREATE OR REPLACE TABLE `bq-metadata-spike.raw_ecommerce.shipping_events` (
  event_id STRING OPTIONS(description="Unique shipping event ID"),
  order_id STRING OPTIONS(description="FK to orders"),
  carrier STRING OPTIONS(description="Shipping carrier name"),
  tracking_number STRING OPTIONS(description="Carrier tracking number"),
  event_type STRING OPTIONS(description="Event type: picked_up, in_transit, delivered, returned"),
  event_timestamp STRING OPTIONS(description="Event time as string from carrier API"),
  location STRING OPTIONS(description="Event location"),
  source_system STRING OPTIONS(description="Originating system"),
  _ingested_at TIMESTAMP OPTIONS(description="Ingestion timestamp")
)
OPTIONS(
  description="Raw shipping/logistics events from carrier APIs.",
  labels=[("source", "logistics"), ("owner", "ops-engineering")]
);

INSERT INTO `bq-metadata-spike.raw_ecommerce.shipping_events` VALUES
('SE-001','ORD-1001','FedEx','FX123456','picked_up','2024-01-15T18:00:00','San Francisco, CA','shipstation','2024-01-16 00:00:00 UTC'),
('SE-002','ORD-1001','FedEx','FX123456','in_transit','2024-01-16T06:00:00','Oakland, CA','shipstation','2024-01-17 00:00:00 UTC'),
('SE-003','ORD-1001','FedEx','FX123456','delivered','2024-01-16T14:30:00','San Francisco, CA','shipstation','2024-01-17 00:00:00 UTC'),
('SE-004','ORD-1002','UPS','UP789012','picked_up','2024-01-21T10:00:00','Warehouse, NJ','shipstation','2024-01-22 00:00:00 UTC'),
('SE-005','ORD-1002','UPS','UP789012','in_transit','2024-01-23T08:00:00','Newark, NJ','shipstation','2024-01-24 00:00:00 UTC'),
('SE-006','ORD-1002','UPS','UP789012','delivered','2024-01-25T11:00:00','New York, NY','shipstation','2024-01-26 00:00:00 UTC'),
('SE-007','ORD-1003','FedEx','FX345678','picked_up','2024-02-01T16:00:00','Warehouse, IL','shipstation','2024-02-02 00:00:00 UTC'),
('SE-008','ORD-1003','FedEx','FX345678','delivered','2024-02-02T10:00:00','Chicago, IL','shipstation','2024-02-03 00:00:00 UTC'),
('SE-009','ORD-1005','FedEx','FX901234','picked_up','2024-03-01T08:00:00','Warehouse, TX','shipstation','2024-03-02 00:00:00 UTC'),
('SE-010','ORD-1005','FedEx','FX901234','in_transit','2024-03-02T12:00:00','Dallas, TX','shipstation','2024-03-03 00:00:00 UTC'),
('SE-011','ORD-1013','UPS','UP567890','picked_up','2024-05-16T09:00:00','Warehouse, TN','shipstation','2024-05-17 00:00:00 UTC'),
('SE-012','ORD-1013','UPS','UP567890','returned','2024-05-22T14:00:00','Nashville, TN','shipstation','2024-05-23 00:00:00 UTC');
```

### 2.3 Create Staging Layer (Cleaned & Typed)

```sql
-- ============================================================
-- STAGING LAYER: staging_ecommerce
-- Deduplication, type casting, null handling
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
```

### 2.4 Create Analytics Layer (Business Logic)

```sql
-- ============================================================
-- ANALYTICS LAYER: analytics_ecommerce
-- Joins, aggregations, derived metrics
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
```

### 2.5 Create Reporting Layer (BI Views)

```sql
-- ============================================================
-- REPORTING LAYER: reporting_ecommerce
-- Views optimized for BI dashboards (Looker, Tableau, Power BI)
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
```

---

## Part 3: Run the Metadata Extraction

See `extract_metadata.py` — this script pulls everything a metadata platform would consume.

```bash
pip install google-cloud-bigquery google-cloud-bigquery-datatransfer

export GOOGLE_APPLICATION_CREDENTIALS=~/bq-spike-sa-key.json
python extract_metadata.py
```

This produces `bigquery_metadata.json` — a comprehensive metadata payload.

---

## Part 4: Visualize

Open `metadata_dashboard.html` in a browser, or run the React artifact in Claude.

---

## Cleanup

```bash
# Delete all datasets
bq rm -r -f bq-metadata-spike:raw_ecommerce
bq rm -r -f bq-metadata-spike:staging_ecommerce
bq rm -r -f bq-metadata-spike:analytics_ecommerce
bq rm -r -f bq-metadata-spike:reporting_ecommerce

# Or delete the entire project
gcloud projects delete bq-metadata-spike
```
