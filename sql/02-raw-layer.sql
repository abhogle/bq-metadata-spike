-- ============================================================
-- Layer: Raw
-- Purpose: Create and populate raw e-commerce tables with source system data
-- Run: bq query --use_legacy_sql=false < sql/02-raw-layer.sql
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
