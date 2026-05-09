CREATE SCHEMA IF NOT EXISTS minio.gold
WITH(
	location = 's3a://etl-data/trino-warehouse/gold/'
);

SELECT * FROM minio.gold.v_product_insights;

CREATE OR REPLACE VIEW minio.gold.v_product_insights AS
SELECT
    p.product_category_name AS product_category,
    COUNT(f.order_id) AS items_sold,
    SUM(f.total_amount) AS revenue,
    AVG(CAST(r.review_score AS DOUBLE)) AS avg_rating
FROM minio.gold.fact_sales f
JOIN minio.silver.products p ON f.product_id = p.product_id
LEFT JOIN minio.silver.orderReviews r ON f.order_id = r.order_id
GROUP BY p.product_category_name
ORDER BY revenue DESC;

SELECT * FROM minio.gold.v_geospatial_analysis

CREATE OR REPLACE VIEW minio.gold.v_geospatial_analysis AS
SELECT
    c.customer_state,
    c.customer_city,
    AVG(CAST(g.geolocation_lat AS DOUBLE)) AS latitude,
    AVG(CAST(g.geolocation_lng AS DOUBLE)) AS longitude,
    SUM(f.total_amount) AS total_sales,
    COUNT(f.order_id) AS order_count
FROM minio.gold.fact_sales f
JOIN minio.gold.dim_customer c ON f.customer_id = c.customer_id
JOIN minio.silver.geolocation g ON CAST(c.customer_zip_code_prefix AS VARCHAR) = CAST(g.geolocation_zip_code_prefix AS VARCHAR)
GROUP BY c.customer_state, c.customer_city;

SELECT * FROM minio.gold.v_kpi_metrics;

CREATE OR REPLACE VIEW minio.gold.v_kpi_metrics AS
WITH customer_stats AS (
    SELECT
        customer_id,
        SUM(total_amount) AS total_customer_spend
    FROM minio.gold.fact_sales
    GROUP BY customer_id
),
overall_kpi AS (
    SELECT
        SUM(total_amount) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders
    FROM minio.gold.fact_sales
)
SELECT
    k.total_revenue,
    k.total_orders,
    (SELECT AVG(total_customer_spend) FROM customer_stats) AS customer_lifetime_value_avg,
    CAST(COUNT(DISTINCT CASE WHEN o.order_status = 'delivered' THEN o.order_id END) AS DOUBLE) /
    NULLIF(COUNT(DISTINCT o.order_id), 0) AS order_success_rate
FROM minio.gold.fact_sales f
JOIN minio.silver.orders o ON f.order_id = o.order_id
CROSS JOIN overall_kpi k
GROUP BY k.total_revenue, k.total_orders;

SELECT * FROM minio.gold.fact_sales;

CREATE TABLE minio.gold.fact_sales
WITH (format = 'PARQUET', external_location = 's3a://etl-data/data-lake/gold/fact_sales/') AS
SELECT
    oi.order_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_amount,
    o.order_status
FROM minio.silver.orders o
JOIN minio.silver.orderItems oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered';

SELECT * FROM minio.gold.dim_time;

CREATE TABLE minio.gold.dim_time
WITH (format = 'PARQUET', external_location = 's3a://etl-data/data-lake/gold/dim_time/') AS
SELECT DISTINCT
    CAST(order_purchase_timestamp AS DATE) AS date_key,
    EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
    EXTRACT(QUARTER FROM order_purchase_timestamp) AS quarter,
    EXTRACT(MONTH FROM order_purchase_timestamp) AS month,
    FORMAT_DATETIME(order_purchase_timestamp, 'MMMM') AS month_name,
    EXTRACT(DAY FROM order_purchase_timestamp) AS day
FROM minio.silver.orders;

SELECT * FROM minio.gold.dim_product;

CREATE TABLE minio.gold.dim_product
WITH (format = 'PARQUET', external_location = 's3a://etl-data/data-lake/gold/dim_product/') AS
SELECT
    p.product_id,
    c.product_category_name_english AS category,
    p.product_weight_g,
    p.product_length_cm
FROM minio.silver.products p
LEFT JOIN minio.silver.categories c ON p.product_category_name = c.product_category_name;

SELECT * FROM minio.silver.customers;

CREATE TABLE minio.gold.dim_customer
WITH (format = 'PARQUET', external_location = 's3a://etl-data/data-lake/gold/dim_customer/') AS
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM minio.silver.customers;