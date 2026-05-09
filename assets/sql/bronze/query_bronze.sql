CREATE SCHEMA IF NOT EXISTS minio.bronze
WITH(
	location = 's3a://etl-data/trino-warehouse/bronze/'
);

DROP TABLE minio.bronze.categories;
SELECT * FROM minio.bronze.categories;

CREATE TABLE minio.bronze.categories(
        product_category_name varchar,
        product_category_name_english varchar
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/category/',
    format = 'PARQUET'
);

DROP TABLE minio.bronze.sellers;
SELECT * FROM minio.bronze.sellers;

CREATE TABLE minio.bronze.sellers(
        seller_id varchar,
        seller_zip_code_prefix integer,
        seller_city varchar,
        seller_state varchar
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/seller/',
    format = 'PARQUET'
);

DROP TABLE minio.bronze.products;
SELECT * FROM minio.bronze.products;

CREATE TABLE minio.bronze.products(
        product_id varchar,
        product_category_name varchar,
        product_name_lenght integer,
        product_description_lenght integer,
        product_photos_qty integer,
        product_weight_g integer,
        product_length_cm integer,
        product_height_cm integer,
        product_width_cm integer
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/product/',
    format = 'PARQUET'
);

DROP TABLE minio.bronze.orders;
SELECT * FROM minio.bronze.orders;

CREATE TABLE minio.bronze.orders(
        order_id varchar,
        customer_id varchar,
        order_status varchar,
        order_purchase_timestamp timestamp,
        order_approved_at timestamp,
        order_delivered_carrier_date timestamp,
        order_delivered_customer_date timestamp,
        order_estimated_delivery_date timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/order/',
    format = 'PARQUET'
);

DROP TABLE minio.bronze.orderReviews;
SELECT * FROM minio.bronze.orderReviews;

CREATE TABLE minio.bronze.orderReviews(
        review_id varchar,
        order_id varchar,
        review_score varchar,
        review_comment_title varchar,
        review_comment_message varchar,
        review_creation_date varchar,
        review_answer_timestamp varchar
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/orderReview/',
    format = 'PARQUET'
);

DROP TABLE minio.bronze.orderPayments;
SELECT * FROM minio.bronze.orderPayments;

CREATE TABLE minio.bronze.orderPayments(
	    order_id varchar,
        payment_sequential integer,
        payment_type varchar,
        payment_installments integer,
        payment_value double
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/orderPayment/',
    format = 'PARQUET'
);


DROP TABLE minio.bronze.orderItems;
SELECT * FROM minio.bronze.orderItems;

CREATE TABLE minio.bronze.orderItems(
 		order_id varchar,
        order_item_id integer,
        product_id varchar,
        seller_id varchar,
        shipping_limit_date timestamp,
        price double,
        freight_value double
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/orderItem/',
    format = 'PARQUET'
);

DROP TABLE minio.bronze.geolocation;
SELECT * FROM minio.bronze.geolocation

CREATE TABLE minio.bronze.geolocation(
    geolocation_zip_code_prefix integer,
    geolocation_lat double,
    geolocation_lng double,
    geolocation_city varchar,
    geolocation_state varchar
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/geolocation/',
    format = 'PARQUET'
);

DROP TABLE minio.bronze.customers
SELECT * FROM minio.bronze.customers

CREATE TABLE minio.bronze.customers(
	customer_id varchar,
    customer_unique_id varchar,
    customer_zip_code_prefix integer,
    customer_city varchar,
    customer_state varchar
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/customer/',
    format = 'PARQUET'
);