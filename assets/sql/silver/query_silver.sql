CREATE SCHEMA IF NOT EXISTS minio.silver
WITH(
	location = 's3a://etl-data/trino-warehouse/silver/'
);

DROP TABLE minio.silver.categories;
SELECT * FROM minio.silver.categories;

CREATE TABLE minio.silver.categories(
        product_category_id varchar,
        product_category_name varchar,
        product_category_name_english varchar,
        created_at timestamp,
        updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/category/',
    format = 'PARQUET'
);

DROP TABLE minio.silver.sellers;
SELECT * FROM minio.silver.sellers;

CREATE TABLE minio.silver.sellers(
        seller_id varchar,
        seller_zip_code_prefix varchar,
        seller_city varchar,
        seller_state varchar,
        created_at timestamp,
        updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/seller/',
    format = 'PARQUET'
);

DROP TABLE minio.silver.products;
SELECT * FROM minio.silver.products;

CREATE TABLE minio.silver.products(
        product_id varchar,
        product_category_name varchar,
        product_name_lenght integer,
        product_description_lenght integer,
        product_photos_qty integer,
        product_weight_g integer,
        product_length_cm integer,
        product_height_cm integer,
        product_width_cm integer,
        created_at timestamp,
        updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/product/',
    format = 'PARQUET'
);

DROP TABLE minio.silver.orders;
SELECT * FROM minio.silver.orders;

CREATE TABLE minio.silver.orders(
        order_id varchar,
        customer_id varchar,
        order_status varchar,
        order_purchase_timestamp timestamp,
        order_approved_at timestamp,
        order_delivered_carrier_date timestamp,
        order_delivered_customer_date timestamp,
        order_estimated_delivery_date timestamp,
        created_at timestamp,
        updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/order/',
    format = 'PARQUET'
);

DROP TABLE minio.silver.orderReviews;
SELECT * FROM minio.silver.orderReviews;

CREATE TABLE minio.silver.orderReviews(
        review_id varchar,
        order_id varchar,
        review_score varchar,
        review_comment_title varchar,
        review_comment_message varchar,
        review_creation_date varchar,
        review_answer_timestamp varchar,
        created_at timestamp,
        updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/orderReview/',
    format = 'PARQUET'
);

DROP TABLE minio.silver.orderPayments;
SELECT * FROM minio.silver.orderPayments;

CREATE TABLE minio.silver.orderPayments(
	    order_id varchar,
        payment_sequential integer,
        payment_type varchar,
        payment_installments integer,
        payment_value double,
        created_at timestamp,
        updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/orderPayment/',
    format = 'PARQUET'
);


DROP TABLE minio.silver.orderItems;
SELECT * FROM minio.silver.orderItems;

CREATE TABLE minio.silver.orderItems(
 		order_id varchar,
        order_item_id integer,
        product_id varchar,
        seller_id varchar,
        shipping_limit_date timestamp,
        price double,
        freight_value double,
        created_at timestamp,
        updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/orderItem/',
    format = 'PARQUET'
);


DROP TABLE minio.silver.geolocation;
SELECT * FROM minio.silver.geolocation

CREATE TABLE minio.silver.geolocation(
	geolocation_id varchar,
    geolocation_zip_code_prefix integer,
    geolocation_lat double,
    geolocation_lng double,
    geolocation_city varchar,
    geolocation_state varchar,
    created_at timestamp,
    updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/geolocation/',
    format = 'PARQUET'
);


DROP TABLE minio.silver.customers;
SELECT * FROM minio.silver.customers;

CREATE TABLE minio.silver.customers(
	customer_id varchar,
    customer_unique_id varchar,
    customer_zip_code_prefix varchar,
    customer_city varchar,
    customer_state varchar,
    created_at timestamp,
    updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/customer/',
    format = 'PARQUET'
);
