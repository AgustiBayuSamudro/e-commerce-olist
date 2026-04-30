from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

def spark_task(task_id, script_path):
    return SparkSubmitOperator(
        task_id=task_id,
        application=script_path,
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
    )

with DAG(
    dag_id="olist_bronze_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=['olist', 'bronze']
) as dag:

    process_customers = spark_task(
        task_id="bronze_customers",
        script_path="/opt/airflow/scripts/bronze/olist_customers_bronze.py"
    )

    process_categories = spark_task(
        task_id="bronze_categories",
        script_path="/opt/airflow/scripts/bronze/product_category_name_translation.py"
    )

    process_orders = spark_task(
        task_id="bronze_orders",
        script_path="/opt/airflow/scripts/bronze/olist_orders_bronze.py"
    )

    process_products = spark_task(
        task_id="bronze_products",
        script_path="/opt/airflow/scripts/bronze/olist_products_bronze.py"
    )

    process_sellers = spark_task(
        task_id="bronze_sellers",
        script_path="/opt/airflow/scripts/bronze/olist_sellers_bronze.py"
    )

    process_orderItems = spark_task(
        task_id="bronze_orderItems",
        script_path="/opt/airflow/scripts/bronze/olist_order_items_bronze.py"
    )

    process_orderReviews = spark_task(
        task_id="bronze_orderReviews",
        script_path="/opt/airflow/scripts/bronze/olist_order_reviews_bronze.py"
    )

    process_orderPayments = spark_task(
        task_id="bronze_orderPayments",
        script_path="/opt/airflow/scripts/bronze/olist_order_payments_bronze.py"
    )

    process_geolocations = spark_task(
        task_id="bronze_geolocations",
        script_path="/opt/airflow/scripts/bronze/olist_geolocation_bronze.py"
    )

    process_customers >> process_categories >> process_orders >> process_products >> process_sellers >> process_orderItems >> process_orderReviews >> process_orderPayments >> process_geolocations