from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

def spark_task(task_id, script_path):
    return SparkSubmitOperator(
        task_id=task_id,
        application=script_path,
        conn_id="spark_default",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        conf={
            "spark.executor.instances": "1",
            "spark.executor.memory": "512M",
            "spark.driver.memory": "512M",
            "spark.executor.cores": "1",
            "spark.sql.shuffle.partitions": "1",
            "spark.default.parallelism": "1",
            "spark.sql.adaptive.enabled": "true",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        }
    )

with DAG(
    dag_id="olist_silver_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=['olist', 'silver']
) as dag:

    process_customers_silver = spark_task(
        task_id="silver_customers",
        script_path="/opt/airflow/scripts/silver/olist_customers_silver.py"
    )

    process_geolocation_silver = spark_task(
        task_id="silver_geolocation",
        script_path="/opt/airflow/scripts/silver/olist_geolocation_silver.py"
    )

    process_orderPayments_silver = spark_task(
        task_id="silver_orderPayments",
        script_path="/opt/airflow/scripts/silver/olist_order_payments_silver.py"
    )

    process_orders_silver = spark_task(
        task_id="silver_orders",
        script_path="/opt/airflow/scripts/silver/olist_orders_silver.py"
    )

    process_orderReviews_silver = spark_task(
        task_id="silver_orderReviews",
        script_path="/opt/airflow/scripts/silver/olist_order_reviews_silver.py"
    )

    process_products_silver = spark_task(
        task_id="silver_products",
        script_path="/opt/airflow/scripts/silver/olist_products_silver.py"
    )

    process_sellers_silver = spark_task(
        task_id="silver_sellers",
        script_path="/opt/airflow/scripts/silver/olist_sellers_silver.py"
    )

    process_categories_silver = spark_task(
        task_id="silver_categories",
        script_path="/opt/airflow/scripts/silver/product_category_name_translation.py"
    )

    process_orderItems_silver = spark_task(
        task_id="silver_orderItems",
        script_path="/opt/airflow/scripts/silver/olist_order_items_silver.py"
    )
    process_customers_silver >> process_geolocation_silver >> process_orderPayments_silver >> process_orders_silver >> process_orderReviews_silver >> process_products_silver >> process_sellers_silver >> process_categories_silver >> process_orderItems_silver