from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

def spark_task(task_id, script_path):
    return SparkSubmitOperator(
        task_id=task_id,
        application=script_path,
        conn_id="spark_default",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.2",
        conf={
            "spark.executor.instances": "1",
            "spark.executor.memory": "800M",
            "spark.driver.memory": "800M",
            "spark.executor.cores": "1",
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
        script_path="/opt/airflow/scripts/silver/olist_customers_silver_stream.py"
    )

    process_customers_silver