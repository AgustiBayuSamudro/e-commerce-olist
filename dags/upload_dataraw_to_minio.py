from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import os

def upload_csv_to_minio_logic():    
    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket_name = "etl-data"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} berhasil dibuat.")

    source_folder = "/opt/airflow/data/raw"

    files = [f for f in os.listdir(source_folder) if f.endswith('.csv')]
    
    if not files:
        print(f"Tidak ada file CSV yang ditemukan di {source_folder}")
        return

    for file_name in files:
        file_path = os.path.join(source_folder, file_name)       

        clean_name = os.path.splitext(file_name)[0]
        table_name = clean_name.replace("_dataset", "")
        
        object_name = f"data-lake/raw/{table_name}/{file_name}"
        client.fput_object(
            bucket_name,
            object_name,
            file_path
        )
        print(f"Berhasil diunggah ke sub-folder: {object_name}")

with DAG(
    dag_id="ingest_olist_to_minio",
    start_date=datetime(2026, 4, 1),
    schedule=None, 
    catchup=False,
    tags=['ecommerce', 'olist']
) as dag:

    task_upload = PythonOperator(
        task_id="upload_raw_csv_files",
        python_callable=upload_csv_to_minio_logic
    )