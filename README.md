# 🚀 Flow Pipline Olist Brazilion E-commerce

Pengolahan data E-commerce Brazilion ini menggunakan **Python (PySpark)** yang menerapkan konsep ETL (Extract Transform Load), untuk data source menggunakan file dengan format csv kemudian di ingestion ke data-lake menggunkana **MINIO**. Proyek ini menggunakan konsep **Medallion architecture** dan **Trino Engine** sebagai pengolah data minio yang sudah di transform, untuk data visualisasi menggunakan **Metabase**.

## ✨ Tools yang digunakan
![GitHub Logo](assets/images/tools.png)

## 🛠️ Tools yang digunakan
* **Language**: Python
* **Workflow Orchestration**: Apache Airflow
* **Distributed Processing**: PySpark 3.5.0
* **Object Storage**: MinIO
* **Database**: PostgreSQL
* **Data Manipulation**: Spark SQL
* **Environment Management**: Python Dotenv
* **Database Driver**: Psycopg2 Binary
* **Containerization**: Docker & Docker Compose

## 🚀 Cara Menjalankan (Quick Start)
Pastikan kamu sudah menginstall **Docker** dan **Docker Compose** di laptopmu.

1. **Clone Repositori**
   ```bash
    https://github.com/AgustiBayuSamudro/e-commerce-olist.git

2. **Masukan data raw Olist Brazilion E-commerce pada folder**
   ``` bash
    └── E-COMMERCE-OLIST/
        └── data/
            └── raw/
                └── olist brazilion e-commerce.csv
3. **Build dan jalankan docker**
   ``` bash
   docker compose up --build -d
4. **Di sini saya menggunakan vps, jika tidak bisa ganti vps dengan localhost**
   ``` bash
   http://103.196.155.168 atau http://localhost
* **MINIO**
    ``` bash
    http://103.196.155.168:9001/
![GitHub Logo](assets/images/minio.jpeg)
* **AIRFLOW**
    ``` bash
    http://103.196.155.168:8080/
![GitHub Logo](assets/images/airflow.jpeg)
* **SPARK**
    ``` bash
    http://103.196.155.168:8081/
![GitHub Logo](assets/images/spark-master.jpeg)
* **METABASE**
    ``` bash
    http://103.196.155.168:8082/
![GitHub Logo](assets/images/metabase.jpeg)
5. **Buat scema pada trino engine**
Struktur schema pada engine Trino yang mengelola data di MinIO:
![GitHub Logo](assets/images/schema.jpeg)
## 🧪 Cara Pengujian (Testing)
1. **Buat data-lake minio**
![GitHub Logo](assets/images/data-lake.jpeg)
2. **Buat Koneksi spark-master di airflow**
![GitHub Logo](assets/images/koneksi-master.jpeg)
3. **Jalankan airflow**
![GitHub Logo](assets/images/run-airflow.jpeg)
4. **Buat db metabase**
![GitHub Logo](assets/images/dbmetabase.jpeg)
5. **Jalankan query sql pada folder assets di trino engine**
    ``` bash
    └── E-COMMERCE OLIST/
        └── assets/
            ├── images/
            └── sql/
                ├── bronze/
                ├── silver/
                └── gold/
6. **Cek view diagram**
* **bronze**
![GitHub Logo](assets/images/bronze.jpeg)
* **silver**
![GitHub Logo](assets/images/silver.jpeg)
* **gold**
![GitHub Logo](assets/images/gold.jpeg)
7. **Visualisasi Metabase**
![GitHub Logo](assets/images/visualisasi.jpeg)
8. **Selamat sudah berhasil melakukan pemrosesan data**
