from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Olist_Silver_Customers") \
    .getOrCreate()

# Konfigurasi Hadoop untuk akses MinIO
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 1. Membaca data Parquet dari layer Bronze (MinIO)
df_bronze = spark.read.parquet("s3a://etl-data/data-lake/bronze/customer")

# 2. Mendaftarkan DataFrame sebagai Temporary View agar bisa diolah via SQL
df_bronze.createOrReplaceTempView("customers_stream")

# 3. Melakukan transformasi data menggunakan SQL
df_silver = spark.sql("""
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        created_at,
        updated_at
    FROM customers_stream
    WHERE customer_id IS NOT NULL
""")

# 4. Menulis hasil transformasi langsung ke PostgreSQL (Layer Silver)
# Tanpa menggunakan fungsi pembungkus atau epoch_id
print("Memulai pengiriman data ke PostgreSQL...")
df_silver.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://103.196.155.168:5432/dwh_olist") \
    .option("dbtable", "silver.dim_customers") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("Data berhasil disimpan ke PostgreSQL.")

# Menutup session
spark.stop()