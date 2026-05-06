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

df_bronze = spark.read.parquet("s3a://etl-data/data-lake/bronze/customer")
df_bronze.createOrReplaceTempView("customers_stream")

df_silver = spark.sql("""
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(customer_unique_id) AS customer_unique_id,
        TRIM(customer_zip_code_prefix) AS customer_zip_code_prefix,
        TRIM(LOWER(customer_city)) AS customer_city,
        TRIM(customer_state) AS customer_state,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM customers_stream
    WHERE customer_id IS NOT NULL;
""").dropDuplicates(["customer_id"])

print("Memulai pengiriman data ke PostgreSQL...")
df_silver.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://103.196.155.168:5432/dwh_olist") \
    .option("dbtable", "silver.customers") \
    .option("batchsize", "5000") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", "true") \
    .mode("overwrite") \
    .save()

print("Data berhasil disimpan ke PostgreSQL.")
spark.stop()