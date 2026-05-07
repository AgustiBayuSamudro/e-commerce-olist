from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Olist_Silver_OrderPayments") \
    .getOrCreate()

# Konfigurasi Hadoop untuk akses MinIO
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_bronze = spark.read.parquet("s3a://etl-data/data-lake/bronze/orderPayment")
df_bronze.createOrReplaceTempView("orderPayments_stream")

df_silver = spark.sql("""
    SELECT
        TRIM(order_id) AS order_id,
        hex(md5((order_id || payment_sequential))) AS order_payment_id,
        payment_sequential,
        TRIM(payment_type) AS payment_type,
        payment_installments,
        payment_value,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM orderPayments_stream
    WHERE order_id IS NOT NULL AND payment_sequential IS NOT NULL;
""")

print("Memulai pengiriman data ke MinIO (Silver Layer)...")

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://etl-data/data-lake/silver/orderPayment")

print("Data berhasil disimpan ke MinIO (Silver Layer).")
spark.stop()