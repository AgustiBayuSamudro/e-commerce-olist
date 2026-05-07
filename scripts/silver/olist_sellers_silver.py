from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Olist_Silver_Sellers") \
    .getOrCreate()

# Konfigurasi Hadoop untuk akses MinIO
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_bronze = spark.read.parquet("s3a://etl-data/data-lake/bronze/seller")
df_bronze.createOrReplaceTempView("sellers_stream")

df_silver = spark.sql("""
    SELECT
        TRIM(seller_id) AS seller_id,
        TRIM(seller_zip_code_prefix) AS seller_zip_code_prefix,
        TRIM(LOWER(seller_city)) AS seller_city,
        TRIM(seller_state) AS seller_state,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
	FROM sellers_stream
    WHERE seller_id IS NOT NULL;
""")

print("Memulai pengiriman data ke MinIO (Silver Layer)...")

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://etl-data/data-lake/silver/seller")

print("Data berhasil disimpan ke MinIO (Silver Layer).")
spark.stop()