from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Olist_Silver_Geolocation") \
    .getOrCreate()

# Konfigurasi Hadoop untuk akses MinIO
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_bronze = spark.read.parquet("s3a://etl-data/data-lake/bronze/geolocation")
df_bronze.createOrReplaceTempView("geolocation_stream")

df_silver = spark.sql("""
    SELECT DISTINCT
        hex(md5((geolocation_city || geolocation_state))) AS geolocation_id,
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM geolocation_stream
    WHERE geolocation_zip_code_prefix IS NOT NULL;
""")

print("Memulai pengiriman data ke MinIO (Silver Layer)...")

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://etl-data/data-lake/silver/geolocation")

print("Data berhasil disimpan ke MinIO (Silver Layer).")
spark.stop()