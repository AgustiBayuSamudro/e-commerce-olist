from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Olist_Silver_Products") \
    .getOrCreate()

# Konfigurasi Hadoop untuk akses MinIO
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_bronze = spark.read.parquet("s3a://etl-data/data-lake/bronze/product")
df_bronze.createOrReplaceTempView("products_stream")

df_silver = spark.sql("""
    select
        TRIM(product_id) AS product_id,
        TRIM(LOWER(product_category_name)) AS product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm ,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    from products_stream
    WHERE product_id IS NOT NULL;
""")

print("Memulai pengiriman data ke MinIO (Silver Layer)...")

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://etl-data/data-lake/silver/product")

print("Data berhasil disimpan ke MinIO (Silver Layer).")
spark.stop()