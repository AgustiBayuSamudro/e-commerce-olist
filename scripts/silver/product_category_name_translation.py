from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Olist_Silver_Categories") \
    .getOrCreate()

# Konfigurasi Hadoop untuk akses MinIO
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_bronze = spark.read.parquet("s3a://etl-data/data-lake/bronze/category")
df_bronze.createOrReplaceTempView("categories_stream")

df_silver = spark.sql("""
    SELECT
        hex(md5((product_category_name))) AS product_category_id,
        trim(lower(product_category_name)) AS product_category_name,
        TRIM(LOWER(product_category_name_english)) AS product_category_name_english,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM categories_stream
    WHERE product_category_name IS NOT NULL AND product_category_name_english IS NOT NULL;
""")

print("Memulai pengiriman data ke MinIO (Silver Layer)...")

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://etl-data/data-lake/silver/category")

print("Data berhasil disimpan ke MinIO (Silver Layer).")
spark.stop()