from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("Olist_Silver_OrderReviews") \
    .getOrCreate()

# Konfigurasi Hadoop untuk akses MinIO
sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_bronze = spark.read.parquet("s3a://etl-data/data-lake/bronze/orderReview")
df_bronze.createOrReplaceTempView("orderReviews_stream")

df_silver = spark.sql("""
    SELECT DISTINCT
        TRIM(review_id) AS review_id,
        TRIM(order_id) AS order_id,
        review_score,
        TRIM(LOWER(review_comment_title)) AS review_comment_title,
        TRIM(LOWER(review_comment_message)) AS review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM orderReviews_stream
    WHERE review_id IS NOT NULL AND order_id IS NOT NULL;
""")

print("Memulai pengiriman data ke MinIO (Silver Layer)...")

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://etl-data/data-lake/silver/orderReview")

print("Data berhasil disimpan ke MinIO (Silver Layer).")
spark.stop()