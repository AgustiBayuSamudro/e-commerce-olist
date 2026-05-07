from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Olist_Silver_Customers") \
    .getOrCreate()

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
    SELECT DISTINCT
        TRIM(customer_id) AS customer_id,
        TRIM(customer_unique_id) AS customer_unique_id,
        TRIM(customer_zip_code_prefix) AS customer_zip_code_prefix,
        TRIM(LOWER(customer_city)) AS customer_city,
        TRIM(UPPER(customer_state)) AS customer_state,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM customers_stream
    WHERE customer_id IS NOT NULL;
""")

print("Memulai pengiriman data ke MinIO (Silver Layer)...")

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3a://etl-data/data-lake/silver/customer")

print("Data berhasil disimpan ke MinIO (Silver Layer).")
spark.stop()