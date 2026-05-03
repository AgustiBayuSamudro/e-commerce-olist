from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Olist_Bronze_Customers") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.csv("s3a://etl-data/data-lake/raw/olist_customers/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_customers")

df_bronze = spark.sql("""
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(customer_unique_id) AS customer_unique_id,
        TRIM(customer_zip_code_prefix) AS customer_zip_code_prefix,
        TRIM(LOWER(customer_city)) AS customer_city,
        TRIM(customer_state) AS customer_state,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM raw_customers;
""").dropDuplicates(["customer_id"])

output_path = "s3a://etl-data/data-lake/bronze/customer"
df_bronze.write.mode("append").parquet(output_path)

print(f"Berhasil! Data bronze customer tersimpan di: {output_path}")
spark.stop()