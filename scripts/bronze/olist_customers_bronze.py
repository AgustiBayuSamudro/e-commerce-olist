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
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM raw_customers;
""")

output_path = "s3a://etl-data/data-lake/bronze/customer"
df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Berhasil! Data bronze customer tersimpan di: {output_path}")
spark.stop()