from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Olist_Bronze_Orders") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.csv("s3a://etl-data/data-lake/raw/olist_orders/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_orders")

df_bronze = spark.sql("""
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from raw_orders;
""")

output_path = "s3a://etl-data/data-lake/bronze/order"
df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Berhasil! Data bronze order tersimpan di: {output_path}")

spark.stop()