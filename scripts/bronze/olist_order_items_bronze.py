from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

spark = SparkSession.builder \
    .appName("Olist_Bronze_OrderItems") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.csv("s3a://etl-data/data-lake/raw/olist_order_items/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_orderItems")

df_bronze = spark.sql("""
    SELECT
        TRIM(order_id) AS order_id,
        order_item_id,
        TRIM(product_id) AS product_id,
        TRIM(seller_id) AS seller_id,
        shipping_limit_date,
        price,
        freight_value,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM raw_orderItems;
""")

output_path = "s3a://etl-data/data-lake/bronze/orderItem"
df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Berhasil! Data bronze orderItem tersimpan di: {output_path}")
df_kafka = df_bronze.select(
    to_json(struct("*")).alias("value")
)
df_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "topic_order_items") \
    .save()

print("Berhasil kirim ke Kafka (topic_order_items)")
spark.stop()