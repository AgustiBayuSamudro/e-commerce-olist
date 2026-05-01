from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

spark = SparkSession.builder \
    .appName("Olist_Bronze_OrderReviews") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.csv("s3a://etl-data/data-lake/raw/olist_order_reviews/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_orderReviews")

df_bronze = spark.sql("""
    SELECT
        TRIM(review_id) AS review_id,
        TRIM(order_id) AS order_id,
        review_score,
        TRIM(LOWER(review_comment_title)) AS review_comment_title,
        TRIM(LOWER(review_comment_message)) AS review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM raw_orderReviews;
""")

output_path = "s3a://etl-data/data-lake/bronze/orderReview"
df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Berhasil! Data bronze orderReview tersimpan di: {output_path}")
df_kafka = df_bronze.select(
    to_json(struct("*")).alias("value")
)
df_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "topic_order_reviews") \
    .save()

print("Berhasil kirim ke Kafka (topic_order_reviews)")
spark.stop()