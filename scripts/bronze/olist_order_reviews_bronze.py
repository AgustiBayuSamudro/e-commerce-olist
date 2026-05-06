from pyspark.sql import SparkSession

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
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM raw_orderReviews;
""")

output_path = "s3a://etl-data/data-lake/bronze/orderReview"
df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Berhasil! Data bronze orderReview tersimpan di: {output_path}")
spark.stop()