from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

spark = SparkSession.builder \
    .appName("Olist_Bronze_Categories") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.csv("s3a://etl-data/data-lake/raw/product_category_name_translation/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_categories")

df_bronze = spark.sql("""
    SELECT
        hex(md5((product_category_name))) AS product_category_id,
        trim(lower(product_category_name)) AS product_category_name,
        TRIM(LOWER(product_category_name_english)) AS product_category_name_english
    FROM raw_categories;
""")

output_path = "s3a://etl-data/data-lake/bronze/category"
df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Berhasil! Data bronze category tersimpan di: {output_path}")
df_kafka = df_bronze.select(
    to_json(struct("*")).alias("value")
)

df_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "topic_categories") \
    .save()

print("Berhasil kirim ke Kafka (topic_categories)")

spark.stop()