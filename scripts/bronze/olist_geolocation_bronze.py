from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

spark = SparkSession.builder \
    .appName("Olist_Bronze_Geolocations") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.csv("s3a://etl-data/data-lake/raw/olist_geolocation/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_geolocations")

df_bronze = spark.sql("""
    SELECT
        hex(md5((geolocation_city || geolocation_state))) AS geolocation_id,
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    FROM raw_geolocations;
""")

output_path = "s3a://etl-data/data-lake/bronze/geolocation"
df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Berhasil! Data bronze geolocation tersimpan di: {output_path}")
df_kafka = df_bronze.select(
    to_json(struct("*")).alias("value")
)
df_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "topic_geolocation") \
    .save()

print("Berhasil kirim ke Kafka (topic_geolocation)")
spark.stop()