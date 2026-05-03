from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Olist_Bronze_Sellers") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.csv("s3a://etl-data/data-lake/raw/olist_sellers/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_sellers")

df_bronze = spark.sql("""
    SELECT
        TRIM(seller_id) AS seller_id,
        TRIM(seller_zip_code_prefix) AS seller_zip_code_prefix,
        TRIM(LOWER(seller_city)) AS seller_city,
        TRIM(seller_state) AS seller_state
	FROM raw_sellers;
""").dropDuplicates(["seller_id"])

output_path = "s3a://etl-data/data-lake/bronze/seller"
df_bronze.write.mode("append").parquet(output_path)

print(f"Berhasil! Data bronze seller tersimpan di: {output_path}")
spark.stop()