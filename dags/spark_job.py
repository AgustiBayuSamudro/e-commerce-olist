from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test ETL to MinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# data dummy
data = [("Agusti", 25), ("Budi", 30)]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

# simpan ke MinIO
df.write \
    .mode("overwrite") \
    .parquet("s3a://etl-data/users/")

spark.stop()