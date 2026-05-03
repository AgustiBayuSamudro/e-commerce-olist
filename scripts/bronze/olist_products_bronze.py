from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Olist_Bronze_Products") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.csv("s3a://etl-data/data-lake/raw/olist_products/", header=True, inferSchema=True)
df_raw.createOrReplaceTempView("raw_products")

df_bronze = spark.sql("""
    select
        TRIM(product_id) AS product_id,
        TRIM(LOWER(product_category_name)) AS product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm ,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    from raw_products;
""").dropDuplicates(["product_id"])

output_path = "s3a://etl-data/data-lake/bronze/product"
df_bronze.write.mode("append").parquet(output_path)

print(f"Berhasil! Data bronze product tersimpan di: {output_path}")
spark.stop()