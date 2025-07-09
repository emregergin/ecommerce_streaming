from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_endpoint = "http://localhost:9000"
minio_bucket_path = "s3a://my-bucket/purchased-items/"

spark = SparkSession.builder \
    .appName("KafkaToMinIO") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                                   "org.apache.hadoop:hadoop-aws:3.3.2") \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

product_schema = StructType([
    StructField("ProductId", StringType()),
    StructField("ItemCount", IntegerType()),
    StructField("ItemPrice", FloatType()),
    StructField("ItemDiscount", FloatType()),
])

purchase_schema = StructType([
    StructField("SessionId", StringType()),
    StructField("TimeStamp", StringType()),
    StructField("UserId", StringType()),
    StructField("TotalPrice", FloatType()),
    StructField("OrderId", StringType()),
    StructField("Products", ArrayType(product_schema)),
    StructField("PaymentType", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "PurchasedItem") \
    .option("startingOffsets", "earliest") \
    .load()

df_json = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), purchase_schema).alias("data")) \
    .select("data.*")

query = df_json.writeStream \
    .format("parquet") \
    .option("path", minio_bucket_path) \
    .option("checkpointLocation", "./checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination()