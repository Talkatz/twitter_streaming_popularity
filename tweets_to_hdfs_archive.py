from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import Column, from_json, split, col
from pyspark.sql.types import *
import datetime
import json

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

bootstrapServers = "localhost:9092"
topic_hdfsArchive = 'full_tweets_to_archive'

spark = SparkSession \
    .builder \
    .appName("writeToArchive") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrapServers) \
  .option("startingOffsets", "earliest") \
  .option("subscribe", topic_hdfsArchive) \
  .load()

# The json schema
schema = StructType([
    StructField("user_name", StringType()),
    StructField("user_id", StringType()),
    StructField("created_at", StringType()),
    StructField("text", StringType()),
    StructField("game_console", StringType()),
    StructField("hashtags", StringType()),
    StructField("followers", IntegerType()),
    StructField("retweets", IntegerType())])

destPath = "hdfs://localhost:8020/.../archive/"
# Checkpoint for a failure in the process
checkpoint= "hdfs://localhost:8020/.../archive.checkPnt"
# Sending the values forward to HDFS in JSON format
StreamingQuery = df \
             .select(col("value").cast("string")).select(from_json(col("value"), schema).alias("value")).select("value.*") \
             .writeStream \
             .format("json") \
             .partitionBy("created_at") \
             .option("path", destPath) \
             .option("checkpointLocation", checkpoint) \
             .outputMode("append") \
             .start()
