from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import Column, from_json, explode, split, col, to_json, udf
from pyspark.sql.types import *
import re
import datetime
import json
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

bootstrapServers = "localhost:9092"
topic_tweets_parquet = 'full_tweets_to_parquet'

spark = SparkSession \
    .builder \
    .appName("process") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", topic_tweets_parquet) \
    .load()

schema = StructType([
    StructField("user_name", StringType()),
    StructField("user_id", StringType()),
    StructField("created_at", StringType()),
    StructField("text", StringType()),
    StructField("game_console", StringType()),
    StructField("hashtags", StringType()),
    StructField("followers", IntegerType()),
    StructField("retweets", IntegerType())])

# data frame for holding the incoming data in a defined schema
dfSchema = df.select(col("value").cast("string")) \
    .select(from_json(col("value"), schema).alias("value")) \
    .select("value.*")

query = dfSchema \
    .writeStream \
    .queryName("testSchema") \
    .format('memory') \
    .start()

# The sentiment instance and method
sid = SentimentIntensityAnalyzer()
# Cleaning the text from irrelevant data such as tagging of other users and links
def textAnalysys_vader_nltk(text_str):
    text_str = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|' \
                      '(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text_str)
    text_str = re.sub("(@[A-Za-z0-9_]+)", "", text_str)
    scores = sid.polarity_scores(text_str)
    result = scores.get('compound')
    return result

# A method to get the current time
def getNowTime():
    time = datetime.datetime.now().time()
    timeStr = str(time)
    return timeStr

# UDF conversions of the methods
get_textAnalysysVader_udf = udf(textAnalysys_vader_nltk, FloatType())
get_time_udf = udf(getNowTime, StringType())

# Applying the methods of the incoming data from kafka
df_with_sentiment = dfSchema.withColumn('sentiment', get_textAnalysysVader_udf(col('text')))
df_with_sentiment = df_with_sentiment.withColumn("time", get_time_udf())

#query = df_with_sentiment \
#    .writeStream \
#    .queryName("df_text_analysis")\
#    .format('memory') \
#    .start()

destPathParquet = "hdfs://localhost:8020/.../tweets_parquet/"
# Checkpoint for a failure in the process
checkpointParquet= "hdfs://localhost:8020/.../tweets_parquet.checkPnt"
# Sending the values forward to Parquet with two partitions: game console type and the date without time
write_to_hdfsParquet_query = df_with_sentiment\
    .writeStream\
    .format("parquet")\
    .partitionBy("game_console", "created_at")\
    .option("path", destPathParquet)\
    .option("checkpointLocation", checkpointParquet)\
    .outputMode("append")\
    .start()