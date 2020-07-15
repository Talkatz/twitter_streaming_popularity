from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import Column, from_json, explode, split, col, to_json, udf, window
from pyspark.sql.types import *
import re
import datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

bootstrapServers = "localhost:9092"
topic_tweets_analysis = 'full_tweets_for_analysis'
topic_kafka_sentiment = "alerts_sentiments"
topic_kafka_aggregations = "alerts_aggregations"

spark = SparkSession \
    .builder \
    .appName("alerts") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrapServers) \
  .option("startingOffsets", "earliest") \
  .option("subscribe", topic_tweets_analysis) \
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
    .queryName("dfSchema")\
    .format('memory') \
    .start()

# The sentiment instance and method
sid = SentimentIntensityAnalyzer()
# Cleaning the text from irrelevant data such as tagging of other users and links
def textAnalysys_vader_nltk(text_str):
    text_str = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|'\
                       '(?:%[0-9a-fA-F][0-9a-fA-F]))+','', text_str)
    text_str = re.sub("(@[A-Za-z0-9_]+)","", text_str)
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

# Creating the alert where the data frame will hold only tweets with distinct sentiment value
#and with more than 50 followers
df_with_sentiment = df_with_sentiment\
    .select("*")\
    .where((col("followers") > "50") & ((col("sentiment") > "0.5") | (col("sentiment") < "-0.5")))

query = df_with_sentiment \
    .writeStream \
    .queryName("df_sentimentTIme")\
    .format('memory') \
    .start()

# Creating the second alert which aggregated over a 30 minutes window the first alert with sum of followers,
# a count for the mentions of the game console, and average sentiment
df_sum_followers = df_with_sentiment.groupBy('game_console',  window("time", "30 minutes")).agg({'followers': 'sum', 'game_console':'count', 'sentiment': 'avg'})

# Checkpoint for a failure in the process
checkpointSprkToKafkaSentm= "hdfs://localhost:8020/.../alertsSentm.checkPnt"

# Sending the values of the first alert back to kafka
query = df_with_sentiment \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option("topic", topic_kafka_sentiment) \
    .option("checkpointLocation", checkpointSprkToKafkaSentm)\
    .trigger(processingTime='5 seconds') \
    .start()

# Checkpoint for a failure in the process
checkpointSprkToKafkaFlwers = "hdfs://localhost:8020/.../alertsFlwers.checkPnt"

# Sending the values of the second alert back to kafka with an update mode because of the aggregation and time window
query3 = df_sum_followers \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option("topic", topic_kafka_aggregations) \
    .option("checkpointLocation", checkpointSprkToKafkaFlwers)\
    .outputMode("update") \
    .trigger(processingTime='30 minutes') \
    .start()