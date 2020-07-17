Streaming popularity on Twitter messages

The tool presented here is a project I did for a data engineering course. 
Every python file in the project works with the others in real-time, and have its own functionality.

Background

Twitter is a social networking service, in which one can post a message (‘tweet’), and other people can interact with that message, such as liking and sharing his message (‘retweet’). The tweets can be a great resource for various companies and organizations, by analyzing the frequency of a product’s mentioning, and the opinion of people on it, and more. 
A good example will be following the people response on twitter for a product commercial campaign, or the reactions to a speech of a candidate for some official office. Another example will be the preparation for an advertising campaign, with researching which use of words or slang, would be better for reaching more people.   

In order to do so, a Big Data angle must be taken for the following reasons:
* The data on twitter is massive, gathered from all around the world, and ever changing.
* In order to capture the reaction for some event, it is necessary to collect real-time data. 

The product that is presented here uses Big Data tools, so it will be able to listen and consume real time data from tweeter, pass it with a data pipeline to big data storage, and while transferring the data, it will be able to transform, enrich and clean the data before storing it. 
The chosen use case will the game consoles PlayStation and its rival Xbox.
The ‘Streaming popularity tool’ will enable the user to insert two ‘confronting’ products, and check which one is more popular on Twitter over a selected period of time, with the assumption that most of the people will speak about the product they use (or wish to have) in a positive way. It means that the majority will mention ‘PlayStation’, for example, in a positive way, and not to condemn it. Since in some of the cases this assumption will not be true, a sentiment check of the text will show if the tweet is positive, negative or neutral. 
The project was done using the Hadoop framework, and was written in Python code, using various APIs for the different assignments.

So what is the ‘Streaming popularity tool’? It basically a Big Data pipeline.  The pipeline is added to the repository as a jpeg and to understand the process, one should view it while reading the following explanation of the flow. 
The starting point of the pipeline is Twitter. By using Tweepy, a python library for using Twitter’s API, one can listen to a real time stream of messages that contains selected key words. For the use case, the words are the ones describing the games consoles, such as: Playstation, PS4, Xbox, x-box and others. 
Relevant data of each message is collected: User name, User Id, the date, the text, the game console, hashtags, number of followers of the user that posted the message, and data if the message was retweeted. 
Kafka is an open source software which provides a framework for storing, reading, publishing and analyzing streaming data. By using Kafka’s API and Producer, the data of each message, which contained in a JSON format, is published forward with a given topic. 
The twitter producer python file is named: twitter_producer.py
The data from Kafka, by relevant topic, is then picked up and processed with Spark. Spark is an open-source, distributed processing system used for big data workloads. It utilizes in-memory caching and optimized query execution for fast queries against data of any size.
With Spark, the data can be filtered, analyzed and aggregated in real time. From Spark the data is processed and sent forward for the following cases:

1. The raw data is sent to archive in HDFS. The Hadoop Distributed File System (HDFS) is the primary data storage system used by Hadoop applications. It implements a distributed file system that provides high-performance access to data across highly scalable Hadoop clusters. 
The data in the archive is being saved in JSON format, meaning each tweet and its details, and is partitioned with the date (created_at column).
The relevant python file is named: tweets_to_hdfs_archive.py

2. The raw data receives ‘Data Enrichment’.  Sentiment analysis is the interpretation and classification of emotions (positive, negative and neutral) within text data using text analysis techniques. 
The text of the message is being real-timed cleaned for this purpose. The sentiment analysis is preformed by using NLTK library, which deals with text analysis. It uses VADER dictionary to do so. VADER (Valence Aware Dictionary for Sentiment Reasoning) is a model used for text sentiment analysis that is sensitive to both polarity (positive/negative) and intensity (strength) of emotion. 
The sentiment value for each text is between minus one (negative sentiment text) and one (positive sentiment text). Around zero means that the text is neutral. 

The relevant python file is named: tweets_alerts_spark.py

3. The enriched data is then being sent to HDFS in a Parquet file format, so one will be able to analyze it with Hive\Impala. Parquet is a columnar file format that provides optimizations to speed up queries. Spark SQL provides support for both reading and writing parquet files that automatically capture the schema of the original data.
Parquet is useful for the current tool, because in the end we care less for the user who wrote the message, but do care about aggregated data, such as the count of messages per game console, the sum of followers of the writers, the average of the sentiment at a given day, and more.
After this process, a database and tables are needed to be created in Hive\Impala, with connection to the parquet file. After accomplishing this, the data can be analyzed. In the tool there is python script that does so. 

The relevant python file is named: tweets_to_hdfs_parqet.py
The python file for reading the parquet file in Hive\Impala is named: hive_impala.py

4. The last process is creating alerts of the enriched data. Alerts are our business decision of interesting data. In the tool we have two alerts, which are connected to each other.
The first is that only distinct positive or negative values are being taken, meaning with a value above 0.5 for significant positive sentiment, and less than -0.5 for significant negative sentiment. Also, the writer of the message should have at least 50 followers. So each message of this kind is being sent by Spark forward back to a Kafka consumer, and from there each message is processed and sent into a mySql table. 
Upon the first alert, there is aggregation with a 30 minutes window. Meaning each 30 minutes the collected data is aggregated. It is grouped by the game console type, and there a count of message in that time window, a summing of the followers, and an average of the sentiment. 
There is another consumer that receives this data, and in this case also, the data is inserted into a mySql table. The data in mySql, like the one in Hive\Impala, can be analyzed using SQL queries on the tables. 

The relevant python file are named: mySql_alerts_sentiment.py, mySql_alerts_aggregations.py
