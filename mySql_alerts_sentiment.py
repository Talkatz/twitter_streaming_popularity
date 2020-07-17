from kafka import KafkaConsumer
import json
topic_kafka_sentiment = "alerts_sentiments"

# Getting the consumer topic for receiving alerts data from spark
consumer_sentiment = KafkaConsumer(topic_kafka_sentiment)

consumer_sentiment = KafkaConsumer(topic_kafka_sentiment,
                     bootstrap_servers = ['localhost:9092'],
                     auto_offset_reset ='latest', #'earliest',
                     enable_auto_commit = True,
                     auto_commit_interval_ms = 1000,
                     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Creating the MySql database
#import os
#os.system("mysql -unaya -pnaya -e 'create database if not exists twitter_db'")

import mysql.connector as mc

#  MySql connector
mysql_host = 'localhost'
mysql_port = 3306
mysql_database_name = 'twitter_db'
mysql_username = 'userName'
mysql_password = 'userPassword'

mysql_conn = mc.connect(
    user = mysql_username,
    password = mysql_password,
    host = mysql_host,
    port = mysql_port,
    database = mysql_database_name)

mysql_cursor_sentm = mysql_conn.cursor()

# Creating the alerts table
mysql_create_tbl_sentm = '''create table if not exists sentiment_alerts_tbl 
                        (user_name varchar(250),
                         user_id varchar(250) ,  
                         created_at varchar(250),
                         text varchar(250),
                         game_console INT,
                         hashtags varchar(250),
                         followers int,
                         retweets int,
                         sentiment FLOAT,
                         timeStamp varchar(250))'''
mysql_cursor_sentm.execute(mysql_create_tbl_sentm)

# Inserting the values from the consumer messages to the table
for message in consumer_sentiment:
    message = message.value
    lst = ['user_name','user_id', 'created_at','text','game_console', 'hashtags', 'followers','retweets','sentiment']
    alerts = {k:v for k,v in message.items() if k in lst}
    columns = ', '.join(str(x).replace('/', '_')  for x in alerts.keys())
    values = ', '.join("'" + str(x).replace("'", "''") + "'" for x in alerts.values())
    values = values.replace('/', '_')
    sql = """INSERT INTO %s (%s) VALUES (%s);""" % ('sentiment_alerts_tbl', columns, values)
    mysql_cursor_sentm.execute(sql)

# Check the data from the table
mysql_cursor2 = mysql_conn.cursor()
mysql_cursor2.execute('SELECT * FROM sentiment_alerts_tbl;')
result2 = mysql_cursor2.fetchall()
print(result2)
