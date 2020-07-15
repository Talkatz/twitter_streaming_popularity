from kafka import KafkaConsumer
import json

# Getting the consumer topic for receiving alerts data from spark
topic_kafka_aggregations = "alerts_aggregations"
consumer_followers = KafkaConsumer(topic_kafka_aggregations)

consumer_followers = KafkaConsumer(topic_kafka_aggregations,
                     bootstrap_servers = ['localhost:9092'],
                     auto_offset_reset ='latest',
                     enable_auto_commit = True,
                     auto_commit_interval_ms = 1000,
                     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

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

mysql_cursor_sum = mysql_conn.cursor()

mysql_create_tbl_sum = '''create table if not exists aggregations_alerts_tbl 
                        (game_console varchar(250),
                         window_start DATETIME,
                         window_end DATETIME,
                         count_messages INT,
                         sum_followers INT,
                         avg_sentiment DECIMAL(3,2))'''
mysql_cursor_sum.execute(mysql_create_tbl_sum)

#  Inserting the values from the consumer messages to the table by type in case of dictionary date
for messageFl in consumer_followers:
    messageFlVal = messageFl.value
    insertVals = ""
    print(messageFlVal)
    for value in messageFlVal.values():
        currentType = str(type(value))
        if 'str' in currentType:
            currentValue = "'" + value + "', "
            insertVals += currentValue
        elif 'dict' in currentType:
            currentValueStrt = value.get('start')
            currentValueStrt = "'" + currentValueStrt + "', "
            insertVals += currentValueStrt
            currentValueEnd = value.get('end')
            currentValueEnd = "'" + currentValueEnd + "', "
            insertVals += currentValueEnd
        elif 'int' in currentType:
            currentValue = "'" + str(value) + "', "
            insertVals += currentValue
        elif 'float' in currentType:
            floatVal = '%.2f' % value
            currentValue = "'" + str(floatVal) + "',"
            insertVals += currentValue
    columns_fl = "game_console, window_start, window_end, avg_sentiment, sum_followers, count_messages"
    insertVals = insertVals[:-2]
    sql_fl = """INSERT INTO %s (%s) VALUES (%s);""" % ('aggregations_alerts_tbl', columns_fl, insertVals)
    print(sql_fl)
    mysql_cursor_sum.execute(sql_fl)

    # Check the data from the table
    mysql_cursor3 = mysql_conn.cursor()
    mysql_cursor3.execute(
        'SELECT game_console, avg_sentiment, sum_followers, count_messages, Date_Format(window_start,"%m/%d/%Y %h:%i:%s") FROM aggregations_alerts_tbl WHERE game_console = \'xbox\';')
    result3 = mysql_cursor3.fetchall()
    print(result3)
