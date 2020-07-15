import os
from pyhive import hive
import pyarrow as pa

# Defining the Hive for connection
hdfs_host = 'localhost'
hdfs_port = 9870
hive_port = 10000
hive_username = 'hdfs'
hive_password = 'password'
hive_database = 'twitter_db'
hive_mode = 'CUSTOM'

hive_cnx = hive.Connection(
  host = hdfs_host,
  port = hive_port,
  username = hive_username,
  password = hive_password,
  database = hive_database,
  auth = hive_mode)

# Creating the database in Hive
cursor = hive_cnx.cursor()
cursor.execute('''CREATE DATABASE IF NOT EXISTS twitter_db''')
cursor.close()

# Creating the table with partitions in Hive
cursor = hive_cnx.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS twitter_db.twitter_data_tbl 
                  (user_name string, 
                   user_id string,
                   text string,
                   hashtags string,
                   followers int,
                   retweets int,
                   sentiment float,
                   time string)
                   PARTITIONED BY (game_console string, created_at string)
                   stored as parquet''')
cursor.close()

# Connecting with the parquet source
source_path = '/.../tweets_parquet'

fs = pa.hdfs.connect(
  host='localhost',
  port=8020,
  user='hdfs',
  kerb_ticket=None,
  driver='libhdfs',
  extra_conf=None)

fs.ls(source_path)

# Defining the partitions
consoles_groupes = ['playstation', 'xbox', 'playstation_and_xbox']
#print(consoles_groupes)

parquet_path = '/.../tweets_parquet/'

# Getting all the current partitons, which is the game console and the date
partitions = []
for console in consoles_groupes:
    sub_partiton = parquet_path + '/game_console={}'.format(console)
    for dateString in fs.ls(sub_partiton):
        idx = dateString.rfind('=')
        currentDate = dateString[idx + 1:]
        partitions.append((console, currentDate))

# print(partitions)

# Altering the table to hold the partitons
cursor = hive_cnx.cursor()
for console, date in partitions:
    cursor.execute('''ALTER TABLE twitter_db.twitter_data_tbl         
                      ADD IF NOT EXISTS PARTITION (game_console="{}", created_at="{}")'''.format(console, date))
cursor.close()

# Adding the partitons
db_path = '/.../tweets_parquet'
cursor = hive_cnx.cursor()
for console, date in partitions:
    try:
        cursor.execute('''LOAD DATA INPATH 'hdfs://localhost:8020{2}/game_console={0}/created_at={1}'   
                          INTO TABLE twitter_db.twitter_data_tbl 
                          PARTITION (game_console='{0}', created_at='{1}')'''.format(console, date, db_path))
    except:
        pass
cursor.close()