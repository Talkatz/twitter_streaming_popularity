import tweepy
from kafka import KafkaClient, KafkaProducer
import json
import datetime

auth = tweepy.OAuthHandler('from twitter')
auth.set_access_token('from twitter')
api = tweepy.API(auth)
topic_hdfsArchive = 'full_tweets_to_archive'
topic_tweets_analysis = 'full_tweets_for_analysis'
topic_tweets_parquet = 'full_tweets_to_parquet'


# A class to handle the incoming tweets, and send them with a Kafka producer forward
class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        try:
            if status.retweeted:
                return
            record = {}
            record['user_name'] = status.user.screen_name
            user_id = str(status.user.id)
            record['user_id'] = user_id
            full_dt = str(status.created_at)
            date_time_obj = datetime.datetime.strptime(full_dt, '%Y-%m-%d %H:%M:%S')
            final_date = date_time_obj.date()
            record['created_at'] = final_date
            record['text'] = status.text
            text = status.text
            currentList = ""
            tagsStr = ""
            # Type of game console for the partitions
            if any(strCheck.lower() in text for strCheck in playstationListLower):
                currentList = "playstation"

            if any(strCheck.lower() in text for strCheck in xboxListLower):
                if currentList == "playstation":
                    currentList = "playstation_and_xbox"
                else:
                    currentList = "xbox"
            # Hashtags spliting
            tagsSet = {tag.strip("#") for tag in text.split() if tag.startswith("#")}
            tagsList = [str(s) for s in tagsSet]
            tagsStr = ",".join(tagsList)
            record['game_console'] = currentList
            record['hashtags'] = tagsStr
            record['followers'] = status.user.followers_count
            record['retweets'] = status.retweet_count

            json_record = json.dumps(record, default=dateConverter).encode('utf-8')
            if currentList != "":
                print(json_record)
                producer.send(topic=topic_hdfsArchive, value=json_record)
                producer.send(topic=topic_tweets_analysis, value=json_record)
                producer.send(topic=topic_tweets_parquet, value=json_record)

        except Exception as e:
            print(str(e))

    def on_error(self, status):
        print(status)


# A convertor for handling date in json-dump function
def dateConverter(o):
    if isinstance(o, datetime.date):
        return o.__str__()


# Lists for the tagging and listening to twitter streaming messages
playstationList = ["playstation", "PS2", "PS3", "PS4", "PS5"]
playstationListLower = [x.lower() for x in playstationList]
xboxList = ["xbox", "x-box"]
xboxListLower = [x.lower() for x in xboxList]
tweeterSearchList = playstationList + xboxList

# Kafka port and producer
kafka = ("localhost:9092")
producer = KafkaProducer(bootstrap_servers=kafka)
# Twitter stream settings
stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=tweeterSearchList, languages=['en'])
