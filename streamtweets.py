import tweepy
from google.cloud import pubsub_v1
from tweepy.streaming import StreamListener
import json

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("finalproject-312215", "biden")

CONSUMER_KEY = 'btyqBzjSyZ7SqaEOGKsG4uMWd'
CONSUMER_SECRET = '1j6P2I0UuxSca47G1OjJe9ieFGUkdNFWcW5q9N7OKrrujsM7Lw'
ACCESS_TOKEN = '936675231065833472-Wxt6JddTwn8C8lkHkdFQPYcIUFkqQ3q'
ACCESS_TOKEN_SECRET = 'XvPqGZmRg74UbEdiT8QNcUjyJQqxTYZpdi6P7vZ3OdU6J'
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)
key = ["biden"]

def push(tweet):
    data_tweet = {"id": tweet["id"], "lang": tweet["lang"]}
    if "extended_tweet" in tweet:
        data_tweet["text"] = tweet["extended_tweet"]["full_text"]
    elif "full_text" in tweet:
        data_tweet["text"] = tweet["full_text"]
    else:
        data_tweet["text"] = tweet["text"]

    if data_tweet["lang"] == "en":
        publisher.publish(topic_path, data=json.dumps({"text":data_tweet["text"],
        }).encode("utf-8"), tweet_id=str(data_tweet["id"]).encode("utf-8"))

class Listener(StreamListener):
    def __init__(self):
        super(Listener, self).__init__()

    def on_status(self, data):
        print('tweet streamed')
        push(data._json)
        return True

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False

listen = Listener()
stream = tweepy.Stream(auth, listen, tweet_mode='extended')
stream.filter(track=key)
