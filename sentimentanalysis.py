from google.cloud import storage
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import re
from wordcloud import WordCloud, STOPWORDS
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from langdetect import detect
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

storage_client = storage.Client.from_service_account_json('/Users/augustolimonti/Desktop/finalproject-312215-dce2a96c65e9.json')
bucket = storage_client.get_bucket('bidentwitter')

negative = 0
positive = 0
neutral = 0
count = 0
neg_count = [negative]
pos_count = [positive]
neutral_count = [neutral]
tweet_count = [count]
neg_list = []
pos_list = []

def removeNonAlpabet(s):
    return ''.join([i.lower() for i in s if i.isalpha() or i==' ']).lstrip().rstrip()

def removeNonEnglishWords(s):
    return ''.join([i.lower() for i in s if i in english_words or i==' ']).lstrip().rstrip()

sc = pyspark.SparkContext()
nltk.download('words')
english_words = set(nltk.corpus.words.words())

start_time = time.time()

for text_file in bucket.list_blobs():
    text_file.download_to_filename('tweets.csv')
    file = open('tweets.csv', 'r')
    tweets = file.readlines()
    rdd = sc.parallelize(tweets)
    rdd = rdd.map(lambda x: x.replace("text",""))
    rdd = rdd.map(removeNonAlpabet).map(removeNonEnglishWords).distinct().collect()
    for tweet in rdd:
        tweet = tweet.replace('rt','')
        score = SentimentIntensityAnalyzer().polarity_scores(tweet)
        neg = score['neg']
        neu = score['neu']
        pos = score['pos']
        if pos > neg:
            pos_list.append(tweet)
            positive += 1
            pos_count.append(positive)
            neg_count.append(negative)
            neutral_count.append(neutral)
        elif neg > pos:
            neg_list.append(tweet)
            negative += 1
            neg_count.append(negative)
            pos_count.append(positive)
            neutral_count.append(neutral)
        else:
            neutral += 1
            neutral_count.append(neutral)
            pos_count.append(positive)
            neg_count.append(negative)
        count += 1
        tweet_count.append(count)

print("--- %s seconds ---" % (time.time() - start_time))

print("total tweets: ", count)
print("positive tweets: ",positive)
print("negative tweets: ", negative)
print("neutral tweets: ",neutral)

plt.plot(tweet_count, pos_count,
             tweet_count, neg_count,
             tweet_count, neutral_count)
plt.xlabel('Number of Tweets')
plt.ylabel('Sentiment')
plt.title("Number of Tweets vs Sentiment")
plt.show()

def cloud(df):
    text = df.tweets[0]
    text = " ".join(review for review in df.tweets)
    stopwords = set(STOPWORDS)
    wordcloud = WordCloud(background_color="white",
     max_words=3000,
     stopwords=stopwords,
     repeat=True).generate(text)

    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()

df = pd.DataFrame({"tweets":neg_list})
df.head()
cloud(df)

df = pd.DataFrame({"tweets":pos_list})
df.head()
cloud(df)
