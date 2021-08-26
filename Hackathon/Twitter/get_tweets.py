import tweepy
import pandas
import json # The API returns JSON formatted text
import os
import sys
import codecs
from minio import Minio
from minio.error import S3Error

# Create a new topic
os.system('start /MIN cmd /c python create_topic.py tweets')

access_token = "1421233278707585026-s1UbXFX11FADjq953nUgcsazNpLpkM"
access_token_secret =  "WTyWkTqVBDcDpBVco4dvvA40HqgCRNRDk9zRDw6DzQB5i"
consumer_key =  "CtqriL3pm9W5KPmxLlfNRll7E"
consumer_secret =  "twRukl7CyAfIXArBZgy5PFZuZ2gjfNEoyoeo2gZNALQAEwcAnr"

# Pass OAuth details to tweepy's OAuth handler
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)



TRACKING_KEYWORDS = [sys.argv[1]]
OUTPUT_FILE = "tweets.txt"
TWEETS_TO_CAPTURE = int(sys.argv[2])

class MyStreamListener(tweepy.StreamListener):

    # Collect streaming tweets and output to a file
    def __init__(self, api=None):
        super(MyStreamListener, self).__init__()
        self.num_tweets = 0
        self.file = open(OUTPUT_FILE, "w")

    def on_status(self, status):
        tweet = status._json
        self.file.write( json.dumps(tweet) + '\n' )
        self.num_tweets += 1
        try:
        # Stop streaming when limit is reached
            if self.num_tweets < TWEETS_TO_CAPTURE:
                if self.num_tweets % 100 == 0:
                    print('Numer of tweets captured so far: {}'.format(self.num_tweets))
                return True
            else:
                return False
        except:
            self.file.close()

    def on_error(self, status):
        print(status)


# Initialize Stream listener
l = MyStreamListener()

# Create your Stream object with authentication
stream = tweepy.Stream(auth, l)

# Filter Twitter Streams to capture data by the keywords:
stream.filter(track=TRACKING_KEYWORDS)

import matplotlib.pyplot as plt
# Initialize empty list to store tweets
tweets_data = []
OUTPUT_FILE = "tweets.txt"
# Open connection to file
with open(OUTPUT_FILE, "r") as tweets_file:
    # Read in tweets and store in list
    for line in tweets_file:
        tweet = json.loads(line)
        tweets_data.append(tweet)

df = pandas.DataFrame(tweets_data, columns=['created_at','lang', 'text', 'source'])
df['created_at'] = pandas.to_datetime(df.created_at)
# Regular expression to get only what's between HTML tags: > <
df['source'] = df['source'].str.extract('>(.+?)<', expand=False).str.strip()
print(df.head())

dphtml = "<link rel='stylesheet' href='/static/css-table.css'><a href='http://localhost:5000/static/twitter.html'>&#8592 Back</a><a id='landing' href='http://localhost:5000/'>Home-Screen</a>"
dphtml += df.to_html()

#write html to file
text_file = open("index.html", "w",encoding='utf-8')
text_file.write(dphtml)
text_file.close()

# create filter for most popular languages
lang_mask = (df.lang == 'en') | (df.lang == 'ca') | (df.lang == 'fr') | (df.lang == 'es')

# create a filter for most popular sources
source_mask = (df.source == 'Twitter for iPhone') | (df.source == 'Twitter for Android')\
    | (df.source == 'Twitter Web Client') | (df.source == 'Twitter for iPad') \
    | (df.source == 'Twitter Lite') | (df.source == 'Tweet Old Post')


(df[lang_mask & source_mask].groupby(['source','lang']) # apply filter/groupby
 .size() # get count of tweets per source/lang
 .unstack() # unstack to create new DF 
 .fillna(0) # fill NaN with 0
 .plot(kind='bar', figsize=(14,7), title='Tweets by source and language') # plot graph
)

plt.savefig('pic.png')
plt.show()

def minio_upload():
    # Create a client with the MinIO server playground, its access key and secret key.
    client = Minio(
        "play.min.io",
        access_key="Q3AM3UQ867SPQQA43P2F",
        secret_key="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    )

    # Create new bucket 'tweets' if not exists
    found = client.bucket_exists("tweets")
    if not found:
        client.make_bucket("tweets")
    else:
        print("Bucket 'tweets' already exists")


    client.fput_object(
        "tweets", "tweets.txt", "./tweets.txt",num_parallel_uploads = 4
    )
    print(
        "tweet.txt has been uploaded to cloud"
    )
    client.fput_object(
        "tweets", "pic.png", "./pic.png",num_parallel_uploads = 4
    )
    print(
        "Graph has been uploaded to cloud"
    )
    client.fput_object(
        "tweets", "index.html", "./index.html",num_parallel_uploads = 4
    )
    print(
        "DataFrame has been uploaded to cloud"
    )

try:
     minio_upload()
except S3Error as exc:
    print("error occurred.", exc)

