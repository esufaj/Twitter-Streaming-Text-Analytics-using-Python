"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nltk import tokenize 

import warnings
warnings.filterwarnings("ignore", message="The twython library has not been installed. ")

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)

# reminder - lambda functions are just anonymous functions in one line:
#
#   words.flatMap(lambda line: line.split(" "))
#
# is exactly equivalent to
#
#    def space_split(line):
#        return line.split(" ")
#
#    words.filter(space_split)

# split each tweet into words

track = ['#hawks', '#brooklynNets', '#bucks', '#phoenixSuns', '#laclippers'
         '#apple', '#microsoft', '#google', '#Android', '#Nokia'
         '#euro2020', '#Denmark', '#Belgium', '#Eriksen', '#Austria', '#Netherlands'
         '#Science', '#SpaceX' '#Tesla', '#DataScience', '#NASA', '#climatechange'
        ]

def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def tweet_filter(line):
    track_lower = (x.lower() for x in track)
    exists = False 
    if any(filtered in line for filtered in track_lower):
        exists = True
    
    return exists 

filtered_stream = dataStream.filter(lambda x: tweet_filter(x.lower()))
# filtered_stream.pprint()
# Clean the tweets
# tweets = dataStream.map(lambda line: clean_tweet(line))

def get_sentiment(line):
    nba = ['#Hawks', '#BrooklynNets', '#Bucks', '#PhoenixSuns', '#LaClippers']   
    tech = ['#Apple', '#Microsoft', '#Google', '#Android', '#Nokia']
    soccer = ['#euro2020', '#Denmark', '#Belgium', '#Eriksen', '#Austria', '#Netherlands']
    science = ['#Science', '#SpaceX' '#Tesla', '#DataScience', '#NASA', '#climatechange']

    nba = (x.lower() for x in nba)
    tech = (x.lower() for x in tech)
    soccer = (x.lower() for x in soccer)
    science = (x.lower() for x in science)

    line = line.lower()

    topic = 0
    tweet = None

    # print(line)
    if any (topics in line for topics in nba):
        topic = 1
        tweet = line
        # print(line + ", " + str(topic))
    elif any (topics in line for topics in tech):
        topic = 2
        tweet = line
        # print(line + ", " + str(topic))
    elif any (topics in line for topics in soccer):
        topic = 3
        tweet = line
        # print(line + ", " + str(topic))
    elif any (topics in line for topics in science):
        topic = 4
        tweet = line
      
    topic_polarity = {}
    

    if (tweet is not None):
        tweet = clean_tweet(tweet)

        sid = SIA()
        topic_polarity = sid.polarity_scores(tweet)

        pol = None
        
        if topic_polarity['compound'] > 0.2:
            pol = "pos"
        elif topic_polarity['compound'] < -0.2:
            pol = "neg"
        else:
            pol = "neut"



    return (topic, pol) 

analysis_tweet = filtered_stream.map(lambda line: get_sentiment(line))

# analysis_tweet.pprint()

# # #filter hashtags to our own
# hashtag_track = hashtags.filter(lambda h: h in track)

# # # map each hashtag to be a pair of (hashtag,1)
hashtag_counts = analysis_tweet.map(lambda x: (x, 1))


# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    print("new value: " +str(new_values))
    print("total sum: " +str(total_sum))
    return sum(new_values) + (total_sum or 0)

# # do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)
hashtag_totals.pprint()

# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # sort counts (desc) in this time instance and take top 10
        sorted_rdd = rdd.sortBy(lambda x:x[1], False)
        top10 = sorted_rdd.take(5)

        # # print it nicely
        # for tag in top10:
        #     print('{:<40} {}'.format(tag[0], tag[1]))
        
        # send_df_to_dashboard(top10)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# def send_df_to_dashboard(input):
# 	# initialize and send the data through REST API
# 	url = 'http://dashboard:5001/updateData'
# 	request_data = {'data': str(input)}
# 	response = requests.post(url, data=request_data)

# do this for every single interval
hashtag_totals.foreachRDD(process_interval)



# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()