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


#list of hashtags
track = ['#hawks', '#brooklynNets', '#bucks', '#phoenixSuns', '#laclippers', '#miamiheat', '#knicks', '#celtics','#wizards', '#hornets'
         '#apple', '#microsoft', '#google', '#Android', '#Nokia', '#amazon', '#google', '#sony', '#tencent', '#facebook'
         '#euro2020', '#Denmark', '#Belgium', '#Eriksen', '#Austria', '#Netherlands', '#Portugal', '#Germany', '#France', '#Hungary'
         '#Science', '#SpaceX' '#Tesla', '#DataScience', '#NASA', '#climatechange', '#moon', '#physics', '#chemistry', '#nature'
         '#hamilton', '#vettel', '#perez', '#gasly', '#leclerc', '#norris', '#alonso', '#bottas', '#raikkonen', '#schumacher'
        ]

#function that cleans the datastream from links and all signs that are not words
def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

#filter only for tweets that have the hashtags in track in them
def tweet_filter(line):
    track_lower = (x.lower() for x in track)
    exists = False 
    if any(filtered in line for filtered in track_lower):
        exists = True
    
    return exists 

#apply the tweet filter function to all the the lines read in by the datastream
filtered_stream = dataStream.filter(lambda x: tweet_filter(x.lower()))

#gets the sentiment of each tweet and assigns the topic and also its sentiment 
def get_sentiment(line):
    nba = ['#Hawks', '#BrooklynNets', '#Bucks', '#PhoenixSuns', '#LaClippers', '#miamiheat', '#knicks', '#celtics','#wizards', '#hornets']   
    tech = ['#Apple', '#Microsoft', '#Google', '#Android', '#Nokia', '#amazon', '#google', '#sony', '#tencent', '#facebook']
    soccer = ['#euro2020', '#Denmark', '#Belgium', '#Eriksen', '#Austria', '#Netherlands', "#Portugal", "#Germany", "#France", "#Hungary"]
    science = ['#Science', '#SpaceX' '#Tesla', '#DataScience', '#NASA', '#climatechange','#moon', '#physics', '#chemistry', '#nature']
    formula1= ['#hamilton', '#vettel', '#perez', '#gasly', '#leclerc', '#norris', '#alonso', '#bottas', '#raikkonen', '#schumacher']

    nba = (x.lower() for x in nba)
    tech = (x.lower() for x in tech)
    soccer = (x.lower() for x in soccer)
    science = (x.lower() for x in science)
    formula1 = (x.lower() for x in formula1)

    line = line.lower()

    topic = "other"
    tweet = None

    #when the tweet read in has one of the hashtags assigned to the topic, assign the topic name
    if any (topics in line for topics in nba):
        topic = "nba"
        tweet = line
        # print(line + ", " + str(topic))
    elif any (topics in line for topics in tech):
        topic = "tech"
        tweet = line
        # print(line + ", " + str(topic))
    elif any (topics in line for topics in soccer):
        topic = "soccer"
        tweet = line
        # print(line + ", " + str(topic))
    elif any (topics in line for topics in science):
        topic = "science"
        tweet = line
    elif any (topics in line for topics in formula1):
        topic = "formula1"
        tweet = line


    topic_polarity = {}
    
    #get the polarity of the tweet and clean it
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



    return ((topic, pol),1) 

#apply the get_sentiment function to all tweets that were already filtered out
analysis_tweet = filtered_stream.map(lambda line: get_sentiment(line))


# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# # do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = analysis_tweet.updateStateByKey(aggregate_tags_count)
hashtag_totals.pprint()


previous ={}
# process a single time interval, calculate the avg sentiment of all the topics and send it to the dashboard
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # get the max 15 different combination of sentiments
        sentiment = rdd.take(15)
        
        accumlated = {}
        
        nba_positive = 0
        nba_negative = 0
        nba_neutral = 0
        nba_total = 0
        nba_occ = 0
        nba_avg = 0

        for tag in sentiment:
            if tag[0][0] == "nba":
                if tag[0][1] == "pos":
                    nba_positive = tag[1]
                elif tag[0][1] == "neg":
                    nba_negative = tag[1]
                elif tag[0][1] == "neut":
                    nba_neutral = tag[1]
        
        nba_total = nba_positive - nba_negative
        nba_occ = nba_positive + nba_negative + nba_neutral
        if (nba_occ == 0):
            nba_avg = (nba_total*1.0)/1.0
        else: 
            nba_avg = (nba_total*1.0)/(nba_occ*1.0)

        
        tech_positive = 0
        tech_negative = 0
        tech_neutral = 0
        tech_total = 0
        tech_occ = 0
        tech_avg = 0

        for tag in sentiment:
            if tag[0][0] == "tech":
                if tag[0][1] == "pos":
                    tech_positive = tag[1]
                elif tag[0][1] == "neg":
                    tech_negative = tag[1]
                elif tag[0][1] == "neut":
                    tech_neutral = tag[1]
        
        tech_total = tech_positive - tech_negative
        tech_occ = tech_positive + tech_negative + tech_neutral
        if (tech_occ == 0):
            tech_avg = (tech_total*1.0)/1.0
        else: 
            tech_avg = (tech_total*1.0)/(tech_occ*1.0)

        
        science_positive = 0
        science_negative = 0
        science_neutral = 0
        science_total = 0
        science_occ = 0
        science_avg = 0

        for tag in sentiment:
            if tag[0][0] == "science":
                if tag[0][1] == "pos":
                    science_positive = tag[1]
                elif tag[0][1] == "neg":
                    science_negative = tag[1]
                elif tag[0][1] == "neut":
                    science_neutral = tag[1]
        
        science_total = science_positive - science_negative
        science_occ = science_positive + science_negative + science_neutral
        if (science_occ == 0):
            science_avg = (science_total*1.0)/1.0
        else: 
            science_avg = (science_total*1.0)/(science_occ*1.0)

        soccer_positive = 0
        soccer_negative = 0
        soccer_neutral = 0
        soccer_total = 0
        soccer_occ = 0
        soccer_avg = 0

        for tag in sentiment:
            if tag[0][0] == "soccer":
                if tag[0][1] == "pos":
                    soccer_positive = tag[1]
                elif tag[0][1] == "neg":
                    soccer_negative = tag[1]
                elif tag[0][1] == "neut":
                    soccer_neutral = tag[1]
        
        soccer_total = soccer_positive - soccer_negative
        soccer_occ = soccer_positive + soccer_negative + soccer_neutral
        if (soccer_occ == 0):
            soccer_avg = (soccer_total*1.0)/1.0
        else: 
            soccer_avg = (soccer_total*1.0)/(soccer_occ*1.0)

        f1_positive = 0
        f1_negative = 0
        f1_neutral = 0
        f1_total = 0
        f1_occ = 0
        f1_avg = 0

        for tag in sentiment:
            if tag[0][0] == "formula1":
                if tag[0][1] == "pos":
                    f1_positive = tag[1]
                elif tag[0][1] == "neg":
                    f1_negative = tag[1]
                elif tag[0][1] == "neut":
                    f1_neutral = tag[1]
        
        f1_total = f1_positive - f1_negative
        f1_occ = f1_positive + f1_negative + f1_neutral
        if (f1_occ == 0):
            f1_avg = (f1_total*1.0)/1.0
        else: 
            f1_avg = (f1_total*1.0)/(f1_occ*1.0)


        accumlated["nba", nba_avg] = nba_occ
        accumlated["tech", tech_avg] = tech_occ
        accumlated["science", science_avg] = science_occ
        accumlated["soccer", soccer_avg] = soccer_occ
        accumlated["formula1", f1_avg] = f1_occ

        for key, value in accumlated.items():
            print(key[0], key[1], value)

        send_df_to_dashboard(accumlated)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def send_df_to_dashboard(input):
	# initialize and send the data through REST API
	url = 'http://dashboard:5001/updateData'
	request_data = {'data': str(input)}
	response = requests.post(url, data=request_data)

# do this for every single interval
hashtag_totals.foreachRDD(process_interval)



# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()