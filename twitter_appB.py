"""
    This script connects to Twitter Streaming API, gets tweets with '#' and
    forwards them through a local connection in port 9009. That stream is
    meant to be read by a spark app for processing. Both apps are designed
    to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --name twitter -p 9009:9009 python bash

    and inside the docker:

        pip install -U git+https://github.com/tweepy/tweepy.git
        python twitter_app.py

    (we don't do pip install tweepy because of a bug in the previous release)
    For more instructions on how to run, refer to final slides in tutorial 8

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Author: Tilemachos Pechlivanoglou

"""

# from __future__ import absolute_import, print_function

import socket
import sys
import json


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream

# Replace the values below with yours
consumer_key="hPa3JWaL2uG6MMI0iPB9Pln5n"
consumer_secret="sJvP8mAtGUfwLLJ07ah9zoXXNdKDCzVDrHseQRpeozR22ZOV8g"
access_token="714640046888984577-CkHiYDE434EWtgcPeRUfqmRIHeGOhoC"
access_token_secret="LNB7U0bStQ1eSHQzg1iaYrXqhjL7C5vdJbsZ0iJGyZVH1"


class TweetListener(StreamListener):
    """ A listener that handles tweets received from the Twitter stream.

        This listener prints tweets and then forwards them to a local port
        for processing in the spark app.
    """
    #modified on_status function, to recieve full tweets instead of truncated ones
    def on_status(self, status):
        """When a tweet is received, forward it"""
        print ("------------------------------------------")
        if hasattr(status, "retweeted_status"):  # Check if Retweet
            try:
                sent_tweet = status.retweeted_status.extended_tweet["full_text"]
                print(status.retweeted_status.extended_tweet["full_text"])
                conn.send(str.encode(sent_tweet + '\n'))
            except AttributeError:
                print(status.retweeted_status.text)
        else:
            try:
                not_retweet = status.extended_tweet["full_text"]
                print(status.extended_tweet["full_text"])
                conn.send(str.encode(not_retweet + '\n'))
            except AttributeError:
                print(status.text)

        return True

    def on_error(self, status):
        print(status)



# ==== setup local connection ====

# IP and port of local machine or Docker
TCP_IP = socket.gethostbyname(socket.gethostname()) # returns local IP
TCP_PORT = 9009

# setup local connection, expose socket, listen for spark app
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")

# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting getting tweets.")


# ==== setup twitter connection ====
listener = TweetListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, listener)

# setup search terms
track = ['#hawks', '#brooklynNets', '#bucks', '#phoenixSuns', '#laclippers', '#miamiheat', '#knicks', '#celtics','#wizards', '#hornets'
         '#apple', '#microsoft', '#google', '#Android', '#Nokia', '#amazon', '#google', '#sony', '#tencent', '#facebook'
         '#euro2020', '#Denmark', '#Belgium', '#Eriksen', '#Austria', '#Netherlands', '#Portugal', '#Germany', '#France', '#Hungary'
         '#Science', '#SpaceX' '#Tesla', '#DataScience', '#NASA', '#climatechange', '#moon', '#physics', '#chemistry', '#nature'
         '#hamilton', '#vettel', '#perez', '#gasly', '#leclerc', '#norris', '#alonso', '#bottas', '#raikkonen', '#schumacher'
        ]
language = ['en']
# locations = [-130,-20,100,50]

# get filtered tweets, forward them to spark until interrupted
try:
    stream.filter(track=track, languages=language)
except KeyboardInterrupt:
    s.shutdown(socket.SHUT_RD)

