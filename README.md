# ** Twitter Real-Time Streaming Text Analytics **

### In this application I perform real time text analytics of Twitter data. Using Apache Spark, AJAX, NLTK, and Tweepy, I am able to perform a number of important operations of data that is extracted from the Tiwtter API.

#### ** Operations that are performed in this app are: **

    1) Identifying Trends - Certain hashtags are tracked in real time as tweets become available and organized based on popularity and occurence. These trends are then plotted again in real time on a graph.
    
    2) Sentiment Analysis - As in the fields of computational linguistics and natural language processing, I perform a sentiment analysis on tweets in order to determine the contextual polarity/emotional reaction of the user and tweet. A ranking of positive, negative pr neutral is given to the tweets using a scoring system and then plotted on a graph in real time.



** How to run 1: **
1) open 3 terminals in file location
2) For terminal 1: 
    - docker run -it -v $PWD:/app --name twitter -w /app python bash
    – pip install -U tweepy
    – python twitter_app.py
3) For terminal 2:
    - docker run -it -v $PWD:/app --name dashboard -w /app python bash
    - python app.py
4) For terminal 3:
    – docker run -it -v $PWD:/app --link twitter:twitter --link dashboard:dashboard eecsyorku/eecs4415
    – spark-submit spark_app.py

** How to run 2: ** 
1) open 3 terminals in file location
2) For terminal 1: 
    - docker run -it -v $PWD:/app --name twitter -w /app python bash
    – pip install -U tweepy
    – python twitter_app.py
3) For terminal 2:
    - docker run -it -v $PWD:/app --name dashboard -w /app python bash
    - python app.py
4) For terminal 3:
    – docker run -it -v $PWD:/app --link twitter:twitter --link dashboard:dashboard eecsyorku/eecs4415
    - pip install nltk
    - python (go into python bash)
    - import nltk
    - nltk.download('vader_lexicon')
    - exit
    – spark-submit spark_app.py


** Note: ** 
If one is re-running a docker image you can restart old docker image by:
1) Checking for list of docker: docker container list -a
2) docker run "nameofDockerImage"
3) docker exec -it "nameofDockerImage" bash

In case there are errors installing nltk libraries:
apt-get update
apt-get install gcc
apt-get install python-dev python3-dev
apt-get install python-nltk
