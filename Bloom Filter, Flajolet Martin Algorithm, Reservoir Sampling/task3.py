import sys
import json
from time import time
import math
import random
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener

num_tweets = 100
frequency = 3
out_file_path = sys.argv[2]

class tweet_analysis(StreamListener):
    def on_error(self, status):
        print("Error: ", str(status))
       
    def on_status(self, status):
        global num_t
        global all_t
        global out_file
        frequent_tags = dict()
        curr_HashTags = status.entities.get("hashtags")
       
        if(len(curr_HashTags) > 0):
            num_t += 1
        
            if(num_t < num_tweets):
                all_t.append(status)
            else:
                randid = random.randint(0, num_t)
            
                if(randid < num_tweets-1):
                    all_t[randid] = status

               
                for tweet in all_t:
                    hashtags = tweet.entities.get("hashtags")
                
                    for ht in hashtags:
                        content = ht["text"]
                        if(content not in frequent_tags.keys()):
                            frequent_tags[content] = 1
                        else:
                            frequent_tags[content] =  frequent_tags[content] + 1

                
                sorted_tweets = sorted(frequent_tags.items(), key=lambda x: (-x[1], x[0]))
                if(len(sorted_tweets) >= frequency):
                    top3tweets = sorted_tweets[0:frequency]
                else:
                    top3tweets = sorted_Tweets

                
                out = "The number of tweets with tags from the beginning: " + str(num_t)+"\n"
                for tweet in top3tweets:
                    out += (tweet[0]+" : "+str(tweet[1])+"\n")

                out_file.write(out)
                out_file.write('\n')
                out_file.flush()
                
                return
        
       

#Get consumerKey, consumerSecretKey, accessToken and accessSecretKey by following the application procedure to receive API Key from Twitter developers page
consumerKey =  "Use your consumerKey"
consumerSecretKey = "Use your consumerSecretKey"
accessToken =  "Use your accessToken"
accessSecretKey = "Use your accessSecretKey"

authentication = tweepy.OAuthHandler(consumer_key=consumerKey, consumer_secret=consumerSecretKey)
authentication.set_access_token(key=accessToken, secret=accessSecretKey)

tweetAPI = tweepy.API(authentication)

num_t = 0
all_t = list()
out_file = open(out_file_path, "w", encoding="utf-8")

tweetStream = Stream(auth=authentication, listener = tweet_analysis())
tweetStream.filter(track=['covid19'])
