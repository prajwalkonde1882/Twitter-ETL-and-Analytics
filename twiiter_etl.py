import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "blcCxID4ugjfgnxpR72UqWoLi" 
    access_secret = "WdkLhPjlcUfLFqFYn5EZPlaHO4YqLsq6bqkL2uCBffbn7xx6U9" 
    consumer_key = "1890031620712955905-Jgod4ZFAP9dF6z7CuEPEXJvYhxmXhi"
    consumer_secret = "yii258HzARu4MU0HA6AN4sRhuwKIxlb5Yj366Z1wcp9DL"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )
    tweet_list =[]
    
    for tweet in tweets:
        text =tweet._json["full_text"]
        
        refined_tweet ={"user":tweet.user.screen_name,
                        'text':text,
                        'favorite_count':tweet.favorite_count,
                        'retweet_count':tweet.retweet_count,
                        'created_at':tweet.created_at}
        tweet_list.append(refined_tweet)
        
    df =pd.DataFrame(tweet_list)
    df.to_csv("elonmusk_twitter_data.csv")