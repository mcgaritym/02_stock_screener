#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 14:07:55 2020

@author: mcgaritym
"""

import twint
import time
import nest_asyncio
import pandas as pd
from datetime import timedelta, date, datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# apply asyncio to resolve capability issues
nest_asyncio.apply()

# create function for scraping tweets and putting into dataframe
def twitter_scraper(company_list):

    # create empty list
    tweet_list = []
    
    # create 1 day time step
    day_delta = timedelta(days=1)
    
    # define start and end date
    start_date = date(2020, 9, 20)
    end_date = date.today()
    
    # loop over company list
    for company in company_list: 
        
        print(company)
        
        # loop over time range in 1 day deltas
        for i in range((end_date - start_date).days):
                        
            # sleep for 5 sec
            time.sleep(6)
            
            # config twint
            c = twint.Config()
            
            # set keyword and other parameters
            c.Search = company
            c.Store_object = True
            c.Limit = 100
            
            # set date(s) window
            date_from = start_date + i*day_delta
            date_to = start_date + (i+1)*day_delta
            c.From = str(date_from)
            c.Until = str(date_to)
            
            # run search and append to list
            twint.run.Search(c)
            tweets = c.search_tweet_list
            for tweet in tweets:
                tweet_list.append(dict({'date': tweet["date"], 'tweet': tweet["tweet"], 'company': company})) 

    # convert list to dataframe
    df = pd.DataFrame(tweet_list, columns=['date', 'tweet', 'company'])
    print(df.groupby('company')['tweet'].count())
    
    # return df 
    return df


def twitter_sentiment(df):
    
    #determine if headlines contain keyword, resample daily, find mean sentiment
    sid = SentimentIntensityAnalyzer()
    df['sentiment_scores'] = df['tweet'].apply(sid.polarity_scores)
    df['sentiment_scores'] = df['sentiment_scores'].apply(lambda x: x['compound'])
    df['date'] = pd.to_datetime(df['date'])
    print()
    df = df.set_index('date')
    df = df[['sentiment_scores', 'company']]
    company_sentiment = df.groupby('company')['sentiment_scores'].resample('1 d').mean().unstack(level=0)
    print(company_sentiment)
    
    # convert to dataframe
    company_sentiment = pd.DataFrame(company_sentiment)
    company_sentiment = company_sentiment.rename(columns=lambda x: x.upper() + '_sentiment')             
    print(company_sentiment)
    
    # print function output
    return company_sentiment


# define list of companies
company_input = ['FACEBOOK', 'AAPLE', 'AMAZON', 'NETFLIX', 'GOOGLE', 'MICROSOFT']

# print function output
df_scraper = twitter_scraper(company_input)
print(df_scraper)

# print function output
df_sentiment = twitter_sentiment(df_scraper)
print(df_sentiment)

# save to csv file
df_sentiment.to_csv('df_twitter_' + '_'.join(company_input) + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)
       
