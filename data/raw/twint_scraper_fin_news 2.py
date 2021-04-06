#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 14:07:55 2020

@author: mcgaritym
"""

import twint
import time
import pandas as pd
from datetime import timedelta, date, datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import nest_asyncio
import asyncio

# apply asyncio to resolve capability issues
nest_asyncio.apply()

# create function for scraping tweets and putting into dataframe
def twitter_scraper(company_list):
    
    # loop over company list
    for company in company_list: 
        
        # create 1 day time step
        day_delta = timedelta(days=1)
        
        # define start and end date
        start_date = date(2018, 1, 1)
        end_date = date.today()
        
        # create empty list
        tweet_list = []
        
        print(company)
        
        # loop over time range in 1 day deltas
        for i in range((end_date - start_date).days):
               
            try: 
                # print i to see status
                print(i)
                
                # config twint
                c = twint.Config()
                
                # set keyword and other parameters
                c.Search = 'from:' + company
                c.Store_object = True
                c.Limit = 150
                
                # set date(s) window
                date_from = start_date + i*day_delta
                date_to = start_date + (i+1)*day_delta
                c.From = str(date_from)
                c.Until = str(date_to)
                
                # run search and append to list
                twint.run.Search(c)
                
                # sleep for 10 sec
                time.sleep(5)
                
                # convert to list and append to larger empty list
                tweets = c.search_tweet_list
                #print(tweets)
                if tweets != None:
                    for tweet in tweets:
                        tweet_list.append(dict({'date': tweet["date"], 'tweet': tweet["tweet"], 'company': company}))
                else:
                    continue
            except asyncio.TimeoutError:
                print('Timeout, skipped')
                # sleep for 10 sec
                time.sleep(5)
                continue
            
            except TimeoutError:
                print('Timeout, skipped')
                # sleep for 10 sec
                time.sleep(5)
                continue
            
        # print tweet list
        #print(tweet_list)
            
        # convert list to dataframe
        df = pd.DataFrame(tweet_list, columns=['date', 'tweet', 'company'])
        #print(df.groupby('company')['tweet'].count())
        print(df.head())
        
        # save to csv
        df.to_csv('df_twitter_scrape_FIN_NEWS_' + company + '_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=True)

    # return df 
    #return df


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
company_input = ['FortuneMagazine', 'MorningstarInc', 'FinancialTimes', 'FoxBusiness']

# print function output
df_scraper = twitter_scraper(company_input)
print(df_scraper)
# save to csv file
#df_scraper.to_csv('df_twitter_scrape_FIN_NEWS_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=True)

# # print function output
# df_sentiment = twitter_sentiment(df_scraper)
# print(df_sentiment)
# # save to csv file
# df_sentiment.to_csv('df_twitter_sent' + '_'.join(company_input_1) + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=True)

