# import config settings and Setup class
import pandas as pd
import numpy as np
import re
import time
from sqlalchemy import create_engine
import pymysql

import config_cloud
from setup_cloud import Setup

# import NLP libraries
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# call Setup class from setup_cloud.py file
connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()

# get data from SQL
df = pd.read_sql('SELECT * FROM news_sentiment WHERE headline is NOT NULL', con=conn)

# clean headline data, get sentiment
def clean_news(df):

    # clean headlines using vectorized operations
    pattern = r"[^a-zA-Z0-9 ]+"
    df['headlines_clean'] = df['headline'].str.replace(pattern, "").str.strip()

    # split string
    df['headlines_length'] = df['headlines_clean'].str.split()

    # determine length and filter for headlines > 3 words
    df['headlines_length'] = df['headlines_length'].apply(lambda x: len(x))
    df = df[df['headlines_length'] > 3]
    df['sentiment'] = df['headline'].apply(SentimentIntensityAnalyzer().polarity_scores)
    df['sentiment'] = df['sentiment'].apply(lambda x: x['compound'])

    # resample daily and groupby organization
    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    df = df.set_index('date')
    df_sentiment = df.groupby('org')['sentiment'].resample('D').mean().unstack(level=0)
    df_sentiment = df_sentiment.rename(columns=lambda x: x + '_sentiment')

    # create overall sentiment column
    df_sentiment['overall_sentiment'] = df_sentiment.mean(axis=1)

    return df_sentiment

df_clean = clean_news(df)

# send to SQL table
df_clean.to_sql(name='news_sentiment_CLEAN', con=conn, if_exists='replace', index_label='date')

print('debug')