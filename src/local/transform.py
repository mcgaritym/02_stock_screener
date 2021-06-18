# import config settings and Setup class
import pandas as pd
import numpy as np
import re
import time

import config
from setup import Setup

# import NLP libraries
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# call Setup class from setup.py file
connection = Setup(config.user, config.pwd, config.host, config.port, config.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()

# transform news_sentiment table
# def clean_news(df):
#
#
#
#     # drop na
#     df = df.dropna(subset=['headline'])
#
#     # clean dataframe
#     df_clean = pd.DataFrame()
#
#     # clean string
#     # loop over tickers
#     for i, row in df.iterrows():
#
#         # clean text
#         df_clean.loc[i, 'date'] = row['date']
#         df_clean.loc[i, 'org'] = row['org']
#         df_clean.loc[i, 'headline'] = re.sub(r'[^a-zA-Z0-9 ]+', "", row['headline'].strip())
#
#     # filter for headlines > 3 words
#     df_clean['headline_length'] = [len(x.split()) for x in df_clean['headline']]
#     df_clean = df_clean[df_clean['headline_length'] > 3]
#
#     # create sentiment column for news headlines
#     sid = SentimentIntensityAnalyzer()
#     df_clean['sentiment'] = df_clean['headline'].apply(sid.polarity_scores)
#     df_clean['sentiment'] = df_clean['sentiment'].apply(lambda x: x['compound'])
#
#     print(df_clean['headline'].values)
#     return df_clean
#     #
#     # # drop nan and duplicate values and filter for headline length more than 3 words


    #
    # # create sentiment column for news headlines
    # sid = SentimentIntensityAnalyzer()
    # df_news['sentiment'] = df_news['headline'].apply(sid.polarity_scores)
    # df_news['sentiment'] = df_news['sentiment'].apply(lambda x: x['compound'])
    #
    # # resample daily and groupby organization
    # company_sentiment = df_news.groupby('org')['sentiment'].resample('D').mean().unstack(level=0)
    # company_sentiment = company_sentiment.rename(columns=lambda x: x + '_sentiment')
    #
    # # create overall sentiment column
    # company_sentiment['overall_sentiment'] = company_sentiment.mean(axis=1)
    #
    # return df_cleaned

t1 = time.time()
df_old = pd.read_sql('SELECT * FROM news_sentiment WHERE headline is NOT NULL', con=conn)
df_old = df_old.dropna(subset=['headline'])
t2 = time.time()
print('Time for query: {}'.format(t2-t1))

pattern = r"[^a-zA-Z0-9 ]+"

t3 = time.time()
df_old['headlines_clean_1'] = df_old['headline'].str.replace(pattern, "").str.strip()
t4 = time.time()
print('Time for execution: {}'.format(t4-t3))

t5 = time.time()
df_old['headlines_clean_2'] = df_old['headline'].apply(lambda x: re.sub(pattern, "", x).strip())
t6 = time.time()
print('Time for execution: {}'.format(t6-t5))

t7 = time.time()
df_old['headlines_clean_3'] = [re.sub(pattern, "", x).strip() for x in df_old['headline']]
t8 = time.time()
print('Time for execution: {}'.format(t8-t7))


# df_clean = clean_news(df_old)
# df_clean.head()

# transform stock_financials table



# transform stock_tickers table


print('debug')