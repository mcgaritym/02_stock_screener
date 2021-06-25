#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 20 13:33:15 2021

@author: mcgaritym
"""

import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import glob
import pymysql
import time
import yfinance as yf
import numpy as np


# scraping libraries
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from datetime import timedelta, date, datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions 
from selenium.webdriver.common.by import By

# =============================================================================
# Establish SQL connection and create database
# =============================================================================

# credentials
user = 'root'
pwd = "Nalgene09!"
host = 'localhost'
port= int(3306)


# establish connection, create database
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/",
    echo=False)
conn = engine.connect()
conn.execute("CREATE DATABASE IF NOT EXISTS stocks_db;")
conn.close()

# add database credential and connect
db = 'stocks_db' 

# establish connection to new database
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}",
    echo=False)

conn = engine.connect()

# =============================================================================
# create/load stock ticker table to SQL
# =============================================================================

def get_tickers(url):

    # # set page load and driver options
    # capa = DesiredCapabilities.CHROME
    # capa["pageLoadStrategy"] = "none"
    # driver = webdriver.Chrome(ChromeDriverManager().install(), desired_capabilities=capa)
    # wait = WebDriverWait(driver, 20)

    # # get url
    # driver.get(url)

    # # click More button
    # button_xpath = "//div[@class = 'nasdaq-screener__download']"

    # WebDriverWait(driver, 5).until(expected_conditions.element_to_be_clickable((By.XPATH, button_xpath)))
    # button = driver.find_element_by_xpath(button_xpath)
    # button.click()
    # time.sleep(5)

    # get current working directory
    current_folder = os.getcwd()

    # set os path for downloads folder
    data_folder = os.path.join("/", "Users", "mcgaritym", "Downloads")

    # get csv(s) from data_folder directory
    file = glob.glob(os.path.join(data_folder, '*nasdaq_screener*.csv'))
    file.sort()
    file = file[-1]
    
    df_tickers = pd.read_csv(file)

    df_tickers = df_tickers[['Symbol', 'Name', 'Market Cap', 'Sector', 'Industry']]
    df_tickers = df_tickers.dropna(subset=['Market Cap'])
    df_tickers['Market Cap'] = df_tickers['Market Cap'].astype(str)
    df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('$', ''))
    df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('B', '0000000'))
    df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('M', '0000'))
    df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('.', ''))
    df_tickers['Market Cap'] = df_tickers['Market Cap'].astype(int)
    df_tickers['Market Cap'] = df_tickers['Market Cap'].sort_values(ascending=False)    
        
    return df_tickers

df_tickers = get_tickers('https://www.nasdaq.com/market-activity/stocks/screener')
df_tickers = df_tickers.sort_values(by='Market Cap', ascending=False)
df_tickers.to_sql(name='stock_tickers', con=engine, if_exists='replace', index=False)

# print sample data
print(df_tickers.head())

conn.close()

# =============================================================================
# clean stock_ticker table
# =============================================================================
# conn = engine.connect()

# # rename columns
# conn.execute("""ALTER TABLE stock_tickers
#                   RENAME COLUMN `Symbol` TO symbol,
#                   RENAME COLUMN `Name` TO name,
#                   RENAME COLUMN `Market Cap` TO market_cap,
#                   RENAME COLUMN `Sector` TO sector,
#                   RENAME COLUMN `Industry` TO industry; """)

# # update table keys
# conn.execute("""ALTER TABLE stock_tickers
#                   MODIFY COLUMN symbol varchar(6),
#                   MODIFY COLUMN market_cap BIGINT,
#                   ADD PRIMARY KEY(symbol); """)


# conn.close()

# =============================================================================
# create/load news sentiment table to SQL
# =============================================================================

# conn = engine.connect()

# def clean_news(df):
  
#     # drop na's, duplicates, and more cleaning
#     df = df.dropna()
#     df = df.drop_duplicates()
#     df = df.reset_index(drop=True)
#     df = df.sort_values(by='date', ascending=True)
#     df = df.rename(columns = {'date': 'Date'})
#     df['Date'] = pd.to_datetime(df['Date']) 
    
#     # filter for headline length more than 3 words
#     df['headline_length'] = [len(x.split()) for x in df['headline']]
#     df = df[df['headline_length'] > 3]
    
#     # create sentiment column for news headlines
#     sid = SentimentIntensityAnalyzer()
#     df['sentiment'] = df['headline'].apply(sid.polarity_scores)
#     df['sentiment'] = df['sentiment'].apply(lambda x: x['compound'])

#     return df


# # FINANCIAL NEWS HEADLINES - retrieve data
# def get_news():
    
#     # get current parent directory and data folder path
#     par_directory = os.path.dirname(os.getcwd())
#     data_directory = os.path.join(par_directory, 'data/raw')

#     # specify file names
#     files_headlines = glob.glob(os.path.join(data_directory, '*fin_news_headlines*.csv'))

#     ## create empty dataframe, loop over files and concatenate data to dataframe. next, reset index and print tail
#     for f in files_headlines:
        
#         # read into dataframe
#         data = pd.read_csv(f, parse_dates = ['date'])
#         print(len(data))
        
#         # clean news
#         data = clean_news(data)
        
#         # append to SQL table
#         data.to_sql(name='news_sentiment', con=engine, if_exists='append', index=False)
    
# df_news = get_news()

# conn.close()

# =============================================================================
# create/load stock financial  table to SQL
# =============================================================================

# loop through stock tickers

# get quarterly earnings 1, 2, 3, 4, yearly earnings 1, 2, EPS, PE ratio
# define function for generating stock history of each ticker and concatenate to master dataframe
def stock_financials(df):
    
    # create empty df
    df_financials = pd.DataFrame(columns = ['symbol', 
                                            'earnings_Q-0', 'earnings_Q-1', 'earnings_Q-2', 'earnings_Q-3',
                                            'earnings_Y-0', 'earnings_Y-1', 'earnings_Y-2', 'earnings_Y-3',
                                            'revenue_Q-0', 'revenue_Q-1', 'revenue_Q-2', 'revenue_Q-3',
                                            'revenue_Y-0', 'revenue_Y-1', 'revenue_Y-2', 'revenue_Y-3',
                                            'trailingPE', 'trailingEps', 'twoHundredDayAverage', 'fiftyDayAverage',
                                            'dividendRate', 'industry', 'sector', 'zip'])

    df = df[:3]

    # loop over tickers
    for i, row in df.iterrows():
        
        # sleep
        time.sleep(3)
        
        # get symbol
        df_financials.loc[i, 'symbol'] = row['symbol']
        
        # overall
        try:
                  
            # input various stock tickers 
            ticker = yf.Ticker(row['symbol'])
            stock_quarterly_earnings = ticker.quarterly_earnings
            stock_yearly_earnings = ticker.earnings
            stock_info = ticker.info       
        
        except:
            
            # quarterly
            df_financials.loc[i, 'earnings_Q-0'] = np.nan
            df_financials.loc[i, 'earnings_Q-1'] = np.nan
            df_financials.loc[i, 'earnings_Q-2'] = np.nan
            df_financials.loc[i, 'earnings_Q-3'] = np.nan
            df_financials.loc[i, 'revenue_Q-0'] = np.nan
            df_financials.loc[i, 'revenue_Q-1'] = np.nan
            df_financials.loc[i, 'revenue_Q-2'] = np.nan
            df_financials.loc[i, 'revenue_Q-3'] = np.nan                  
            
            # yearly
            df_financials.loc[i, 'earnings_Y-0'] = np.nan
            df_financials.loc[i, 'earnings_Y-1'] = np.nan
            df_financials.loc[i, 'earnings_Y-2'] = np.nan
            df_financials.loc[i, 'earnings_Y-3'] = np.nan
            df_financials.loc[i, 'revenue_Y-0'] = np.nan
            df_financials.loc[i, 'revenue_Y-1'] = np.nan
            df_financials.loc[i, 'revenue_Y-2'] = np.nan
            df_financials.loc[i, 'revenue_Y-3'] = np.nan                  
                        
            # other ratios, metrics
            df_financials.loc[i, 'trailingPE'] = np.nan
            df_financials.loc[i, 'trailingEps'] = np.nan
            df_financials.loc[i, 'twoHundredDayAverage'] = np.nan
            df_financials.loc[i, 'fiftyDayAverage'] = np.nan
            df_financials.loc[i, 'dividendRate'] = np.nan
            df_financials.loc[i, 'industry'] = np.nan            
            df_financials.loc[i, 'sector'] = np.nan
            df_financials.loc[i, 'zip'] = np.nan           
            
        #quarterly    
        try:
            
            df_financials.loc[i, 'earnings_Q-0'] = stock_quarterly_earnings['Earnings'][-1]
            df_financials.loc[i, 'earnings_Q-1'] = stock_quarterly_earnings['Earnings'][-2]
            df_financials.loc[i, 'earnings_Q-2'] = stock_quarterly_earnings['Earnings'][-3]
            df_financials.loc[i, 'earnings_Q-3'] = stock_quarterly_earnings['Earnings'][-4]
            df_financials.loc[i, 'revenue_Q-0'] = stock_quarterly_earnings['Revenue'][-1]
            df_financials.loc[i, 'revenue_Q-1'] = stock_quarterly_earnings['Revenue'][-2]
            df_financials.loc[i, 'revenue_Q-2'] = stock_quarterly_earnings['Revenue'][-3]
            df_financials.loc[i, 'revenue_Q-3'] = stock_quarterly_earnings['Revenue'][-4]          
            
            
        except:
            
            df_financials.loc[i, 'earnings_Q-0'] = np.nan
            df_financials.loc[i, 'earnings_Q-1'] = np.nan
            df_financials.loc[i, 'earnings_Q-2'] = np.nan
            df_financials.loc[i, 'earnings_Q-3'] = np.nan
            df_financials.loc[i, 'revenue_Q-0'] = np.nan
            df_financials.loc[i, 'revenue_Q-1'] = np.nan
            df_financials.loc[i, 'revenue_Q-2'] = np.nan
            df_financials.loc[i, 'revenue_Q-3'] = np.nan             
                 
            
        # yearly    
        try:
            
            df_financials.loc[i, 'earnings_Y-0'] = stock_yearly_earnings['Earnings'].iloc[-1]
            df_financials.loc[i, 'earnings_Y-1'] = stock_yearly_earnings['Earnings'].iloc[-2]
            df_financials.loc[i, 'earnings_Y-2'] = stock_yearly_earnings['Earnings'].iloc[-3]
            df_financials.loc[i, 'earnings_Y-3'] = stock_yearly_earnings['Earnings'].iloc[-4]
            df_financials.loc[i, 'revenue_Y-0'] = stock_yearly_earnings['Revenue'].iloc[-1]
            df_financials.loc[i, 'revenue_Y-1'] = stock_yearly_earnings['Revenue'].iloc[-2]
            df_financials.loc[i, 'revenue_Y-2'] = stock_yearly_earnings['Revenue'].iloc[-3]
            df_financials.loc[i, 'revenue_Y-3'] = stock_yearly_earnings['Revenue'].iloc[-4]          
            
            
        except:
            
            df_financials.loc[i, 'earnings_Y-0'] = np.nan
            df_financials.loc[i, 'earnings_Y-1'] = np.nan
            df_financials.loc[i, 'earnings_Y-2'] = np.nan
            df_financials.loc[i, 'earnings_Y-3'] = np.nan
            df_financials.loc[i, 'revenue_Y-0'] = np.nan
            df_financials.loc[i, 'revenue_Y-1'] = np.nan
            df_financials.loc[i, 'revenue_Y-2'] = np.nan
            df_financials.loc[i, 'revenue_Y-3'] = np.nan    
            
        # ratios, metrics, etc
        try:
            
            df_financials.loc[i, 'trailingPE'] = stock_info['trailingPE']
            df_financials.loc[i, 'trailingEps'] = stock_info['trailingEps']
            df_financials.loc[i, 'twoHundredDayAverage'] = stock_info['twoHundredDayAverage']
            df_financials.loc[i, 'fiftyDayAverage'] = stock_info['fiftyDayAverage']
            df_financials.loc[i, 'dividendRate'] = stock_info['dividendRate']
            df_financials.loc[i, 'industry'] = stock_info['industry']
            df_financials.loc[i, 'sector'] = stock_info['sector']
            df_financials.loc[i, 'zip'] = stock_info['zip']
            
        except:
            
            df_financials.loc[i, 'trailingPE'] = np.nan
            df_financials.loc[i, 'trailingEps'] = np.nan
            df_financials.loc[i, 'twoHundredDayAverage'] = np.nan
            df_financials.loc[i, 'fiftyDayAverage'] = np.nan
            df_financials.loc[i, 'dividendRate'] = np.nan
            df_financials.loc[i, 'industry'] = np.nan
            df_financials.loc[i, 'sector'] = np.nan
            df_financials.loc[i, 'zip'] = np.nan
   
    # convert data types    
    df_financials['earnings_Q-0'] = df_financials['earnings_Q-0'].astype(float)
    df_financials['earnings_Q-1'] = df_financials['earnings_Q-1'].astype(float)
    df_financials['earnings_Q-2'] = df_financials['earnings_Q-2'].astype(float)
    df_financials['earnings_Q-3'] = df_financials['earnings_Q-3'].astype(float)
    df_financials['revenue_Q-0'] = df_financials['revenue_Q-0'].astype(float)
    df_financials['revenue_Q-1'] = df_financials['revenue_Q-1'].astype(float)
    df_financials['revenue_Q-2'] = df_financials['revenue_Q-2'].astype(float)
    df_financials['revenue_Q-3'] = df_financials['revenue_Q-3'].astype(float)
    
    df_financials['earnings_Y-0'] = df_financials['earnings_Y-0'].astype(float)
    df_financials['earnings_Y-1'] = df_financials['earnings_Y-1'].astype(float)
    df_financials['earnings_Y-2'] = df_financials['earnings_Y-2'].astype(float)
    df_financials['earnings_Y-3'] = df_financials['earnings_Y-3'].astype(float)
    df_financials['revenue_Y-0'] = df_financials['revenue_Y-0'].astype(float)
    df_financials['revenue_Y-1'] = df_financials['revenue_Y-1'].astype(float)
    df_financials['revenue_Y-2'] = df_financials['revenue_Y-2'].astype(float)
    df_financials['revenue_Y-3'] = df_financials['revenue_Y-3'].astype(float)
    
    df_financials['trailingPE'] = df_financials['trailingPE'].astype(float)
    df_financials['trailingEps'] = df_financials['trailingEps'].astype(float)
    df_financials['twoHundredDayAverage'] = df_financials['twoHundredDayAverage'].astype(float)
    df_financials['fiftyDayAverage'] = df_financials['fiftyDayAverage'].astype(float)
    df_financials['dividendRate'] = df_financials['dividendRate'].astype(float)
    df_financials['industry'] = df_financials['industry'].astype(str)
    df_financials['sector'] = df_financials['sector'].astype(str)
    df_financials['zip'] = df_financials['zip'].astype(str)

    return df_financials

tickers_list = pd.read_sql_query("""SELECT symbol FROM stock_tickers;""", con=engine)

df_financials = stock_financials(tickers_list)
df_financials.to_sql(name='stock_financials', con=engine, if_exists = 'replace')

conn.close()

# =============================================================================
# load ride data
# =============================================================================

# def get_files(wildcard_name):

#     # get current parent directory and data folder path
#     par_directory = os.path.dirname(os.getcwd())
#     data_directory = os.path.join(par_directory, 'data/raw')

#     # retrieve tripdata files
#     files = glob.glob(os.path.join(data_directory, wildcard_name))
#     print(files)

#     # create empty dataframe, loop over files and concatenate data to dataframe
#     # df = pd.DataFrame()
#     for f in files[:2]:
#         data = pd.read_csv(f)
#         print(len(data))
        
#         # convert to table
#         data.to_sql(name='ride_table_test', con=engine, if_exists = 'append')


# df = get_files('*tripdata*')

# =============================================================================
# load bike station data into table (via JSON request and pandas)
# =============================================================================

# # request json data
# response = requests.get("https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json")
# todos = json.loads(response.text)['data']
    
# # flatten nested json
# df_nested_list = pd.json_normalize(todos, record_path = ['stations'])
# df_nested_list = df_nested_list.drop(columns = ['rental_methods', 'eightd_station_services'])

# # convert to table
# df_nested_list.to_sql(name='capital_bs_stations', con=engine, if_exists = 'replace')


# # =============================================================================
# # update table columns, keys
# # =============================================================================

# # rename column 
# conn.execute("""ALTER TABLE ride_table_test
#                   RENAME COLUMN `Start station number` TO start_station_num; """)

# # update column 
# conn.execute("""ALTER TABLE ride_table_test
#                    ADD ride_id_key INT NOT NULL AUTO_INCREMENT,
#                    ADD PRIMARY KEY(ride_id_key); """)
                                   
# # update table keys
# conn.execute("""ALTER TABLE capital_bs_stations
#                   MODIFY COLUMN short_name INT,
#                   ADD PRIMARY KEY(short_name); """)

# # =============================================================================
# # analyze SQL table
# # =============================================================================

# # analyze capacity 
# conn.execute("""SELECT ride_table_test.start_station_num, capital_bs_stations.capacity
# FROM ride_table_test
# INNER JOIN capital_bs_stations
# ON ride_table_test.start_station_num=capital_bs_stations.short_name; """)


# =============================================================================
#  close SQL connection
# =============================================================================

# conn.close()

# =============================================================================
#  create AWS RDS connection to backup
# =============================================================================

# host="bikeshare-database-1.cj4j9csypvlc.us-east-2.rds.amazonaws.com"
# port="3306"
# user="admin"
# pwd="Nalgene09!"
# database="bikeshare-database-1"

# engine = create_engine(
#     f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{database}",
#     echo=False)

# # mysqlconnector

# conn = engine.connect()

# # convert to table
# df_nested_list.to_sql(name='capital_bs_stations', con=engine, if_exists = 'replace')

# conn.close()


# import sys
# import boto3
# import os

# ENDPOINT="bikeshare-database-1.cj4j9csypvlc.us-east-2.rds.amazonaws.com"
# PORT="3306"
# USR="admin"
# REGION="us-east-2a"
# os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

# #gets the credentials from .aws/credentials
# session = boto3.Session(profile_name='RDSCreds')
# client = session.client('rds')

# token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USR, Region=REGION)                    
    
