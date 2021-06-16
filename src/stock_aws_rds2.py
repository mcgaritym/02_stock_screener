#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 27 22:17:28 2021

@author: mcgaritym
"""

import pandas as pd
import config_aws
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

#SQL
import mysql.connector 
from mysql.connector import errorcode
from sqlalchemy import create_engine

cnx = mysql.connector.connect(
        host = config_aws.host,
        user = config_aws.user,
        password = config_aws.pwd,
        database = config_aws.db_name)

print(cnx)
cursor = cnx.cursor()

# establish connection to new database
engine = create_engine("mysql+mysqlconnector://{user}:{pwd}@{host}/{db_name}".format(
        host = config_aws.host,
        user = config_aws.user,
        pwd = config_aws.pwd,
        db_name = config_aws.db_name))

conn = engine.connect()

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

    # df_tickers = df_tickers[['Symbol', 'Name', 'Market Cap', 'Sector', 'Industry']]
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
