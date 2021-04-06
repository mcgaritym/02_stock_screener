#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 16:54:18 2020

@author: mcgaritym
"""

# import packages
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import date, datetime
import re
import time
from glob import glob
import os
from datetime import date, datetime
import time

# set driver options and request options
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--headless')
options.add_argument('--incognito')
driver = webdriver.Chrome(ChromeDriverManager().install())
headers = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.39 Safari/537.36', 
            "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}

url = 'https://old.nasdaq.com/screening/companies-by-name.aspx?&render=download'
driver.delete_all_cookies()
driver.get(url)
time.sleep(5)

# get current working directory
current_folder = os.getcwd()
print(current_folder)

# set os path for downloads folder
data_folder = os.path.join("/", "Users", "mcgaritym", "Downloads")
print(data_folder)

# get csv's from data_folder directory
file_list = glob(os.path.join(data_folder, '*companylist*.csv'))
print(file_list)

df_tickers = pd.DataFrame()
for f in file_list:
    data = pd.read_csv(f)
    df_tickers = pd.concat([df_tickers, data], axis=0)

df_tickers = df_tickers.dropna(subset=['MarketCap'])
df_tickers['MarketCap'] = df_tickers['MarketCap'].astype(str)
df_tickers['MarketCap'] = df_tickers['MarketCap'].apply(lambda x: x.replace('$', ''))
df_tickers['MarketCap'] = df_tickers['MarketCap'].apply(lambda x: x.replace('B', '0000000'))
df_tickers['MarketCap'] = df_tickers['MarketCap'].apply(lambda x: x.replace('M', '0000'))
df_tickers['MarketCap'] = df_tickers['MarketCap'].apply(lambda x: x.replace('.', ''))
df_tickers['MarketCap'] = df_tickers['MarketCap'].astype(int)
df_tickers['MarketCap'] = df_tickers['MarketCap'].sort_values(ascending=False)

df_tickers.to_csv('df_tickers_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)

