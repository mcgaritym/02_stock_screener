#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 19:10:36 2020

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


ticker = 'MSFT'
url = 'https://www.marketbeat.com/stocks/NASDAQ/' + ticker + '/earnings/'

print(url)

driver.delete_all_cookies()
time_0 = time.time()
driver.get(url)
driver.execute_script("window.scrollTo(0, 1000000)")
response_delay = time.time() - time_0
time.sleep(5*response_delay) 

page_source = driver.page_source
soup = BeautifulSoup(page_source, "html.parser")
date_today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

list_ = []

# a = soup.find_all(name='div', class_=re.compile('jobsearch-SerpJobCard unifiedRow row'))
# print(a)
tr = soup.select('#cphPrimaryContent_tabEarningsHistory > div:nth-child(13) > div > table > tbody > tr')

print(len(tr))

for tr_item in tr:
    #print(tr_item.td.text)
    td = tr_item.select('td')
    if len(td) < 7:
        continue  
    if td[0] != None:
        date_report = td[0].text
    if td[1] != None:
        date_quarter = td[1].text
    if td[2] != None:
        consensus_estimate_earnings = td[2].text
    if td[3] != None:
        reported_eps = td[3].text
    if td[4] != None:
        GAAP_eps = td[4].text
    if td[5] != None:
        revenue_estimate = td[5].text
    if td[6] != None:
        revenue_actual = td[6].text
    else:
        continue

    list_.append(dict({'date_report': date_report, 
                        'date_quarter': date_quarter, 
                        'consensus_estimate_earnings': consensus_estimate_earnings,
                        'reported_eps': reported_eps,
                        'GAAP_eps': GAAP_eps,
                        'revenue_estimate': revenue_estimate,
                        'revenue_actual': revenue_actual}))
        
df_earnings = pd.DataFrame(list_, columns=['date_report', 
                                            'date_quarter', 
                                            'consensus_estimate_earnings',
                                            'reported_eps',
                                            'GAAP_eps',
                                            'revenue_estimate',
                                            'revenue_actual'])

print(df_earnings) 
        
#df_earnings.to_csv('df_earnings_' + ticker  + '_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)



    

    
    
    


