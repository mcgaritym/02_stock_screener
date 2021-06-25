#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 08:42:34 2020

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

# set driver options and request options
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--headless')
options.add_argument('--incognito')
driver = webdriver.Chrome(ChromeDriverManager().install())
headers = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.39 Safari/537.36', 
           "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}


# create function for given url input
def fin_news_scraper(*urls):

    
    # create empty list            
    list_news = []
    
    #for loop for variable # of url inputs to function
    for url in urls:
        
        # get link, use bs4 to parse
        #driver.manage().timeouts().implicitlyWait(TimeOut, TimeUnit.SECONDS)        
        time_0 = time.time()
        driver.delete_all_cookies()
        driver.get(url)
        driver.execute_script("window.scrollTo(0, 1000)")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 2000)")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 3000)")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 5000)")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 25000)")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 500000)")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 1000000)")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 1000000)")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, 1000000)")
        response_delay = time.time() - time_0
        time.sleep(5*response_delay)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        date_today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # if statement based on url string
        
        if 'forbes' in url:
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('a', class_ = 'happening__title'):
            #     # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today})) 
            for t in soup.find_all('a', attrs = {'data-ga-track': re.compile('Breaking Article')}):
            #     # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today}))                                                 
            for t in soup.find_all('a', class_ = re.compile('data-viz__title')):
                list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today}))                  
            for t in soup.find_all('a', class_ = re.compile('h1--dense')):
                list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today}))               
            for t in soup.find_all('h3', class_ = 'h3--dense'):
                list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today}))                          
            for t in soup.find_all('a', class_ = 'headlink h4--dense'):
                 list_news.append(dict({'headline': t.span.contents[0], 'org':'forbes', 'date': date_today}))             
            for t in soup.find_all('a', attrs = {'data-ga-track': re.compile('Channel - Block')}):
                 list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today})) 
            for t in soup.find_all('h2', class_ = re.compile('h4--dense')):
                list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today}))  
            for t in soup.find_all('a', attrs = {'data-ga-track': re.compile('Featured Block')}):
            #     # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today}))                 
            for t in soup.find_all('a', attrs = {'data-ga-track': re.compile('Channel - Block')}):
            #     # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'forbes', 'date': date_today}))                                                                        

        if 'marketwatch' in url:
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('a', class_ = 'latestNews__headline'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.span.text, 'org':'marketwatch', 'date': date_today}))
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('h3', class_ = 'article__headline'):
                # find all None types:
                if t.a != None:
                    # append dictionary with headline, organization, and current date to empty list
                    list_news.append(dict({'headline': t.a.text, 'org':'marketwatch', 'date': date_today}))           
                else:
                    # append dictionary with headline, organization, and current date to empty list
                    list_news.append(dict({'headline': t.text, 'org':'marketwatch', 'date': date_today}))                         
            for t in soup.find_all('li', class_ = 'bullet__item'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'marketwatch', 'date': date_today}))             

        if 'wsj' in url:
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('h3', class_ = re.compile('WSJTheme--headline')):
                # append dictionary with headline, organization, and current date to empty list
                if t.a != None:
                    list_news.append(dict({'headline': t.a.text, 'org':'wsj', 'date': date_today}))
            for t in soup.find_all('h3', class_ = re.compile('style--headline')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.a.text, 'org':'wsj', 'date': date_today}))
            for t in soup.find_all('h3', class_ = re.compile('WSJTheme--title')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'wsj', 'date': date_today}))
            for t in soup.find_all('div', class_ = re.compile('style--text')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.h3.span.contents[0], 'org':'wsj', 'date': date_today}))

        if 'bloomberg' in url:
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('a', class_ = re.compile('single-story-module')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'bloomberg', 'date': date_today}))
            for t in soup.find_all('a', class_ = re.compile('story-package-module')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'bloomberg', 'date': date_today}))
            for t in soup.find_all('section', class_ = re.compile('single-story-module')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'bloomberg', 'date': date_today}))

            for t in soup.find_all('h3', class_ = re.compile('story-package-module')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.a.text, 'org':'bloomberg', 'date': date_today}))

        if 'reuters' in url:
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('h3', class_ = 'story-title'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'cnn', 'date': date_today}))
            for t in soup.find_all('h3', class_ = 'video-heading'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.a.text, 'org':'cnn', 'date': date_today}))
        
        # NOTE: investopedia has lots of dated/old articles, not up to date       
        if 'investopedia' in url:
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('span', class_ = 'card__title'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'cnn', 'date': date_today}))
           
        if 'cnn' in url:
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('h2', class_ = 'banner-text'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'cnnbusiness', 'date': date_today}))            
            for t in soup.find_all('span', class_ = 'cd__headline-text'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'cnnbusiness', 'date': date_today}))
            
        if 'cnbc' in url:
            # for loop iterates over matching HTML (site specific) and extracts text
            for t in soup.find_all('h2', re.compile('FeaturedCard')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.a.text, 'org':'cnbc', 'date': date_today}))                                   
            for t in soup.find_all('div', class_ = 'LatestNews-headline'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.a.text, 'org':'cnbc', 'date': date_today}))                  
            for t in soup.find_all('li', id = re.compile('FeaturedCard')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.a.text, 'org':'cnbc', 'date': date_today}))             
            for t in soup.find_all('div', class_ = 'SecondaryCard-headline'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.a.text, 'org':'cnbc', 'date': date_today}))             
            for t in soup.find_all('div', class_ = re.compile('RiverHeadline')):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.a.text, 'org':'cnbc', 'date': date_today}))             
            for t in soup.find_all('a', class_ = 'Card-title'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.div.text, 'org':'cnbc', 'date': date_today}))             
            for t in soup.find_all('a', class_ = 'TrendingNowItem-title'):
                # append dictionary with headline, organization, and current date to empty list
                list_news.append(dict({'headline': t.text, 'org':'cnbc', 'date': date_today}))             
                                                                                                  
        if 'ibtimes' in url:
            for t in soup.find_all(name='h3'):
                list_news.append(dict({'headline': t.a.text, 'org':'ibtimes', 'date': date_today}))                             
            for t in soup.find_all(name='li'):
                list_news.append(dict({'headline': t.a.text, 'org':'ibtimes', 'date': date_today}))                             
            for t in soup.find_all(name='div', class_ = 'title'):
                list_news.append(dict({'headline': t.a.text, 'org':'ibtimes', 'date': date_today}))                             
            for t in soup.find_all(name='div', class_ = 'item-link'):
                list_news.append(dict({'headline': t.a.text, 'org':'ibtimes', 'date': date_today}))                             
            for t in soup.find_all(name='h4'):
                list_news.append(dict({'headline': t.a.text, 'org':'ibtimes', 'date': date_today}))                             
            for t in soup.find_all(name='article', class_ = 'title'):
                list_news.append(dict({'headline': t.a.text, 'org':'ibtimes', 'date': date_today}))
                             
        if 'seekingalpha' in url:
            time.sleep(5)
            for t in soup.find_all('a', class_ = '_2when'):
                list_news.append(dict({'headline': t.text, 'org':'seekingalpha', 'date': date_today}))                 
            for t in soup.find_all('h3', attrs = {'data-test-id': "post-list-item-title"}):
                list_news.append(dict({'headline': t.text, 'org':'seekingalpha', 'date': date_today})) 
            for t in soup.select('article > div > a > h3'):
                list_news.append(dict({'headline': t.text, 'org':'seekingalpha', 'date': date_today})) 
      
        if 'fortune' in url:
            for t in soup.find_all('a', class_ = re.compile('featureModule__title')):
                list_news.append(dict({'headline': t.text, 'org':'fortune', 'date': date_today})) 
            for t in soup.find_all('div', class_ = re.compile('featureModule__excerpt')):
                list_news.append(dict({'headline': t.text, 'org':'fortune', 'date': date_today})) 
            for t in soup.find_all('a', class_ = re.compile('contentItem__')):
                list_news.append(dict({'headline': t.text, 'org':'fortune', 'date': date_today})) 
            for t in soup.find_all('a', class_ = re.compile('featureThreeGrid__titleLink')):
                list_news.append(dict({'headline': t.text, 'org':'fortune', 'date': date_today})) 
            for t in soup.find_all('a', class_ = re.compile('grid__titleLink')):
                list_news.append(dict({'headline': t.text, 'org':'fortune', 'date': date_today}))                                                     
            for t in soup.find_all('a', class_ = re.compile('rundownFeature__title')):
                list_news.append(dict({'headline': t.text, 'org':'fortune', 'date': date_today}))                                                     
            for t in soup.find_all('a', class_ = re.compile('rundownItem__title')):
                list_news.append(dict({'headline': t.text, 'org':'fortune', 'date': date_today}))                                                     
                
        if 'economist' in url:
            for t in soup.find_all('h1', class_ = 'weekly-edition-header__headline'):
                list_news.append(dict({'headline': t.text, 'org':'economist', 'date': date_today}))             
            for t in soup.find_all('span', class_ = re.compile('teaser__subheadline')):
                list_news.append(dict({'headline': t.text, 'org':'economist', 'date': date_today}))
            for t in soup.find_all('span', class_ = re.compile('teaser__headline')):
                list_news.append(dict({'headline': t.text, 'org':'economist', 'date': date_today}))                  
 
        if 'morningstar' in url:
            for t in soup.find_all('span', class_ = re.compile('mds-list-group__item')):
                list_news.append(dict({'headline': t.text, 'org':'morningstar', 'date': date_today}))           
            for t in soup.find_all('a', class_ = re.compile('mdc-link mdc-tag')):
                list_news.append(dict({'headline': t.text, 'org':'morningstar', 'date': date_today}))           
            for t in soup.find_all('span', attrs={"itemprop": "name"}):
                list_news.append(dict({'headline': t.text, 'org':'morningstar', 'date': date_today}))           
            for t in soup.find_all('a', class_ = re.compile('mdc-link mds-link')):
                list_news.append(dict({'headline': t.text, 'org':'morningstar', 'date': date_today}))           
            for t in soup.find_all('h2', class_ = re.compile('mdc-heading mdc-video')):
                list_news.append(dict({'headline': t.text, 'org':'morningstar', 'date': date_today}))           

        if 'financialtimes' in url:
            for t in soup.find_all('a', class_ = re.compile('js-teaser-heading')):
                list_news.append(dict({'headline': t.text, 'org':'financialtimes', 'date': date_today}))                                              
            for t in soup.find_all('a', class_ = re.compile('js-teaser-standfirst')):
                list_news.append(dict({'headline': t.text, 'org':'financialtimes', 'date': date_today}))                                              
            for t in soup.find_all('li', class_ = re.compile('o-teaser__related')):
                list_news.append(dict({'headline': t.a.text, 'org':'financialtimes', 'date': date_today}))                                              
                
        if 'thestreet' in url:
            for t in soup.find_all('h2', class_ = re.compile('header-text')):
                list_news.append(dict({'headline': t.text, 'org':'thestreet', 'date': date_today}))         
            for t in soup.find_all('span', class_ = re.compile('m-ellipsis--text')):
                list_news.append(dict({'headline': t.text, 'org':'thestreet', 'date': date_today}))         
            
        if 'msn' in url:
            for t in soup.select('div > a > h3'):
                list_news.append(dict({'headline': t.text, 'org':'msnmoney', 'date': date_today}))               
            for t in soup.find_all('h3', class_ = re.compile('typography-DS-card1')):
                list_news.append(dict({'headline': t.text, 'org':'msnmoney', 'date': date_today}))               
                                               
        if 'google' in url:
            for t in soup.find_all('h3'):
                list_news.append(dict({'headline': t.a.text, 'org':'googlenews', 'date': date_today}))         
            for t in soup.find_all('h4'):
                list_news.append(dict({'headline': t.a.text, 'org':'googlenews', 'date': date_today}))                                                 
            
        if 'fool' in url:
            for t in soup.find_all('div', class_ = 'caption'):
                list_news.append(dict({'headline': t.h3.text, 'org':'motleyfool', 'date': date_today}))                 
            for t in soup.find_all('div', class_ = 'text'):
                list_news.append(dict({'headline': t.h4.text, 'org':'motleyfool', 'date': date_today}))                 
                        
        if 'foxbusiness' in url:
            for t in soup.find_all('h2', class_ = re.compile('title title-color')):
                list_news.append(dict({'headline': t.a.text, 'org':'foxbusiness', 'date': date_today}))         
            for t in soup.find_all('li', class_ = re.compile('related-item')):
                list_news.append(dict({'headline': t.a.text, 'org':'foxbusiness', 'date': date_today}))         
            for t in soup.find_all('h3', class_ = re.compile('title title-color')):
                list_news.append(dict({'headline': t.a.text, 'org':'foxbusiness', 'date': date_today})) 
            for t in soup.find_all('a', attrs = {'data-adv':'hp1s'}):
                list_news.append(dict({'headline': t.text, 'org':'foxbusiness', 'date': date_today}))                                  
            for t in soup.find_all('blockquote', class_ = re.compile('quote-bubble')):
                list_news.append(dict({'headline': t.text, 'org':'foxbusiness', 'date': date_today}))                                                 
                
        if 'yahoo' in url: 
            for t in soup.find_all(name='h2', class_=re.compile(' Lh')):
                list_news.append(dict({'headline': t.text, 'org':'yahoofinance', 'date': date_today})) 
            for t in soup.find_all(name='h3', class_=re.compile(' Lh')):
                list_news.append(dict({'headline': t.text, 'org':'yahoofinance', 'date': date_today})) 
            for t in soup.find_all(name='h3', class_=re.compile('Mb')):
                list_news.append(dict({'headline': t.a.text, 'org':'yahoofinance', 'date': date_today})) 

        # else:
        #     print('Error: URL not found')
            
        #create pandas dataframe based on list with extracted info, with headlne, organization, and date columns
        df = pd.DataFrame(list_news, columns=['headline', 'org', 'date'])
        print(df) 
    
    # close driver
    driver.close()
    
    print(df.groupby('org')['headline'].count())
    print("null values", df.isnull().sum(axis = 0))
    df.to_csv('fin_news_headlines__' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)

time_1 = time.time()

fin_news_scraper(
'https://www.msn.com/money',    
'https://finance.yahoo.com/',
'https://www.wsj.com/',
'https://seekingalpha.com/',
'https://www.marketwatch.com/', 
'https://www.cnn.com/business/',
'https://www.investopedia.com/company-news-4427705',
'https://www.forbes.com/',
'https://www.bloomberg.com/',
'https://www.reuters.com/finance',
'https://www.reuters.com/news/archive/businessnews?page=1',
'https://www.reuters.com/news/archive/businessnews?page=2',
'https://www.reuters.com/news/archive/businessnews?page=3',
'https://www.reuters.com/news/archive/businessnews?page=4',
'https://www.reuters.com/news/archive/businessnews?page=5',  
'https://www.cnbc.com/',
'https://www.ibtimes.com/business',
'https://www.ibtimes.com/technology',
'https://fortune.com/',
'https://www.economist.com/weeklyedition',
'https://www.morningstar.com/',
'http://www.financialtimes.com/',
'https://www.thestreet.com/',
'https://news.google.com/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB?hl=en-US&gl=US&ceid=US:en',
'https://www.fool.com/investing-news/',
'https://www.foxbusiness.com')

time_2 = time.time()

print('Runtime is: {} minutes'.format((time_2 - time_1)/60))


             
    