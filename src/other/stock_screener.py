#!/usr/bin/env python
# coding: utf-8

# # Stock Screener

# <u>TABLE OF CONTENTS:</u> 
# 0. Introduction
# 1. Data Input
#     * 1.1. Stock Tickers  
#     * 1.2. Stock Fundamentals (Prices, Volume, Splits, Revenue, Earnings)
#     * 1.3. Financial News Sentiment
# 2. Data Processing/Cleaning
# 3. Data Visualization/EDA
#     * 3.1. Stock Individual Returns Before vs. During COVID-19  
#     * 3.2. Stock Sector Returns Before vs. During COVID-19 
#     * 3.3. Stock Industry Returns Before vs. During COVID-19 
#     * 3.4. Stock Sector Prices Over Time     
#     * 3.5. Stock Clustering During COVID-19    
#     * 3.6. Financial News Sentiment by Source
#     * 3.7. Stock Correlation to Financial News Sentiment 
#     * 3.8. Industry/Sector Correlation to Financial News Sentiment   
#     * 3.9. Stock Screener (by P/E Ratio, Earnings)      
# 4. Conclusion

# ## 0. Introduction:

# The purpose of this project is to evaluate stock prices and employ various machine learning methods to try and predict their behavior in the future. The motivation for this project is due to personal interest (investing), and the fact that stocks provide a wealth of historical data to include in a machine learning problem. Also, I am interested in determining the effect of the COVID-19 pandemic on stocks in general. 
# 
# The data sources for this project include scraped news headlines from a variety of top financial news sources (for financial news sentiment), the yahoo finance API (for financial fundamentals such as stock prices, earnings, P/E ratios), and the NASDAQ website (for stock tickers, sectors, and industries). 
# 
# This project will try and answer these questions:
# 
# - Which stocks have the greatest rate of return before vs. during COVID-19?
# - Which sectors have the greatest rate of return before vs. during COVID-19?
# - Which industries have the greatest rate of return before vs. during COVID-19?
# - How have sector stock prices changed over time during COVID-19?
# - Can stock price changes be used to cluster groups of individual stocks?
# - Which financial news sources have the highest/lowest headline sentiment? 
# - Which stocks are correlated to financial news sentiment?
# - Which sectors/industries are correlated to financial news sentiment?
# - Can we screen stocks that are good value based on average sector Price/Earnings ratio, and quarterly growth percent?

# ## 1. Data Input:

# In[1]:


# load required libraries
import os
import re
from glob import glob 
import string
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.collections as collections 
import seaborn as sns
from scipy.cluster.hierarchy import linkage, dendrogram
import matplotlib.dates as mdates
from datetime import datetime, time, date
import time

# sklearn libraries
from sklearn.linear_model import Ridge
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.metrics import r2_score
from sklearn.preprocessing import normalize

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


# ### 1.1 Stock Tickers:

# In[2]:


def get_tickers(url):

    # set page load and driver options
    capa = DesiredCapabilities.CHROME
    capa["pageLoadStrategy"] = "none"
    driver = webdriver.Chrome(ChromeDriverManager().install(), desired_capabilities=capa)
    wait = WebDriverWait(driver, 20)

    # get url
    driver.get(url)

    # click More button
    button_xpath = "//div[@class = 'nasdaq-screener__download']"

    WebDriverWait(driver, 5).until(expected_conditions.element_to_be_clickable((By.XPATH, button_xpath)))
    button = driver.find_element_by_xpath(button_xpath)
    button.click()
    time.sleep(5)

    # get current working directory
    current_folder = os.getcwd()

    # set os path for downloads folder
    data_folder = os.path.join("/", "Users", "mcgaritym", "Downloads")

    # get csv(s) from data_folder directory
    file = glob(os.path.join(data_folder, '*companylist*.csv'))
    for f in file:
        df_tickers = pd.read_csv(f)

    df_tickers = df_tickers[['Symbol', 'Name', 'MarketCap', 'Sector', 'industry']]
    df_tickers = df_tickers.dropna(subset=['MarketCap'])
    df_tickers['MarketCap'] = df_tickers['MarketCap'].astype(str)
    df_tickers['MarketCap'] = df_tickers['MarketCap'].apply(lambda x: x.replace('$', ''))
    df_tickers['MarketCap'] = df_tickers['MarketCap'].apply(lambda x: x.replace('B', '0000000'))
    df_tickers['MarketCap'] = df_tickers['MarketCap'].apply(lambda x: x.replace('M', '0000'))
    df_tickers['MarketCap'] = df_tickers['MarketCap'].apply(lambda x: x.replace('.', ''))
    df_tickers['MarketCap'] = df_tickers['MarketCap'].astype(int)
    df_tickers['MarketCap'] = df_tickers['MarketCap'].sort_values(ascending=False)    
    
    return df_tickers

df_tickers = get_tickers('https://www.nasdaq.com/market-activity/stocks/screener')
df_tickers = df_tickers.sort_values(by='MarketCap', ascending=False)

# print sample data
df_tickers.head()


# In[3]:


# print number of companies per sector
print(df_tickers['Sector'].value_counts())


# In[4]:


# print number of companies per sector
print(df_tickers['industry'].value_counts())


# In[8]:


def top_tickers_sector(df, sector):
    
    ticker_list = []
    
    for sec in sector:
                
        # filter by sector and sort by market cap
        df_tick = df[df['Sector'] == sec].sort_values(by='MarketCap', ascending=False)
        df_tick = list(df_tick.iloc[:10]['Symbol'])   
        
        for tick in df_tick:
            
            ticker_list.append(tick)
                         
    return ticker_list  


# In[10]:


# get top 10 tickers of all sectors
all_sectors = list(df_tickers['Sector'].unique())
top_tickers_sector = top_tickers_sector(df_tickers, sector = all_sectors)
print(top_tickers_sector)


# In[11]:


def top_tickers_industry(df, industry):
    
    ticker_list = []
    
    for ind in industry:
                
        # filter by sector and sort by market cap
        df_tick = df[df['industry'] == ind].sort_values(by='MarketCap', ascending=False)
        df_tick = list(df_tick.iloc[:3]['Symbol'])   
        
        for tick in df_tick:
            
            ticker_list.append(tick)
                         
    return ticker_list 


# In[12]:


# print sample data
all_industries = list(df_tickers['industry'].unique())
ticker_list_ind = top_tickers_industry(df_tickers, industry = all_industries)
print(ticker_list_ind)


# ### 1.2 Stock Fundamentals (Prices, Volume, Splits, Revenue, Earnings):

# In[38]:


# define function for generating stock history of each ticker and concatenate to master dataframe
def stock_preprocess(tickers):

    # create empty dataframe
    df_api = pd.DataFrame()
    
    # loop over each stock ticker
    for tick in tickers:
            
        # input various stock tickers 
        ticker = yf.Ticker(tick)
        
        # get historical market data
        stock_hist = ticker.history(period="1y", interval='1D', auto_adjust = True)
    
        # rename columns unique to eack ticker
        stock_hist = stock_hist[['Close', 'Volume', 'Stock Splits']]
        
        # get quarterly earnings
        stock_earnings = ticker.quarterly_earnings
        
        # rename columns unique to each ticker, and format dates (assume the earning reporting dates are 1 month after quarter ends)
        stock_earnings = stock_earnings.reset_index()
        stock_earnings = stock_earnings.rename(columns = {'Quarter': 'Date'})
        stock_earnings = stock_earnings.replace(('4Q2019', '1Q2020', '2Q2020', '3Q2020'), ('2020-01-31', '2020-04-30', '2020-07-30', '2020-10-31')   )
        stock_earnings['Date'] = pd.to_datetime(stock_earnings['Date'])
        
        # concat dataframes
        stock_hist = stock_hist.merge(stock_earnings, on='Date', how='outer')
        
        # sort date values
        stock_hist = stock_hist.set_index('Date')
        stock_hist = stock_hist.sort_index(ascending=True)
        
        # fill in nan values and drop remaining nan values
        stock_hist = stock_hist.fillna(method = 'ffill')
        stock_hist = stock_hist.dropna()
        stock_hist = stock_hist.drop_duplicates()
        
        # rename columns with ticker name
        stock_hist = stock_hist.rename(columns=lambda x: tick.upper() + '_' + x)
        
        # append to empty dataframe
        stock_hist = stock_hist.loc[~stock_hist.index.duplicated()]
        df_api = df_api.loc[~df_api.index.duplicated()]
        df_api = pd.concat([df_api, stock_hist], axis=1, sort=False)
                        
    # print and specift dataframe is returned    
    return df_api

# print sample data call function with ticker_input
stock_preprocess(['AMZN', 'MSFT']).head()


# ### 1.3 Financial News Sentiment:

# In[14]:


# FINANCIAL NEWS HEADLINES - retrieve data
def get_news():
    
    # get current parent directory and data folder path
    par_directory = os.path.dirname(os.getcwd())
    data_directory = os.path.join(par_directory, 'data/raw')

    # specify file names
    files_headlines = glob(os.path.join(data_directory, '*fin_news_headlines*.csv'))

    ## create empty dataframe, loop over files and concatenate data to dataframe. next, reset index and print tail
    df_news = pd.DataFrame()
    for f in files_headlines:
        data = pd.read_csv(f, parse_dates = ['date'])
        df_news = pd.concat([df_news, data], axis=0, sort='False')
    df_news = df_news.dropna()
    df_news = df_news.drop_duplicates()
    
    # reset index, sort values by date and print data sample
    df_news = df_news.reset_index(drop=True)
    df_news = df_news.sort_values(by='date', ascending=True)
    df_news = df_news.rename(columns = {'date': 'Date'})
    df_news['Date'] = pd.to_datetime(df_news['Date'])
    df_news = df_news.set_index('Date')
    
    return df_news

df_news = get_news()
df_news.head()


# ## 2. Data Processing/Cleaning:

# In[15]:


def clean_news(df):
  
    # drop nan and duplicate values and filter for headline length more than 3 words
    df_news = df.dropna()
    df_news = df_news.drop_duplicates()
    df_news['headline_length'] = [len(x.split()) for x in df_news['headline']]
    df_news = df_news[df_news['headline_length'] > 3]
    

    # create sentiment column for news headlines
    sid = SentimentIntensityAnalyzer()
    df_news['sentiment'] = df_news['headline'].apply(sid.polarity_scores)
    df_news['sentiment'] = df_news['sentiment'].apply(lambda x: x['compound'])

    # resample daily and groupby organization
    company_sentiment = df_news.groupby('org')['sentiment'].resample('D').mean().unstack(level=0)
    company_sentiment = company_sentiment.rename(columns=lambda x: x + '_sentiment')

    # create overall sentiment column
    company_sentiment['overall_sentiment'] = company_sentiment.mean(axis=1)

    return company_sentiment

df_news = clean_news(df_news)
df_news.head()


# ## 3. Data Visualization/EDA

# ### 3.1. Stock Individual Returns Before vs. During COVID-19

# In[16]:


# preprocess ticker list to get values
def stock_preprocess_prices(tickers):
    
    # create empty dataframe
    df_api = pd.DataFrame()
    
    # loop over each stock ticker
    for tick in tickers:
            
        # input various stock tickers 
        ticker = yf.Ticker(tick)
        
        # get historical market data
        stock_hist = ticker.history(period="1y", interval='1D', auto_adjust=True)
    
        # filter columns for closing price
        stock_hist = stock_hist[['Close']]
        
        # fill in nan values and drop remaining nan values
        stock_hist = stock_hist.interpolate(method='linear', limit_direction = 'forward', axis=0)
#         stock_hist = stock_hist.fillna(method = 'ffill')
#         stock_hist = stock_hist.dropna()
#         stock_hist = stock_hist.drop_duplicates()
        
        # rename columns with ticker name
        stock_hist = stock_hist.rename(columns=lambda x: tick.upper() + '_C')

        # concat to empty dataframe
        df_api = pd.concat([df_api, stock_hist], axis=1)
        
    return df_api
        
df_all = stock_preprocess_prices(tickers = top_tickers_all)  
df_all.tail()


# In[17]:


def stock_returns(df):
    
    # drop first row of nan values
    df = df.iloc[1:,]
    
    # create empty list
    returns_ = []
    
    # loop over columns
    for x in df.columns:
        
        # create initial val based on pandemic start date, final val based on current date, and calc percent chg
        init_val = df[x].iloc[0]
        # init_val = df.loc['2020-03-02'][x]
        final_val = df[x].iloc[-1]
        pct_chg = ((final_val - init_val)/(init_val))*100
        # append dict to list
        returns_.append(dict({'ticker': x, 'pandemic_change%':pct_chg}))

    # create pandas dataframe based on list 
    df_returns = pd.DataFrame(returns_, columns=['ticker', 'pandemic_change%'])
    df_returns = df_returns.dropna(subset=['pandemic_change%'])
    df_returns = df_returns[df_returns['ticker'] != 'AMOV_C']
    df_returns = df_returns.sort_values(by = 'pandemic_change%', ascending=False)
    df_returns = df_returns.reset_index(drop=True)

    return df_returns
        
df_stock_returns = stock_returns(df_all)


# In[18]:


print('Highest Stock Returns :\n')
df_stock_returns[:10]


# In[19]:


print('Lowest Stock Returns :\n')
df_stock_returns[-10:]


# ### 3.2. Stock Sector Returns Before vs. During COVID-19

# In[20]:


def stock_preprocess_avg_sect(sectors):

    # create empty dataframe
    df_sector = pd.DataFrame()
    
    # loop over sectors
    for sec in sectors:
        
        # get list of tickers for each sector
        tickers = top_tickers(df_tickers, sector = [sec])
        
        # create empty dataframe
        df_stocks = pd.DataFrame()
        
        # loop over each stock ticker
        for tick in tickers:

            # input various stock tickers 
            ticker = yf.Ticker(tick)

            # get historical market data
            stock_hist = ticker.history(period="1y", interval='1D')

            # filter columns for closing price
            stock_hist = stock_hist[['Close']]

            # fill in nan values and drop remaining nan values
            stock_hist = stock_hist.interpolate(method='linear', limit_direction = 'forward', axis=0)
    #         stock_hist = stock_hist.fillna(method = 'ffill')
#             stock_hist = stock_hist.dropna()
#             stock_hist = stock_hist.drop_duplicates()

            # rename columns with ticker name
            stock_hist = stock_hist.rename(columns=lambda x: tick.upper() + '_' + x)

            # concat to empty dataframe
            df_stocks = pd.concat([df_stocks, stock_hist], axis=1)
            
        # create average price columns
        df_avg = pd.DataFrame(df_stocks.mean(axis=1))
        df_avg = df_avg.rename(columns=lambda x: str(sec))
        
        # concat to empty dataframe
        df_sector = pd.concat([df_sector, df_avg], axis=1)

    # remove duplicated indexes (dates)
    df_sector = df_sector.drop(columns = ['nan'])
    df_sector = df_sector[~df_sector.index.duplicated()]

    return df_sector
        
df_sector = stock_preprocess_avg_sect(sectors = list(df_tickers['Sector'].unique()))  
df_sector.tail()


# In[21]:


def sector_returns(df):
    
    # drop first row of nan values
    df = df.iloc[1:,]
    
    # create empty list
    returns_ = []
    
    # loop over columns
    for x in df.columns:
        
        # create initial val based on pandemic start date, final val based on current date, and calc percent chg
        init_val = df[x].iloc[0]
        # init_val = df.loc['2020-03-02'][x]
        final_val = df[x].iloc[-1]
        pct_chg = ((final_val - init_val)/(init_val))*100
        
        # append dict to list
        returns_.append(dict({'sector': x, 'pandemic_change%':pct_chg})) 

    # create pandas dataframe based on list 
    df_returns = pd.DataFrame(returns_, columns=['sector', 'pandemic_change%'])
    df_returns = df_returns.sort_values(by='pandemic_change%', ascending=False)
    df_returns = df_returns.reset_index(drop=True)
    
    return df_returns
        
df_sector_returns = sector_returns(df_sector)
df_sector_returns


# ### 3.3. Stock Industry Returns Before vs. During COVID-19

# In[22]:


# preprocess ticker list to get values
def stock_preprocess_avg_ind(industry):

    # create empty dataframe
    df_industry = pd.DataFrame()
    
    # loop over sectors
    for ind in industry:
        
        # get list of tickers for each sector
        tickers = top_tickers_industry(df_tickers, industry = [ind])
        
        # create empty dataframe
        df_stocks = pd.DataFrame()
        
        # loop over each stock ticker
        for tick in tickers:

            # input various stock tickers 
            ticker = yf.Ticker(tick)

            # get historical market data
            stock_hist = ticker.history(period="1y", interval='1D')

            # filter columns for closing price
            stock_hist = stock_hist[['Close']]

            # fill in nan values and drop remaining nan values
            stock_hist = stock_hist.interpolate(method='linear', limit_direction = 'forward', axis=0)
    #         stock_hist = stock_hist.fillna(method = 'ffill')
#             stock_hist = stock_hist.dropna()
#             stock_hist = stock_hist.drop_duplicates()

            # rename columns with ticker name
            stock_hist = stock_hist.rename(columns=lambda x: tick.upper() + '_' + x)

            # concat to empty dataframe
            df_stocks = pd.concat([df_stocks, stock_hist], axis=1)
            
        # create average price columns
        df_avg = pd.DataFrame(df_stocks.mean(axis=1))
        df_avg = df_avg.rename(columns=lambda x: str(ind))
        
        # concat to empty dataframe
        df_industry.reset_index(drop=True, inplace=True)
        df_avg.reset_index(drop=True, inplace=True)
        df_industry = pd.concat([df_industry, df_avg], axis=1)

    # remove duplicated indexes (dates)
#     df_industry = df_industry.drop(columns = ['nan'])
    df_industry = df_industry[~df_industry.index.duplicated()]
    df_industry = df_industry[:-1]
    
    return df_industry
        
df_industry = stock_preprocess_avg_ind(industry = list(df_tickers['industry'].unique()))  
df_industry.tail()


# In[23]:


def industry_returns(df):
    
    # drop first row of nan values
    df = df.iloc[1:,]
    
    # create empty list
    returns_ = []
    
    # loop over columns
    for x in df.columns:
        
        # create initial val based on pandemic start date, final val based on current date, and calc percent chg
        init_val = df[x].iloc[0]
        # init_val = df.loc['2020-03-02'][x]
        final_val = df[x].iloc[-1]
        pct_chg = ((final_val - init_val)/(init_val))*100
        
        # append dict to list
        returns_.append(dict({'industry': x, 'pandemic_change%':pct_chg})) 

    # create pandas dataframe based on list 
    df_returns = pd.DataFrame(returns_, columns=['industry', 'pandemic_change%'])
    df_returns = df_returns.sort_values(by='pandemic_change%', ascending=False)
    df_returns = df_returns.reset_index(drop=True)
    
    return df_returns
        
df_industry_returns = industry_returns(df_industry)
df_industry_returns


# ### 3.4. Stock Sector Prices Over Time

# In[24]:


def plot_prices(df_tickers, sector):
    
    # get top 10 tickers for sector, process, and filter columns for closing prices
    tickers = top_tickers(df_tickers, [sector])
    print(tickers)
    df = stock_preprocess(tickers)
    cols = df.columns[df.columns.str.contains(pat = 'Close')] 
    df = df[cols]
    
    # set seaborn style, plot options and plot closing prices
    sns.set()
    fig, ax = plt.subplots(figsize = (12,8), dpi=300)
    sns.lineplot(data=df, dashes=False)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    plt.xticks(rotation=45, fontsize=12) 
    ax.set_title('Closing Prices of Stocks: {}'.format(sector), fontweight ='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Closing Price ($ USD)')
    plt.show()
    time.sleep(3)
       


# In[25]:


def plot_sector_prices(df):
    
    # set seaborn style, plot options and plot closing prices
    sns.set()
    fig, ax = plt.subplots(figsize = (12,8), dpi=300)
    sns.lineplot(data=df, dashes=False)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    plt.xticks(rotation=45, fontsize=12) 
    ax.set_title('Closing Prices of Stocks: All Sectors', fontweight ='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Closing Price ($ USD)')
    plt.show()
    time.sleep(3)
    


# In[26]:


plot_prices(df_tickers, 'Health Care')


# In[28]:


plot_prices(df_tickers, 'Finance')


# In[29]:


plot_prices(df_tickers, 'Consumer Services')


# In[40]:


plot_prices(df_tickers, 'Technology')


# In[39]:


plot_prices(df_tickers, 'Capital Goods')


# In[41]:


plot_prices(df_tickers, 'Basic Industries')


# In[42]:


plot_prices(df_tickers, 'Energy')


# In[43]:


plot_prices(df_tickers, 'Consumer Durables')


# In[44]:


plot_prices(df_tickers, 'Consumer Non-Durables')


# In[45]:


plot_prices(df_tickers, 'Public Utilities')


# In[46]:


plot_prices(df_tickers, 'Transportation')


# In[49]:


plot_prices(df_tickers, 'Miscellaneous')


# In[48]:


plot_sector_prices(df_sector)  


# ### 3.5. Stock Clustering During COVID-19

# In[50]:


# create clustering function, percent change columns of closing price
def plot_cluster_sector(df_tickers, sector):
    
    # get top 10 tickers for sector, process, and filter columns for closing prices
    tickers = top_tickers(df_tickers, sector)
    df = stock_preprocess(tickers)
    cols = df.columns[df.columns.str.contains(pat = 'Close')] 
    df = df[cols]
    df = df.rename(columns=lambda x: x.replace('_Close', ''))
    
    # fill in sentiment results
    df = df.replace(0,)
    df = df.interpolate('linear')
    df = df.dropna()
    
    # transpose news orgs and dates
    df = df.transpose()
 
    # create labels
    labels = list(df.index)

    # create linkages
    Z = linkage(df, 'ward')

    # make the dendrogram
    sns.set()
    fig, ax = plt.subplots(figsize = (12,8), dpi=300)  
    plt.title('Hierarchical Clustering Dendrogram', fontweight = 'bold')
    plt.xlabel('stocks')
    plt.ylabel('distance (Ward)')
    dendrogram(Z, labels=df.index, leaf_rotation=90)
    plt.show() 
    
_ = plot_cluster_sector(df_tickers, sector = ['Technology'])


# ### 3.6. Financial News Sentiment By Source 

# In[51]:


def plot_sentiment(df):
    
    # get top 10 tickers for sector, process, and filter columns for closing prices
    cols = df.columns[df.columns.str.contains(pat = '_sentiment')] 
    df = df[cols]
    df = df.rename(columns=lambda x: x.replace('_sentiment', ''))
    
    # fill in sentiment results
    df = df.replace(0,)
    df = df.interpolate('linear')
    df = df.dropna()
    df = df.resample('M').mean()
    
    # set seaborn style, plot options and plot closing prices
    sns.set()
    fig, ax = plt.subplots(figsize = (12,8), dpi=300)
    sns.lineplot(data=df, dashes=False)
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    plt.xticks(rotation=45, fontsize=12) 
    ax.set_title('Financial News Sentiment', fontweight ='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Sentiment (-1 = Neg., +1 = Pos.)')
    plt.show()
    
    # print highest sentiment to lowest sentiment news sources
    print('Average News Sentiment by Source: \n\n{}'.format(df.mean().sort_values(ascending=False)))


# In[52]:


plot_sentiment(df_news)


# ### 3.7. Stock Correlation to Financial News Sentiment

# In[53]:


# merge news dataframe overall sentiment with df_all dataframe
df_all = pd.merge(df_all, df_news[['overall_sentiment']], left_index=True, right_index=True, how='outer')

# filter for weekdays when stock price changes
df_all['day_of_week'] = df_all.index.dayofweek
df_all = df_all[(df_all['day_of_week'] >= 0) & (df_all['day_of_week'] <= 4)]

# print correlation values to determine stocks most correlated with news sentiment
correlation_mat = df_all.corr()
corr_pairs = correlation_mat.unstack()


# In[54]:


print('Stocks with highest correlation to news sentiment: \n\n{}'.format(corr_pairs['overall_sentiment'].sort_values(kind="quicksort", ascending=False).iloc[:10]))


# ### 3.8. Industry/Sector Correlation to Financial News Sentiment

# In[55]:


df_sector_industry = pd.concat([df_sector.reset_index(), df_industry], axis=1)
df_sector_industry = df_sector_industry.rename(columns={"index": "date"})
df_sector_industry = df_sector_industry.set_index('date')
df_sector_industry.tail()


# In[56]:


# merge news dataframe overall sentiment with df_all dataframe
df_all_2 = pd.merge(df_sector_industry, df_news[['overall_sentiment']], left_index=True, right_index=True, how='outer')

# filter for weekdays when stock price changes
df_all_2['day_of_week'] = df_all_2.index.dayofweek
df_all_2 = df_all_2[(df_all_2['day_of_week'] >= 0) & (df_all_2['day_of_week'] <= 4)]

# plot correlation heatmap across all stocks to view correlation
plt.figure(dpi=300)
sns.heatmap(df_all_2.corr()) 

# print correlation values to determine stocks most correlated with news sentiment
correlation_mat = df_all_2.corr()
corr_pairs = correlation_mat.unstack()


# In[57]:


print('Industries/Sectors with highest correlation to news sentiment: \n\n{}'.format(corr_pairs['overall_sentiment'].sort_values(kind="quicksort", ascending=False).iloc[:10]))


# ### 3.9 Stock Screener (by P/E Ratio, Earnings)

# In[68]:


# define function for generating stock history of each ticker and concatenate to master dataframe
def stock_pe_earnings(df):

    # create empty columns
    df = df.copy()
    df['trailingPE'] = ''
    df['earnings_0Q'] = ''
    df['earnings_1Q'] = ''
    df['earnings_2Q'] = ''
    df['earnings_3Q'] = '' 
    
    # loop over each stock ticker to get PE ratio
    for i, row in df.iterrows():
        
        # sleep
        time.sleep(1)
                            
        # input various stock tickers 
        ticker = yf.Ticker(row['Symbol'])
        stock_earnings = ticker.quarterly_earnings
        stock_info = ticker.info       
            
        try:
            
            # get info
            df.loc[i, 'earnings_0Q'] = stock_earnings['Earnings'][-1]
            df.loc[i, 'earnings_1Q'] = stock_earnings['Earnings'][-2]
            df.loc[i, 'earnings_2Q'] = stock_earnings['Earnings'][-3]
            df.loc[i, 'earnings_3Q'] = stock_earnings['Earnings'][-4]
            
        except:
            
            df.loc[i, 'earnings_0Q'] = np.nan
            df.loc[i, 'earnings_1Q'] = np.nan
            df.loc[i, 'earnings_2Q'] = np.nan
            df.loc[i, 'earnings_3Q'] = np.nan
            
        try:
            
            df.loc[i, 'trailingPE'] = stock_info['trailingPE']
            
        except:
            
            df.loc[i, 'trailingPE'] = np.nan
                    
        time.sleep(1)
        
    # convert data types    
    df['trailingPE'] = df['trailingPE'].astype(float)
    df['earnings_0Q'] = df['earnings_0Q'].astype(int)
    df['earnings_1Q'] = df['earnings_1Q'].astype(int)
    df['earnings_2Q'] = df['earnings_2Q'].astype(int)
    df['earnings_3Q'] = df['earnings_3Q'].astype(int)

    # drop na
    df = df.dropna(how='any')
    
    # get sector average pe
    df_sector_pe = df.groupby('Sector')['trailingPE'].mean().reset_index()
    df_sector_pe = df_sector_pe.rename(columns = {'trailingPE': 'sector_trailingPE'})
    
    # merge dataframes
    df = df.merge(df_sector_pe, on='Sector', how='left')
        
    return df


# In[69]:


df_pe_earnings = stock_pe_earnings(df_tickers.sort_values(by='MarketCap', ascending=False)[:50])
df_pe_earnings.head()


# In[70]:


def stock_filter(df, quarter_growth_percent = None):
    
    df['earnings_growth'] = ''
    quarter_growth_str = 'earnings_growth_' + str(quarter_growth_percent)
    quarter_growth_percent = 1 + (quarter_growth_percent/100)
    df[quarter_growth_str] = ''
    
    for i, row in df.iterrows():
        
        if (row['earnings_0Q'] > row['earnings_1Q']) and (row['earnings_1Q'] > row['earnings_2Q']) and (row['earnings_2Q'] > row['earnings_3Q']):
        
            df.loc[i, 'earnings_growth'] = 'Yes'
            
        else:
            
            df.loc[i, 'earnings_growth'] = 'No'
            
    
        if (row['earnings_0Q'] > quarter_growth_percent*(row['earnings_1Q'])) and (row['earnings_1Q'] > quarter_growth_percent*(row['earnings_2Q'])) and (row['earnings_2Q'] > quarter_growth_percent*(row['earnings_3Q'])):
        
            df.loc[i, quarter_growth_str] = 'Yes'
            
        else:
            
            df.loc[i, quarter_growth_str] = 'No'    
            
            
    df = df[(df[quarter_growth_str] == 'Yes') & (df['sector_trailingPE'] > df['trailingPE'])]
            
    return df
    


# In[71]:


df_filtered = stock_filter(df_pe_earnings, quarter_growth_percent = 5)
df_filtered


# ## 4. Conclusion

# This project performed data analysis on stocks and scraped news headlines and tried to answer the following questions. The answers and results are shown below:
# 
# 
# ### **Which stocks have the greatest rate of return before vs. during COVID-19?** 
# 
# The stocks with the highest returns include the following:
# 
# | ticker | pandemic_change% 
# | :- | :-: 
# | TSLA | 589.309 | 
# | CVNA | 477.913 |  
# | PDD | 290.054 | 
# | GNRC | 282.471 | 
# | MELI | 232.365 | 
# | DE | 178.419 | 
# | SCCO | 167.482 | 
# | FDX | 162.865 | 
# | ASML | 162.79 | 
# | KMX | 153.724 | 
# 
# ### **Which sectors have the greatest rate of return before vs. during COVID-19?**
# 
# The sectors with the highest returns include the following:
# 
# | sector | pandemic_change% 
# | :- | :-: 
# | Consumer Durables | 103.636663 | 
# | Miscellaneous | 101.987157 |  
# | Capital Goods | 99.840312 | 
# | Technology | 95.468499 | 
# | Transportation | 88.925665 | 
# | Finance | 85.931560 | 
# | Basic Industries | 57.395231 | 
# | Consumer Services | 57.371112 | 
# | Consumer Non-Durables | 50.414571 | 
# | Energy | 46.359885 | 
# | Public Utilities | 37.793667 | 
# | Health Care | 36.989555 | 
# 
# ### **Which industries have the greatest rate of return before vs. during COVID-19?**
# 
# The industries with the highest returns include the following:
# 
# | industry | pandemic_change% 
# | :- | :-: 
# | Auto Manufacturing | 304.387023 | 
# | Engineering & Construction | 156.161482 |  
# | Shoe Manufacturing | 127.565042 | 
# | Restaurants | 111.543056 | 
# | Semiconductors | 103.183557 | 
#  
# ### **How have sector stock prices changed over time during COVID-19?**
# 
# The changes in price for all stock sectors is plotted in section 3.4 stock sector prices over time. All sectors show a positive return, with some sectors showing higher growth (e.g. Technology) than others (e.g. Health Care). See section 3.4 for the stock price changes for the leading stocks in each sector. 
# 
# ### **Can stock price changes be used to cluster groups of individual stocks?**
# 
# In section 3.5 stock clustering during COVID-19, a dendogram was plotted for a sample of stocks (the top 10 stocks in Technology sector). The heirarichical clustering dendogram showed the clustering of stock prices in terms of euclidean distance, based on the ward method (minimize variance within each cluster). The results show the strongest clustering between Google (GOOG and GOOGL, obviously), Microsoft and Salesforce (MSFT and CRM), Adobe and Nvidia (ADBE and NVDA), and Apple and Intel (AAPL and INTC). 
# 
# ### **Which financial news sources have the highest/lowest headline sentiment?**
# 
# In section 3.6 financial news sentiment by source, the various scraped news headlines strings were converted to sentiment and resampled daily over time. The most positive news sentiment was found to be Motley Fool, Seeking Alpha and Morninstar. The most negative news sentiment was found to be Economist, CNN Business, and Financial Times. 
# 
# ### **Which stocks are correlated to financial news sentiment?**
# 
# In section 3.7 stock correlation to financial news sentiment, the correlation between stock prices and news sentiment was analyzed. The stocks with the strongest positive correlation to news sentiment, in terms of pearson correlation coefficient, was found to be Fedex (0.422833), Ball Corporation (0.422745), Copart (0.399580), Costco (0.399254), and United Parcel Service (0.353451).  
# 
# ### **Which sectors/industries are correlated to financial news sentiment?**
# 
# In section 3.8 sector/industry correlation to financial news sentiment, the correlation between sectors/industries and news sentiment was analyzed. The sectors/industries with the strongest positive correlation to news sentiment, in terms of pearson correlation coefficient, was found to be Department/Specialty Retail Stores (0.396246), Apparel (0.371543), Air Freight/Delivery Services (0.336459), and Containers/Packaging (0.331440). A correlation heatmap was also plotted to visually show the results. 
# 
# ### **Can we screen stocks that are good value based on average sector Price/Earnings ratio, and quarterly growth percent?**
# 
# In section 3.9, a stock screener based on Price/Earnings ratio (trailing P/E), and quarterly earnings was created, in order to find stocks that are a good value to invest. The screener filtered the top 50 stocks by market cap. The first criteria in filtering stocks was finding stocks with a Price/Earnings ratio that was lower than the average Price/Earnings ratio in that stock's sector. The second criteria was finding stocks with quarterly growth of at least 5% in each of the last four quarters. The resulting stocks that matched this criteria include Facebook, JP Morgan and Chase, Toyota, and Thermo Fisher Scientific. In future work, the screener can be easily adjusted to screen based on other criteria such as yearly earnings, higher earnings growth (e.g. 10% quarter over quarter/year over year), and other metrics. 

# In[ ]:




