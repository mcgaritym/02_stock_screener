#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 09:24:06 2020

@author: mcgaritym
"""
import glob
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.collections as collections 
import seaborn as sns
import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.preprocessing import normalize
from scipy.cluster.hierarchy import linkage, dendrogram

# get dataframe of stock tickers
file_ticker = glob.glob('df_tickers_2020*.csv')
# print(file_ticker)
for file in file_ticker:
    df_tickers = pd.read_csv(file)
    
# input various stock tickers 
tickers = yf.Tickers('fb aapl amzn nflx googl msft ccl aal luv')

# # get historical market data
FB_hist = tickers.tickers.FB.history(period="3y", interval='1D')
AAPL_hist = tickers.tickers.AAPL.history(period="3y", interval='1D')
AMZN_hist = tickers.tickers.AMZN.history(period="3y", interval='1D')
NFLX_hist = tickers.tickers.NFLX.history(period="3y", interval='1D')
GOOGL_hist = tickers.tickers.GOOGL.history(period="3y", interval='1D')
MSFT_hist = tickers.tickers.MSFT.history(period="3y", interval='1D')
CCL_hist = tickers.tickers.CCL.history(period="3y", interval='1D')
AAL_hist = tickers.tickers.AAL.history(period="3y", interval='1D')
LUV_hist = tickers.tickers.LUV.history(period="3y", interval='1D')


# create dataframe of multiple stock closing prices
frame = {'FB': FB_hist.Close, 
         'AAPL': AAPL_hist.Close,
         'AMZN': AMZN_hist.Close,
         'NFLX': NFLX_hist.Close,
         'GOOGL': GOOGL_hist.Close,
         'MSFT': MSFT_hist.Close,
         'CCL': CCL_hist.Close,
         'AAL': AAL_hist.Close,
         'LUV': LUV_hist.Close}
         
         
close_prices = pd.DataFrame(frame, index = AAPL_hist.index)
close_prices_nodates = close_prices.reset_index(drop=True)
close_prices_nodates['days'] = close_prices_nodates.index - close_prices_nodates.index[0]
close_prices_nodates.set_index('days', inplace=True)


# =============================================================================
# CLEAN DATA
# =============================================================================

# # fill in missing data over time (linear interpolation)
missing_prices = close_prices_nodates.isna()
close_prices_nodates_interpolate = close_prices_nodates.interpolate('linear')

# # replace outliers by setting mean to 0, calculate std, use abs value of each datapoint, replace outlier w/ median
close_prices_nodates_centered = close_prices_nodates - close_prices_nodates.mean()
close_prices_nodates_std = close_prices_nodates.std()
close_prices_nodates_outliers = np.abs(close_prices_nodates_centered) > (close_prices_nodates_std * 3)
close_prices_nodates_no_outliers = close_prices_nodates_centered.copy()
close_prices_nodates_no_outliers[close_prices_nodates_outliers] = np.nanmedian(close_prices_nodates_no_outliers)

# =============================================================================
# CREATE ADDITIONAL FEATURES
# =============================================================================

# # create additional features via min, max, mean, std, percentiles, day/week/month
# min, max, mean, std
# Define a rolling window with Pandas, excluding the right-most datapoint of the window
close_prices_rolling = close_prices.rolling(20, min_periods=5, closed='right')

# calculate 10, 50, and 90 quantiles for rolling price
close_prices_rolling10 = close_prices.rolling(20, min_periods=5, closed='right').quantile(.1, interpolation='linear').dropna()
close_prices_rolling10 = close_prices_rolling10.rename(columns=lambda x: x+'_10_percentile')
close_prices_rolling50 = close_prices.rolling(20, min_periods=5, closed='right').quantile(.5, interpolation='linear').dropna()
close_prices_rolling50 = close_prices_rolling50.rename(columns=lambda x: x+'_50_percentile')
close_prices_rolling90 = close_prices.rolling(20, min_periods=5, closed='right').quantile(.9, interpolation='linear').dropna()
close_prices_rolling90 = close_prices_rolling90.rename(columns=lambda x: x+'_90_percentile')

# create time lags
shifts = [1, 2, 3, 5, 10, 15, 30, 60, 90, 248, 496]

# Use a dictionary comprehension to create name: value pairs, one pair per shift, and convert to dataframe
shifted_data_GOOG = {"GOOG_lag_{}_day".format(day_shift): close_prices_rolling['GOOGL'].mean().shift(day_shift) for day_shift in shifts}
close_price_rolling_shifted_GOOG = pd.DataFrame(shifted_data_GOOG)

shifted_data_FB = {"FB_lag_{}_day".format(day_shift): close_prices_rolling['FB'].mean().shift(day_shift) for day_shift in shifts}
close_price_rolling_shifted_FB = pd.DataFrame(shifted_data_FB)

shifted_data_AAPL = {"AAPL_lag_{}_day".format(day_shift): close_prices_rolling['AAPL'].mean().shift(day_shift) for day_shift in shifts}
close_price_rolling_shifted_AAPL = pd.DataFrame(shifted_data_AAPL)

shifted_data_AMZN = {"AMZN_lag_{}_day".format(day_shift): close_prices_rolling['AMZN'].mean().shift(day_shift) for day_shift in shifts}
close_price_rolling_shifted_AMZN = pd.DataFrame(shifted_data_AMZN)

shifted_data_NFLX = {"NFLX_lag_{}_day".format(day_shift): close_prices_rolling['NFLX'].mean().shift(day_shift) for day_shift in shifts}
close_price_rolling_shifted_NFLX = pd.DataFrame(shifted_data_NFLX)

shifted_data_MSFT = {"MSFT_lag_{}_day".format(day_shift): close_prices_rolling['MSFT'].mean().shift(day_shift) for day_shift in shifts}
close_price_rolling_shifted_MSFT = pd.DataFrame(shifted_data_MSFT)


# Plot the first 100 shifted data
# ax = close_price_rolling_shifted_MSFT.iloc[:100].plot(cmap=plt.cm.viridis)
# prices_perc.iloc[:100].plot(color='r', lw=2)
# ax.legend(loc='best')
# plt.show()


# Define the features you'll calculate for each window
features_stats = [np.min, np.max, np.mean, np.std]

# Calculate these features for your rolling window object
close_prices_rolling = close_prices_rolling.aggregate(features_stats).dropna()
close_prices_rolling.columns = close_prices_rolling.columns.map('_'.join)

# day, week, month
close_prices_rolling.index = pd.to_datetime(close_prices_rolling.index)
close_prices_rolling['day_of_week'] = close_prices_rolling.index.weekday
close_prices_rolling['week_of_year'] = close_prices_rolling.index.week
close_prices_rolling['month_of_year'] = close_prices_rolling.index.month

# merge existing dataframe with quantile values
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_prices_rolling10,
                 left_index=True, right_index=True, 
                 how='left')
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_prices_rolling50,
                 left_index=True, right_index=True, 
                 how='left')
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_prices_rolling90,
                 left_index=True, right_index=True, 
                 how='left')

# merge more dataframes
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_price_rolling_shifted_GOOG,
                 left_index=True, right_index=True, 
                 how='left')
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_price_rolling_shifted_AAPL,
                 left_index=True, right_index=True, 
                 how='left')
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_price_rolling_shifted_AMZN,
                 left_index=True, right_index=True, 
                 how='left')
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_price_rolling_shifted_NFLX,
                 left_index=True, right_index=True, 
                 how='left')
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_price_rolling_shifted_MSFT,
                 left_index=True, right_index=True, 
                 how='left')
close_prices_rolling = pd.merge(close_prices_rolling,
                 close_price_rolling_shifted_FB,
                 left_index=True, right_index=True, 
                 how='left')


# --------------------------------------

# load in news sentiment and frequency dataframe
file = glob.glob('df_keywords_sentiment*.csv')
# print(file)
for f in file:   
    df_keywords_sentiment = pd.read_csv(f)
df_keywords_sentiment = df_keywords_sentiment.set_index('index')
df_keywords_sentiment.index = pd.to_datetime(df_keywords_sentiment.index)

# merge news sentiment and frequency dataframe
close_prices_rolling = pd.merge(close_prices_rolling,
                 df_keywords_sentiment,
                 left_index=True, right_index=True, 
                 how='left')

# get additional feature - bank/analyst ratings 
# map ratings to dictionary
def analyst_ratings(*tickers):
    
    dict = {'Sell' : 1, 
                    'Underperform' : 2, 
                    'Overweight' : 2, 
                    'Hold' : 3, 
                    'Neutral' : 3, 
                    'Equal-Weight' : 3, 
                    'Market Perform' : 3.5,  
                    'Perform' : 3.5,                
                    'Outperform' : 4, 
                    'Buy' : 5,
                    'Strong Buy' : 5}

    df_recommendations = pd.DataFrame()

    for tick in tickers:
        
        # input various stock tickers         
        key2 = tick.upper() + '_recommendation'
        tick = yf.Ticker(tick)

        # convert grade to numeric, clean data and resample daily

        frame2 = tick.recommendations
        frame2[key2] = frame2['To Grade']
        frame2 = frame2[[key2]]
        frame2 = frame2.replace({key2: dict}) 
        frame2['drop'] = frame2[key2].str.isdigit()
        frame2 = frame2[frame2['drop'] != False]
        frame2 = frame2[[key2]].astype(int)
        frame2.index = pd.to_datetime(frame2.index)
        frame2 = frame2[key2].resample('D').mean()
        frame2 = frame2.interpolate(method='linear')
        df_recommendations = pd.concat([df_recommendations, frame2], axis=1)
        
    return df_recommendations

df_recommendations = analyst_ratings('aapl', 'amzn', 'msft', 'nflx')
# print(df_recommendations)

# merge analyst recommendations dataframe
close_prices_rolling = pd.merge(close_prices_rolling,
                  df_recommendations,
                  left_index=True, right_index=True, 
                  how='left')


# --------------------

# =============================================================================
# PLOT DENDROGRAM FOR STOCK PRICE MOVEMENT
# =============================================================================

# create percent change columns of closing price
close_prices_rolling['AAPL_percent_change'] = close_prices_rolling['AAPL_mean'].pct_change()
close_prices_rolling['FB_percent_change'] = close_prices_rolling['FB_mean'].pct_change()
close_prices_rolling['AMZN_percent_change'] = close_prices_rolling['AMZN_mean'].pct_change()
close_prices_rolling['NFLX_percent_change'] = close_prices_rolling['NFLX_mean'].pct_change()
close_prices_rolling['GOOGL_percent_change'] = close_prices_rolling['GOOGL_mean'].pct_change()
close_prices_rolling['MSFT_percent_change'] = close_prices_rolling['MSFT_mean'].pct_change()
close_prices_rolling['CCL_percent_change'] = close_prices_rolling['CCL_mean'].pct_change()
close_prices_rolling['AAL_percent_change'] = close_prices_rolling['AAL_mean'].pct_change()
close_prices_rolling['LUV_percent_change'] = close_prices_rolling['LUV_mean'].pct_change()

# create labels
labels = ['AAPL', 'FB', 'AMZN', 'NFLX', 'GOOGL', 'MSFT', 'CCL', 'AAL', 'LUV']

# create numpy array of stock price changes
movements = close_prices_rolling[['AAPL_percent_change', 
                                                        'FB_percent_change',
                                                        'AMZN_percent_change',
                                                        'NFLX_percent_change',
                                                        'GOOGL_percent_change',
                                                        'MSFT_percent_change',
                                                        'CCL_percent_change',
                                                        'AAL_percent_change',
                                                        'LUV_percent_change']].dropna(axis=0).to_numpy().transpose()

# normalized_movements = normalize(close_prices_rolling[['AAPL_percent_change', 
#                                                        'FB_percent_change',
#                                                        'AMZN_percent_change',
#                                                        'NFLX_percent_change',
#                                                        'GOOGL_percent_change',
#                                                        'MSFT_percent_change']])

# calculate the linkage: mergings
mergings = linkage(movements, method='complete')

# plot the dendrogram
plt.figure(dpi=250)
dendrogram(mergings, labels=labels, leaf_rotation=90, leaf_font_size=6)
plt.show()



# =============================================================================
# PLOT CLOSING PRICES OVER TIME OF ALL STOCKS
# =============================================================================
# fig, ax = plt.subplots(figsize = (10,5), dpi=200)
# plt.plot(close_prices_nodates)
# ax.legend(close_prices.columns)
# plt.show()

# =============================================================================
# PLOT CLOSING PRICES OVER TIME OF SINGLE STOCK W/ MEAN, STD
# =============================================================================
# fig, ax = plt.subplots(figsize = (10,5), dpi=200)
# plt.plot(close_prices_nodates['GOOGL'])
# line_mean = close_prices_nodates['GOOGL'].mean()
# line_std = close_prices_nodates['GOOGL'].std()
# plt.axhline(line_mean, ls='-', c='r')
# plt.axhline(line_mean + line_std * 3, ls='--', c='r')
# plt.axhline(line_mean - line_std * 3, ls='--', c='r')
# plt.show()

# =============================================================================
# PLOT PERCENT CHANGE IN CLOSING PRICES OVER TIME W/ MEAN, STD
# =============================================================================
# fig, ax = plt.subplots(figsize = (10,5), dpi=200)
# pct_change = close_prices_nodates['GOOGL'].rolling(window=20).aggregate(percent_change)
# plt.plot(pct_change)
# line_mean = pct_change.mean()
# line_std = pct_change.std()
# plt.axhline(line_mean, ls='-', c='r')
# plt.axhline(line_mean + line_std * 3, ls='--', c='r')
# plt.axhline(line_mean - line_std * 3, ls='--', c='r')
# plt.show()


# # fit simple regression model, predict GOOGL price based on other 5 stock prices 
# Use stock symbols to extract training data
# X = close_prices_nodates[['FB', 'AAPL', 'AMZN', 'NFLX', 'MSFT']]
# y = close_prices_nodates[['GOOGL']]

# # Split our data into training and test sets
# X_train, X_test, y_train, y_test = train_test_split(X, y, 
#                                                     train_size=.8, shuffle=False, random_state=5)

# # Fit our model and generate predictions
# model = Ridge()
# model.fit(X_train, y_train)
# predictions = model.predict(X_test)
# score = r2_score(y_test, predictions)
# print(score)

# # Visualize our predictions along with the "true" values, and print the score
# fig, ax = plt.subplots(figsize=(15, 5), dpi=200)
# ax.plot(y_test.reset_index(drop=True), color='k', lw=3)
# ax.plot(predictions, color='r', lw=2)
# plt.show()

# Download stock data then export as CSV
#data_df = yf.download("AAPL", start="2020-02-01", end="2020-03-20")
#data_df.to_csv('aapl.csv')