# import libraries
import pandas as pd
import os
import glob
import time
import numpy as np
import yfinance as yf

# import config file and Setup class
import config
from setup import Setup

# call Setup class from setup.py file
connection = Setup(config.user, config.pwd, config.host, config.port, config.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()

# extract financial data from stock data API (based on stock ticker table)
def stock_financials(df):
    # create empty df
    df_financials = pd.DataFrame(columns=['symbol',
                                          'earnings_Q-0', 'earnings_Q-1', 'earnings_Q-2', 'earnings_Q-3',
                                          'earnings_Y-0', 'earnings_Y-1', 'earnings_Y-2', 'earnings_Y-3',
                                          'revenue_Q-0', 'revenue_Q-1', 'revenue_Q-2', 'revenue_Q-3',
                                          'revenue_Y-0', 'revenue_Y-1', 'revenue_Y-2', 'revenue_Y-3',
                                          'trailingPE', 'trailingEps', 'twoHundredDayAverage', 'fiftyDayAverage',
                                          'dividendRate', 'industry', 'sector', 'zip', 'state'])

    # loop over tickers
    for i, row in df.iterrows():

        # get symbol
        df_financials.loc[i, 'symbol'] = row['symbol']
        print(i, row['symbol'])

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
            df_financials.loc[i, 'state'] = np.nan

            # quarterly
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

        except:

            df_financials.loc[i, 'trailingPE'] = np.nan

        try:

            df_financials.loc[i, 'trailingEps'] = stock_info['trailingEps']

        except:

            df_financials.loc[i, 'trailingEps'] = np.nan

        try:
            df_financials.loc[i, 'zip'] = stock_info['zip']
            df_financials.loc[i, 'state'] = stock_info['state']

        except:
            df_financials.loc[i, 'zip'] = np.nan
            df_financials.loc[i, 'state'] = np.nan

        try:

            df_financials.loc[i, 'twoHundredDayAverage'] = stock_info['twoHundredDayAverage']
            df_financials.loc[i, 'fiftyDayAverage'] = stock_info['fiftyDayAverage']
            df_financials.loc[i, 'dividendRate'] = stock_info['dividendRate']
            df_financials.loc[i, 'industry'] = stock_info['industry']
            df_financials.loc[i, 'sector'] = stock_info['sector']

        except:

            df_financials.loc[i, 'twoHundredDayAverage'] = np.nan
            df_financials.loc[i, 'fiftyDayAverage'] = np.nan
            df_financials.loc[i, 'dividendRate'] = np.nan
            df_financials.loc[i, 'industry'] = np.nan
            df_financials.loc[i, 'sector'] = np.nan

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
    df_financials['state'] = df_financials['state'].astype(str)

    return df_financials


tickers_list = pd.read_sql_query("""SELECT symbol FROM stock_tickers WHERE `Market Cap` > 0;""", con=conn)
df_financials = stock_financials(tickers_list[:20])
df_financials.to_sql(name='stock_financials', con=conn, if_exists='replace')

# close connection
connection.close_connection()
