# import libraries
import requests
import sys
import time
import pandas as pd
import numpy as np
from config import *
from SqlConnect import SqlConnect

# function to get financial info via yfinance API:
def extract_transform_load_financials():

    # # get class, and create connections
    # stocks_connect = SqlConnect(MYSQL_HOST, MYSQL_USER, MYSQL_ROOT_PASSWORD, MYSQL_PORT, MYSQL_DATABASE)
    # connection = stocks_connect.connect_sqlalchemy()
    #
    # # read SQL table for stocks with positive market cap and remove 2021 IPO stocks
    # tickers_list = pd.read_sql_query(
    #     """SELECT Symbol FROM tickers WHERE `Market Cap` > 0 AND `IPO Year` != '2021' OR `IPO Year` IS NULL;""",
    #     con=connection)
    # connection.dispose()

    tickers_list = ['GOOG', 'AMZN', 'AAPL', 'CM', 'V', 'UBER', 'CO', 'LYFT', 'BAC', 'Z', 'IBM', 'COST']

    # create empty list to append dictionary values
    list_financials = []
    counter = 0

   # loop over ticker symbols
    for row in tickers_list:
    # for row in tickers_list.values:

        # get symbol
        counter += 1
        # symbol = row[0]
        symbol = row
        print(symbol, counter)

        # get ticker information
        try:

            # get url, connect
            url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol={}&apikey={}'.format(symbol, API_KEY)
            r = requests.get(url)

            # grab financials for list of tickers
            data = r.json()

            # append info
            list_financials.append(dict({
                                            'symbol': data['Symbol'],
                                            'name': data['Name'],
                                            'country': data['Country'],
                                            'sector': data['Sector'],
                                            'industry': data['Industry'],
                                            'marketCapitalization': data['MarketCapitalization'],
                                            'peRatio': data['PERatio'],
                                            'trailingPE': data['TrailingPE'],
                                            'forwardPE': data['ForwardPE'],
                                            'priceToSalesRatioTTM': data['PriceToSalesRatioTTM'],
                                            'dividendYield': data['DividendYield'],
                                            'dividendDate': data['DividendDate'],
                                            'exDividendDate': data['ExDividendDate'],
                                            'ePS': data['EPS'],
                                            'revenuePerShareTTM': data['RevenuePerShareTTM'],
                                            'quarterlyEarningsGrowthYOY': data['QuarterlyEarningsGrowthYOY'],
                                            'quarterlyRevenueGrowthYOY': data['QuarterlyRevenueGrowthYOY'],
                                            '52WeekHigh': data['52WeekHigh'],
                                            '52WeekLow': data['52WeekLow'],
                                            '50DayMovingAverage': data['50DayMovingAverage']
                                            }))

        # if exception occurs, print
        except Exception as e:

            # print exception, sleep for 1 minute
            print('Exception Error: ', e)
            time.sleep(60)

            # get url, connect
            url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol={}&apikey={}'.format(symbol, API_KEY)
            r = requests.get(url)

            # grab financials for list of tickers
            data = r.json()

            # append info
            list_financials.append(dict({
                'symbol': data['Symbol'],
                'name': data['Name'],
                'country': data['Country'],
                'sector': data['Sector'],
                'industry': data['Industry'],
                'marketCapitalization': data['MarketCapitalization'],
                'peRatio': data['PERatio'],
                'trailingPE': data['TrailingPE'],
                'forwardPE': data['ForwardPE'],
                'priceToSalesRatioTTM': data['PriceToSalesRatioTTM'],
                'dividendYield': data['DividendYield'],
                'dividendDate': data['DividendDate'],
                'exDividendDate': data['ExDividendDate'],
                'ePS': data['EPS'],
                'revenuePerShareTTM': data['RevenuePerShareTTM'],
                'quarterlyEarningsGrowthYOY': data['QuarterlyEarningsGrowthYOY'],
                'quarterlyRevenueGrowthYOY': data['QuarterlyRevenueGrowthYOY'],
                '52WeekHigh': data['52WeekHigh'],
                '52WeekLow': data['52WeekLow'],
                '50DayMovingAverage': data['50DayMovingAverage']
            }))

    # convert dictionary list to dataframe
    df = pd.DataFrame(list_financials, columns=['symbol', 'name', 'country', 'sector', 'industry',
                                                'marketCapitalization', 'peRatio', 'trailingPE',
                                                'forwardPE', 'priceToSalesRatioTTM', 'dividendYield',
                                                'dividendDate', 'exDividendDate', 'ePS', 'revenuePerShareTTM',
                                                'quarterlyEarningsGrowthYOY', 'quarterlyRevenueGrowthYOY',
                                                '52WeekHigh', '52WeekLow', '50DayMovingAverage'])


    # # using dictionary to convert specific columns
    # convert_dict = {
    #     'symbol': str,
    #     'name': str,
    #     'country': str,
    #     'sector': str,
    #     'industry': str,
    #     'dividendDate': str,
    #     'exDividendDate': str,
    #     'marketCapitalization': float,
    #     'peRatio': float,
    #     'trailingPE': float,
    #     'forwardPE': float,
    #     'priceToSalesRatioTTM': float,
    #     'dividendYield': float,
    #     'ePS': float,
    #     'revenuePerShareTTM': float,
    #     'quarterlyEarningsGrowthYOY': float,
    #     'quarterlyRevenueGrowthYOY': float,
    #     '52WeekHigh': float,
    #     '52WeekLow': float,
    #     '50DayMovingAverage': float
    #     }
    #

    # df_financials = df.astype(convert_dict)
    print(df.head())
    print(df.tail())

    # # send to SQL
    # df_financials.to_sql(name='stock_financials', con=connection, if_exists='replace')

    return print("Financials Sent to local MySQL")

extract_transform_load_financials()
