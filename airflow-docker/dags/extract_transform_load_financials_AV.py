# import libraries
import requests
import sys
import time
import pandas as pd
from config import *
from google.oauth2 import service_account
from glob import glob
import os
import numpy as np
from airflow import AirflowException


# function to get financial info via yfinance API:
def extract_transform_load_financials_AV():

    # get credentials for BigQuery API Connection:
    credentials = glob(os.path.join(os.getcwd(), '*credentials.json'))[0]

    # get from BigQuery
    tickers_list = pd.read_gbq("""SELECT Symbol FROM {} WHERE market_cap > 0 ORDER BY market_cap DESC;""".format(
        'stock_tickers.stock_tickers'),
                               project_id='stock-screener-342515',
                               credentials=service_account.Credentials.from_service_account_file(credentials))
    # tickers_list = tickers_list[:300]
    print(tickers_list)

    # create empty list to append dictionary values
    list_financials = []
    counter = 0

    # loop over ticker symbols
    for row in tickers_list.values:

        # get symbol
        counter += 1
        symbol = row[0]
        print(symbol, counter)

        # get ticker information
        try:

            # get url, connect
            time.sleep(0.8)
            url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol={}&apikey={}'.format(symbol, API_KEY)
            r = requests.get(url)

            # grab financials for list of tickers
            data = r.json()
            print(data)

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
                                            'FiftyTwoWeekHigh': data['52WeekHigh'],
                                            'FiftyTwoWeekLow': data['52WeekLow'],
                                            'FiftyDayMovingAverage': data['50DayMovingAverage'],
                                            'ProfitMargin': data['ProfitMargin'],
                                            'PEGRatio': data['PEGRatio']
                                            }))

        # if exception occurs, print and sleep for 60 sec
        except KeyError as e:

            # print exception, sleep for 1 minute
            print(sys.exc_info()[0])
            print('First Exception Error: ', e)
            time.sleep(2)
            continue

    # convert dictionary list to dataframe
    df = pd.DataFrame(list_financials, columns=['symbol', 'name', 'country', 'sector', 'industry',
                                                'marketCapitalization', 'peRatio', 'trailingPE',
                                                'forwardPE', 'priceToSalesRatioTTM', 'dividendYield',
                                                'dividendDate', 'exDividendDate', 'ePS', 'revenuePerShareTTM',
                                                'quarterlyEarningsGrowthYOY', 'quarterlyRevenueGrowthYOY',
                                                'FiftyTwoWeekHigh', 'FiftyTwoWeekLow', 'FiftyDayMovingAverage',
                                                'ProfitMargin', 'PEGRatio'])

    # replace empty and None values
    df.replace('None', np.nan, inplace=True)
    df.replace('-', np.nan, inplace=True)

    # send to BigQuery
    df.to_gbq(destination_table = 'stock_tickers.stock_financials',
                        project_id= 'stock-screener-342515',
                        credentials = service_account.Credentials.from_service_account_file(credentials),
                        table_schema=[
                                    {'name': 'symbol', 'type': 'STRING'},
                                    {'name': 'name', 'type': 'STRING'},
                                    {'name': 'country', 'type': 'STRING'},
                                    {'name': 'sector', 'type': 'STRING'},
                                    {'name': 'industry', 'type': 'STRING'},
                                    {'name': 'marketCapitalization', 'type': 'NUMERIC'},
                                    {'name': 'peRatio', 'type': 'NUMERIC'},
                                    {'name': 'trailingPE', 'type': 'NUMERIC'},
                                    {'name': 'forwardPE', 'type': 'NUMERIC'},
                                    {'name': 'priceToSalesRatioTTM', 'type': 'NUMERIC'},
                                    {'name': 'dividendYield', 'type': 'NUMERIC'},
                                    {'name': 'dividendDate', 'type': 'DATE'},
                                    {'name': 'exDividendDate', 'type': 'DATE'},
                                    {'name': 'ePS', 'type': 'NUMERIC'},
                                    {'name': 'revenuePerShareTTM', 'type': 'NUMERIC'},
                                    {'name': 'quarterlyEarningsGrowthYOY', 'type': 'NUMERIC'},
                                    {'name': 'quarterlyRevenueGrowthYOY', 'type': 'NUMERIC'},
                                    {'name': 'FiftyTwoWeekHigh', 'type': 'NUMERIC'},
                                    {'name': 'FiftyTwoWeekLow', 'type': 'NUMERIC'},
                                    {'name': 'FiftyDayMovingAverage', 'type': 'NUMERIC'},
                                    {'name': 'ProfitMargin', 'type': 'NUMERIC'},
                                    {'name': 'PEGRatio', 'type': 'NUMERIC'}

                        ],
                        if_exists = 'replace')

    # get from BigQuery
    df_financials = pd.read_gbq('SELECT * FROM {} LIMIT 5'.format('stock_tickers.stock_financials'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))

    print(df_financials)

    return print("Financials Sent to BigQuery")