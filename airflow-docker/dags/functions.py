# import libraries
from config import *
from sql_connect import SqlConnect
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pretty_html_table import build_table
import pandas as pd
import requests
import sys
import time
from google.oauth2 import service_account
from glob import glob
import os
import numpy as np

# function to create database if applicable, and drop tables
def create_database():

    # get class, and create connections
    stocks_connect = SqlConnect(MYSQL_HOST, MYSQL_USER, MYSQL_ROOT_PASSWORD, MYSQL_PORT, MYSQL_DATABASE)
    connection, cursor = stocks_connect.connect_mysql()

    # connect to database
    cursor.execute('set GLOBAL max_allowed_packet=1073741824')
    cursor.execute("set GLOBAL sql_mode=''")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stocks_db;")
    cursor.execute("DROP TABLE IF EXISTS tickers;")
    cursor.execute("DROP TABLE IF EXISTS fundamentals;")
    cursor.execute("DROP TABLE IF EXISTS sentiment;")
    cursor.close()

    return print("Stocks Database Created")

# function to get stock tickers, save to SQL
def extract_load_tickers():

    # retrieve stock ticker file files
    tickers = glob(os.path.join(os.getcwd(), '*nasdaq_screener*.csv'))[0]
    print('Tickers File: ', tickers)

    df_tickers = pd.read_csv(tickers)
    df_tickers = df_tickers.rename({'Last Sale': 'last_sale',
                         'Net Change': 'net_change',
                         '% Change': 'percent_change',
                         'Market Cap': 'market_cap',
                         'IPO Year': 'ipo_year'},
                        axis='columns')
    print(df_tickers.head())

    #  df_tickers = df_tickers[['Symbol', 'Name', 'Market Cap', 'Sector', 'Industry']]
    df_tickers = df_tickers.dropna(subset=['market_cap'])
    df_tickers['market_cap'] = df_tickers['market_cap'].astype(str)
    df_tickers['market_cap'] = df_tickers['market_cap'].apply(lambda x: x.replace('$', ''))
    df_tickers['market_cap'] = df_tickers['market_cap'].apply(lambda x: x.replace('B', '0000000'))
    df_tickers['market_cap'] = df_tickers['market_cap'].apply(lambda x: x.replace('M', '0000'))
    df_tickers['market_cap'] = df_tickers['market_cap'].apply(lambda x: x.replace('.', ''))
    df_tickers['market_cap'] = df_tickers['market_cap'].astype(int)
    df_tickers['market_cap'] = df_tickers['market_cap'].sort_values(ascending=False)
    df_tickers['last_sale'] = df_tickers['last_sale'].apply(lambda x: x.replace('$', ''))
    df_tickers['last_sale'] = df_tickers['last_sale'].astype(float)
    df_tickers = df_tickers.sort_values(by='market_cap', ascending=False)

    # send to csv
    df_tickers.to_csv('tickers_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)
    print('Tickers CSV: ', df_tickers.head())

    # get credentials for BigQuery API Connection:
    credentials = glob(os.path.join(os.getcwd(), '*credentials.json'))[0]
    print(credentials)

    # send to BigQuery
    df_tickers.to_gbq(destination_table = 'stock_tickers.stock_tickers',
                        project_id= 'stock-screener-342515',
                        credentials = service_account.Credentials.from_service_account_file(credentials),
                        if_exists = 'replace',
                        table_schema=[
                          {'name': 'last_sale', 'type': 'Float'},
                          {'name': 'market_cap', 'type': 'Float'},
                          {'name': 'ipo_year', 'type': 'String'},
                          {'name': 'percent_change', 'type': 'Float'},
                          {'name': 'net_change', 'type': 'Float'},
                          {'name': 'Volume', 'type': 'Float'},
                          {'name': 'Symbol', 'type': 'STRING'},
                          {'name': 'Name', 'type': 'STRING'},
                          {'name': 'Country', 'type': 'STRING'},
                          {'name': 'Sector', 'type': 'STRING'},
                          {'name': 'Industry', 'type': 'STRING'}]
                      )

    # get from BigQuery
    df_tickers = pd.read_gbq('SELECT * FROM {}'.format('stock_tickers.stock_tickers'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))
    print('Tickers BQ: ', df_tickers.head())

    return print("Tickers Sent to s3")


def extract_load_financials():

    # get credentials for BigQuery API Connection:
    credentials = glob(os.path.join(os.getcwd(), '*credentials.json'))[0]

    # get from BigQuery
    tickers_list = pd.read_gbq("""SELECT Symbol FROM {} WHERE market_cap > 0 ORDER BY market_cap DESC;""".format(
        'stock_tickers.stock_tickers'),
                               project_id='stock-screener-342515',
                               credentials=service_account.Credentials.from_service_account_file(credentials))
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
                                            'PEGRatio': data['PEGRatio'],
                                            'bookvalue': data['BookValue'],
                                            'OperatingMarginTTM': data['OperatingMarginTTM'],
                                            'ReturnOnAssetsTTM': data['ReturnOnAssetsTTM'],
                                            'ReturnOnEquityTTM': data['ReturnOnEquityTTM'],
                                            'PriceToBookRatio': data['PriceToBookRatio'],
                                            'EVToRevenue': data['EVToRevenue'],
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
                                            'TwoHundredDayMovingAverage': data['200DayMovingAverage'],
                                            'ProfitMargin': data['ProfitMargin']
                                            }))

        # if exception occurs, print and sleep for 60 sec
        except Exception as e:

            # print exception, sleep for 1 minute
            print(sys.exc_info()[0])
            print(e)
            time.sleep(5)
            continue

    # convert dictionary list to dataframe
    df = pd.DataFrame(list_financials, columns=[
                                                'symbol',
                                                'name',
                                                'country',
                                                'sector',
                                                'industry',
                                                'marketCapitalization',
                                                'peRatio',
                                                'PEGRatio',
                                                'bookvalue',
                                                'OperatingMarginTTM',
                                                'ReturnOnAssetsTTM',
                                                'ReturnOnEquityTTM',
                                                'PriceToBookRatio',
                                                'EVToRevenue',
                                                'trailingPE',
                                                'forwardPE',
                                                'priceToSalesRatioTTM',
                                                'dividendYield',
                                                'dividendDate',
                                                'exDividendDate',
                                                'ePS',
                                                'revenuePerShareTTM',
                                                'quarterlyEarningsGrowthYOY',
                                                'quarterlyRevenueGrowthYOY',
                                                'FiftyTwoWeekHigh',
                                                'FiftyTwoWeekLow',
                                                'FiftyDayMovingAverage',
                                                'TwoHundredDayMovingAverage',
                                                'ProfitMargin'
                                                ])

    # replace empty and None values
    df.replace('None', np.nan, inplace=True)
    df.replace('-', np.nan, inplace=True)

    # send to BigQuery
    df.to_gbq(destination_table = 'stock_tickers.stock_financials',
                        project_id = 'stock-screener-342515',
                        credentials = service_account.Credentials.from_service_account_file(credentials),
                        table_schema = [
                                    {'name': 'symbol', 'type': 'STRING'},
                                    {'name': 'name', 'type': 'STRING'},
                                    {'name': 'country', 'type': 'STRING'},
                                    {'name': 'sector', 'type': 'STRING'},
                                    {'name': 'industry', 'type': 'STRING'},
                                    {'name': 'marketCapitalization', 'type': 'NUMERIC'},
                                    {'name': 'peRatio', 'type': 'NUMERIC'},
                                    {'name': 'PEGRatio', 'type': 'NUMERIC'},
                                    {'name': 'bookvalue', 'type': 'NUMERIC'},
                                    {'name': 'OperatingMarginTTM', 'type': 'NUMERIC'},
                                    {'name': 'ReturnOnAssetsTTM', 'type': 'NUMERIC'},
                                    {'name': 'ReturnOnEquityTTM', 'type': 'NUMERIC'},
                                    {'name': 'PriceToBookRatio', 'type': 'NUMERIC'},
                                    {'name': 'EVToRevenue', 'type': 'NUMERIC'},
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
                                    {'name': 'TwoHundredDayMovingAverage', 'type': 'NUMERIC'},
                                    {'name': 'ProfitMargin', 'type': 'NUMERIC'}
                        ],
                        if_exists = 'replace')

    # get from BigQuery
    df_financials = pd.read_gbq('SELECT * FROM {} LIMIT 5'.format('stock_tickers.stock_financials'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))

    print(df_financials)

    return print("Financials Sent to s3")


def query_stocks():
    # get credentials for BigQuery API Connection:
    credentials = glob(os.path.join(os.getcwd(), '*credentials.json'))[0]
    print(credentials)

    # get from BigQuery
    undervalued_stocks = pd.read_gbq("""
    with sectorTrailingPE_avg as (
    SELECT fin.sector, AVG(fin.trailingpe) as sectorTrailingPE
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorTrailingPE DESC
    ),

    industryTrailingPE_avg as (
    SELECT fin.industry, AVG(fin.trailingpe) as industryTrailingPE
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryTrailingPE DESC
    ),

    sectorTrailingPS_avg as (
    SELECT fin.sector, AVG(fin.priceToSalesRatioTTM) as sectorTrailingPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorTrailingPS DESC
    ),   

    industryTrailingPS_avg as (
    SELECT fin.industry, AVG(fin.priceToSalesRatioTTM) as industryTrailingPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryTrailingPS DESC
    ),   

    sectorPEG_avg as (
    SELECT fin.sector, AVG(fin.PEGRatio) as sectorPEG
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorPEG DESC
    ),  

    industryPEG_avg as (
    SELECT fin.industry, AVG(fin.PEGRatio) as industryPEG
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryPEG DESC
    ),  

    sectorBookValue_avg as (
    SELECT fin.sector, AVG(fin.bookvalue) as sectorBookValue
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorBookValue DESC
    ),  

    industryBookValue_avg as (
    SELECT fin.industry, AVG(fin.bookvalue) as industryBookValue
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryBookValue DESC
    ),  

    sectorOperatingMarginTTM_avg as (
    SELECT fin.sector, AVG(fin.OperatingMarginTTM) as sectorOperatingMarginTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorOperatingMarginTTM DESC
    ),  

    industryOperatingMarginTTM_avg as (
    SELECT fin.industry, AVG(fin.OperatingMarginTTM) as industryOperatingMarginTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryOperatingMarginTTM DESC
    ), 

    sectorReturnOnAssetsTTM_avg as (
    SELECT fin.sector, AVG(fin.ReturnOnAssetsTTM) as sectorReturnOnAssetsTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorReturnOnAssetsTTM DESC
    ),  

    industryReturnOnAssetsTTM_avg as (
    SELECT fin.industry, AVG(fin.ReturnOnAssetsTTM) as industryReturnOnAssetsTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryReturnOnAssetsTTM DESC
    ), 

    sectorReturnOnEquityTTM_avg as (
    SELECT fin.sector, AVG(fin.ReturnOnEquityTTM) as 
    sectorReturnOnEquityTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorReturnOnEquityTTM DESC
    ),  

    industryReturnOnEquityTTM_avg as (
    SELECT fin.industry, AVG(fin.ReturnOnEquityTTM) as 
    industryReturnOnEquityTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryReturnOnEquityTTM DESC
    ), 

    sectorPriceToBookRatio_avg as (
    SELECT fin.sector, AVG(fin.PriceToBookRatio) as 
    sectorPriceToBookRatio
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorPriceToBookRatio DESC
    ),  

    industryPriceToBookRatio_avg as (
    SELECT fin.industry, AVG(fin.PriceToBookRatio) as     
    industryPriceToBookRatio
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryPriceToBookRatio DESC
    ), 

    sectorDividendYield_avg as (
    SELECT fin.sector, AVG(fin.dividendYield) as 
    sectorDividendYield
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorDividendYield DESC
    ),  

    industryDividendYield_avg as (
    SELECT fin.industry, AVG(fin.dividendYield) as     
    industryDividendYield
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryDividendYield DESC
    ), 

    sectorEPS_avg as (
    SELECT fin.sector, AVG(fin.ePS) as 
    sectorEPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorEPS DESC
    ),  

    industryEPS_avg as (
    SELECT fin.industry, AVG(fin.ePS) as     
    industryEPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryEPS DESC
    ), 


    sectorEVToRevenue_avg as (
    SELECT fin.sector, AVG(fin.EVToRevenue) as sectorEVToRevenue
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorEVToRevenue DESC
    ),

    industryEVToRevenue_avg as (
    SELECT fin.industry, AVG(fin.EVToRevenue) as industryEVToRevenue
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryEVToRevenue DESC
    ),

    sectorRevenuePerShareTTM_avg as (
    SELECT fin.sector, AVG(fin.revenuePerShareTTM) as sectorRevenuePerShareTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorRevenuePerShareTTM DESC
    ),

    industryRevenuePerShareTTM_avg as (
    SELECT fin.industry, AVG(fin.revenuePerShareTTM) as industryRevenuePerShareTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryRevenuePerShareTTM DESC
    ),

    offsectorFiftyTwoWeekHigh_avg as (
    SELECT fin.sector, AVG((last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100) as offsectorFiftyTwoWeekHigh
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    AND FiftyTwoWeekHigh > 0
    GROUP BY fin.sector
    ORDER BY offsectorFiftyTwoWeekHigh DESC
    ),

    offindustryFiftyTwoWeekHigh_avg as (
    SELECT fin.industry, AVG((last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100) as offindustryFiftyTwoWeekHigh
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    AND FiftyTwoWeekHigh > 0
    GROUP BY fin.industry
    ORDER BY offindustryFiftyTwoWeekHigh DESC
    ),

    offsectorTwoHundredDayMovingAverage_avg as (
    SELECT fin.sector, AVG((last_sale - TwoHundredDayMovingAverage)/(TwoHundredDayMovingAverage)*100) as offsectorTwoHundredDayMovingAverage
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    AND TwoHundredDayMovingAverage > 0
    GROUP BY fin.sector
    ORDER BY offsectorTwoHundredDayMovingAverage DESC
    ),

    offindustryTwoHundredDayMovingAverage_avg as (
    SELECT fin.industry, AVG((last_sale - TwoHundredDayMovingAverage)/(TwoHundredDayMovingAverage)*100) as offindustryTwoHundredDayMovingAverage
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    AND TwoHundredDayMovingAverage > 0
    GROUP BY fin.industry
    ORDER BY offindustryTwoHundredDayMovingAverage DESC
    ),


    sectorProfitMargin_avg as (
    SELECT fin.sector, AVG(fin.ProfitMargin) as sectorProfitMargin
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorProfitMargin DESC
    ),

    industryProfitMargin_avg as (
    SELECT fin.industry, AVG(fin.ProfitMargin) as industryProfitMargin
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryProfitMargin DESC
    ),

    sectorQuarterlyEarningsGrowthYOY_avg as (
    SELECT fin.sector, AVG(fin.quarterlyEarningsGrowthYOY) as sectorQuarterlyEarningsGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorQuarterlyEarningsGrowthYOY DESC
    ),

    industryQuarterlyEarningsGrowthYOY_avg as (
    SELECT fin.industry, AVG(fin.quarterlyEarningsGrowthYOY) as industryQuarterlyEarningsGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryQuarterlyEarningsGrowthYOY DESC
    ),

    sectorQuarterlyRevenueGrowthYOY_avg as (
    SELECT fin.sector, AVG(fin.quarterlyRevenueGrowthYOY) as sectorQuarterlyRevenueGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorQuarterlyRevenueGrowthYOY DESC
    ),

    industryQuarterlyRevenueGrowthYOY_avg as (
    SELECT fin.industry, AVG(fin.quarterlyRevenueGrowthYOY) as industryQuarterlyRevenueGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryQuarterlyRevenueGrowthYOY DESC
    ),

    value_stocks_cte AS (
    SELECT tick.Symbol, tick.Name, fin.sector, fin.industry, last_sale, (last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100 AS offFiftyTwoWeekHigh,
    (
    (CASE WHEN (trailingpe < sectorTrailingPE) OR (trailingpe < industryTrailingPE) THEN 1 ELSE 0 END) + 
    (CASE WHEN (priceToSalesRatioTTM < sectorTrailingPS) OR (priceToSalesRatioTTM < industryTrailingPS) THEN 1 ELSE 0 END) + 
    (CASE WHEN (ProfitMargin >= sectorProfitMargin) OR (ProfitMargin >= industryProfitMargin) THEN 1 ELSE 0 END) + 
    (CASE WHEN (PEGRatio <= industryPEG) OR (PEGRatio <= sectorPEG) THEN 1 ELSE 0 END) + 
    (CASE WHEN (bookvalue <= sectorBookValue) OR (bookvalue <= industryBookValue) THEN 1 ELSE 0 END) + 
    (CASE WHEN (OperatingMarginTTM >= sectorOperatingMarginTTM) OR (OperatingMarginTTM >= industryOperatingMarginTTM) THEN 1 ELSE 0 END) + 
    (CASE WHEN (ReturnOnAssetsTTM >= sectorReturnOnAssetsTTM) OR (ReturnOnAssetsTTM >= industryReturnOnAssetsTTM) THEN 1 ELSE 0 END) + 
    (CASE WHEN (ReturnOnEquityTTM >= sectorReturnOnEquityTTM) OR (ReturnOnEquityTTM >= industryReturnOnEquityTTM) THEN 1 ELSE 0 END) + 
    (CASE WHEN (PriceToBookRatio <= sectorPriceToBookRatio) OR (PriceToBookRatio <= industryPriceToBookRatio) THEN 1 ELSE 0 END) + 
    (CASE WHEN (dividendYield >= sectorDividendYield) OR (dividendYield >= industryDividendYield) THEN 1 ELSE 0 END) + 
    (CASE WHEN (ePS >= sectorEPS) OR (ePS >= industryEPS) THEN 1 ELSE 0 END) +   
    (CASE WHEN (EVToRevenue <= sectorEVToRevenue) OR (EVToRevenue <= industryEVToRevenue) THEN 1 ELSE 0 END) + 
    (CASE WHEN (revenuePerShareTTM >= sectorRevenuePerShareTTM) OR (revenuePerShareTTM >= industryRevenuePerShareTTM) THEN 1 ELSE 0 END) + 
    (CASE WHEN (quarterlyEarningsGrowthYOY >= sectorQuarterlyEarningsGrowthYOY) OR (quarterlyEarningsGrowthYOY >= industryQuarterlyEarningsGrowthYOY) THEN 1 ELSE 0 END) + 
    (CASE WHEN (quarterlyRevenueGrowthYOY >= sectorQuarterlyRevenueGrowthYOY) OR (quarterlyRevenueGrowthYOY >= industryQuarterlyRevenueGrowthYOY) THEN 1 ELSE 0 END) +     
    (CASE WHEN ((last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100 < offsectorFiftyTwoWeekHigh) OR ((last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100 < offindustryFiftyTwoWeekHigh) THEN 1 ELSE 0 END) + 
    (CASE WHEN ((last_sale - TwoHundredDayMovingAverage)/(TwoHundredDayMovingAverage)*100 < offsectorTwoHundredDayMovingAverage) OR ((last_sale - TwoHundredDayMovingAverage)/(TwoHundredDayMovingAverage)*100 < offindustryTwoHundredDayMovingAverage) THEN 1 ELSE 0 END)        
    ) AS relative_value_score

    FROM stock_tickers.stock_financials AS fin

    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol

    JOIN sectorTrailingPE_avg 
    ON sectorTrailingPE_avg.sector = fin.sector

    JOIN industrytrailingPE_avg 
    ON industryTrailingPE_avg.industry = fin.industry 

    JOIN sectorTrailingPS_avg
    ON sectorTrailingPS_avg.sector = fin.sector

    JOIN industryTrailingPS_avg
    ON industryTrailingPS_avg.industry = fin.industry

    JOIN sectorProfitMargin_avg
    ON sectorProfitMargin_avg.sector = fin.sector  

    JOIN industryProfitMargin_avg
    ON industryProfitMargin_avg.industry = fin.industry 

    JOIN sectorPEG_avg
    ON sectorPEG_avg.sector = fin.sector

    JOIN industryPEG_avg
    ON industryPEG_avg.industry = fin.industry

    JOIN sectorBookValue_avg
    ON sectorBookValue_avg.sector = fin.sector

    JOIN industryBookValue_avg
    ON industryBookValue_avg.industry = fin.industry    

    JOIN sectorOperatingMarginTTM_avg
    ON sectorOperatingMarginTTM_avg.sector = fin.sector

    JOIN industryOperatingMarginTTM_avg
    ON industryOperatingMarginTTM_avg.industry = fin.industry    

    JOIN sectorReturnOnAssetsTTM_avg
    ON sectorReturnOnAssetsTTM_avg.sector = fin.sector

    JOIN industryReturnOnAssetsTTM_avg
    ON industryReturnOnAssetsTTM_avg.industry = fin.industry    

    JOIN sectorReturnOnEquityTTM_avg
    ON sectorReturnOnEquityTTM_avg.sector = fin.sector

    JOIN industryReturnOnEquityTTM_avg
    ON industryReturnOnEquityTTM_avg.industry = fin.industry    

    JOIN sectorPriceToBookRatio_avg
    ON sectorPriceToBookRatio_avg.sector = fin.sector

    JOIN industryPriceToBookRatio_avg
    ON industryPriceToBookRatio_avg.industry = fin.industry    

    JOIN sectorDividendYield_avg
    ON sectorDividendYield_avg.sector = fin.sector

    JOIN industryDividendYield_avg
    ON industryDividendYield_avg.industry = fin.industry    

    JOIN sectorEPS_avg
    ON sectorEPS_avg.sector = fin.sector

    JOIN industryEPS_avg
    ON industryEPS_avg.industry = fin.industry    

    JOIN sectorEVToRevenue_avg
    ON sectorEVToRevenue_avg.sector = fin.sector

    JOIN industryEVToRevenue_avg
    ON industryEVToRevenue_avg.industry = fin.industry    

    JOIN sectorRevenuePerShareTTM_avg
    ON sectorRevenuePerShareTTM_avg.sector = fin.sector

    JOIN industryRevenuePerShareTTM_avg
    ON industryRevenuePerShareTTM_avg.industry = fin.industry    

    JOIN offsectorFiftyTwoWeekHigh_avg
    ON offsectorFiftyTwoWeekHigh_avg.sector = fin.sector

    JOIN offindustryFiftyTwoWeekHigh_avg
    ON offindustryFiftyTwoWeekHigh_avg.industry = fin.industry    

    JOIN offsectorTwoHundredDayMovingAverage_avg
    ON offsectorTwoHundredDayMovingAverage_avg.sector = fin.sector

    JOIN offindustryTwoHundredDayMovingAverage_avg
    ON offindustryTwoHundredDayMovingAverage_avg.industry = fin.industry    

    JOIN sectorQuarterlyEarningsGrowthYOY_avg
    ON sectorQuarterlyEarningsGrowthYOY_avg.sector = fin.sector

    JOIN industryQuarterlyEarningsGrowthYOY_avg
    ON industryQuarterlyEarningsGrowthYOY_avg.industry = fin.industry  

    JOIN sectorQuarterlyRevenueGrowthYOY_avg
    ON sectorQuarterlyRevenueGrowthYOY_avg.sector = fin.sector 

    JOIN industryQuarterlyRevenueGrowthYOY_avg
    ON industryQuarterlyRevenueGrowthYOY_avg.industry = fin.industry

    WHERE FiftyTwoWeekHigh > 0
    ),

    value_stocks_ranking_cte AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY sector, industry ORDER BY relative_value_score DESC, offFiftyTwoWeekHigh ASC) AS value_ranking
    FROM value_stocks_cte
    )

    SELECT * 
    FROM value_stocks_ranking_cte
    WHERE value_ranking IN (1, 2, 3)
    ORDER BY relative_value_score DESC, value_ranking DESC, offFiftyTwoWeekHigh ASC
    """,
                                     project_id='stock-screener-342515',
                                     credentials=service_account.Credentials.from_service_account_file(credentials))

    # drop duplicates, send to csv file
    undervalued_stocks.to_csv('undervalued_stocks_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv',
                              index=False)

    # send to BigQuery
    undervalued_stocks.to_gbq(destination_table='stock_tickers.undervalued_stocks',
                              project_id='stock-screener-342515',
                              credentials=service_account.Credentials.from_service_account_file(credentials),
                              if_exists='replace',
                              table_schema=[
                                  {'name': 'marketCapitalization', 'type': 'NUMERIC'},
                                  {'name': 'pe_ratio', 'type': 'Float'},
                                  {'name': 'trailingPE', 'type': 'Float'},
                                  {'name': 'forwardPE', 'type': 'Float'},
                                  {'name': 'priceToSalesRatioTTM', 'type': 'Float'},
                                  {'name': 'dividendYield', 'type': 'Float'},
                                  {'name': 'ePS', 'type': 'Float'},
                                  {'name': 'revenuePerShareTTM', 'type': 'Float'},
                                  {'name': 'quarterlyEarningsGrowthYOY', 'type': 'Float'},
                                  {'name': 'quarterlyRevenueGrowthYOY', 'type': 'Float'},
                                  {'name': 'FiftyTwoWeekHigh', 'type': 'Float'},
                                  {'name': 'FiftyTwoWeekLow', 'type': 'Float'},
                                  {'name': 'FiftyDayMovingAverage', 'type': 'Float'},
                                  {'name': 'ProfitMargin', 'type': 'Float'},
                                  {'name': 'PEGRatio', 'type': 'Float'},
                                  {'name': 'last_sale', 'type': 'Float'},
                                  {'name': 'pct_change_offhigh', 'type': 'Float'},
                                  {'name': 'sector_trailingPE', 'type': 'Float'},
                                  {'name': 'sector_trailingPS', 'type': 'Float'},
                                  {'name': 'sector_PEG', 'type': 'Float'},
                                  {'name': 'sector_ProfitMargin', 'type': 'Float'},
                                  {'name': 'industry_trailingPE', 'type': 'Float'},
                                  {'name': 'industry_trailingPS', 'type': 'Float'},
                                  {'name': 'industry_PEG', 'type': 'Float'},
                                  {'name': 'industry_ProfitMargin', 'type': 'Float'},
                                  {'name': 'sector_quarterlyEarningsGrowthYOY', 'type': 'Float'},
                                  {'name': 'sector_quarterlyRevenueGrowthYOY', 'type': 'Float'},
                                  {'name': 'industry_quarterlyEarningsGrowthYOY', 'type': 'Float'},
                                  {'name': 'industry_quarterlyRevenueGrowthYOY', 'type': 'Float'}
                              ]
                              )

    # get from BigQuery
    undervalued_stocks = pd.read_gbq(
        'SELECT * FROM {} ORDER BY relative_value_score DESC, offFiftyTwoWeekHigh ASC'.format(
            'stock_tickers.undervalued_stocks'),
        project_id='stock-screener-342515',
        credentials=service_account.Credentials.from_service_account_file(credentials))

    print('Undervalued Stocks Query: ', undervalued_stocks.head())

    return print("Undervalued Stocks Query Successful")

# function to email results
def email_results(sender, receiver, email_subject):
    # get credentials for BigQuery API Connection:
    credentials = glob(os.path.join(os.getcwd(), '*credentials.json'))[0]
    print(credentials)

    # get from BigQuery
    df = pd.read_gbq('SELECT * FROM {} ORDER BY relative_value_score DESC, offFiftyTwoWeekHigh ASC LIMIT 50'.format(
        'stock_tickers.undervalued_stocks'),
                     project_id='stock-screener-342515',
                     credentials=service_account.Credentials.from_service_account_file(credentials))

    # specify credentials
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = sender
    print(sender_email)
    receiver_email = [receiver]
    print(receiver_email)
    password = GMAIL_PASSWORD
    print(password)

    # build HTML body with dataframe
    email_html = """
    <html>
      <body>
        <p>Hello, here are today's undervalued stock picks: </p> <br>

        {0}

      </body>
    </html>
    """.format(build_table(df, 'blue_light', font_size='large'))

    message = MIMEMultipart("multipart")
    # Turn these into plain/html MIMEText objects
    part2 = MIMEText(email_html, "html")
    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part2)
    message["Subject"] = email_subject
    message["From"] = sender_email

    ## iterating through the receiver list
    for i, val in enumerate(receiver):
        message["To"] = val
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())

    return print("Stock Picks Email Successful")
