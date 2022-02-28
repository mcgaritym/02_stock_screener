# import libraries
import os
from glob import glob
import pandas as pd
from datetime import timedelta, date, datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# function to get stock tickers, save to SQL
def extract_transform_load_tickers():

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
                        if_exists = 'replace')

    # get from BigQuery
    df_tickers = pd.read_gbq('SELECT * FROM {}'.format('stock_tickers.stock_tickers'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))
    print('Tickers BQ: ', df_tickers.head())

    return print("Tickers Sent to local MySQL")
