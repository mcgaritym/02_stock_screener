# import libraries
from sqlalchemy import create_engine
import os
from glob import glob
import pandas as pd
from datetime import timedelta, date, datetime
from config import *
from SqlConnect import SqlConnect

# function to get stock tickers, save to SQL
def extract_transform_load_tickers():

    # # get current parent directory and data folder path
    # data_directory = os.path.join(os.getcwd(), 'data')
    # print('CWD :', os.getcwd())
    # print('CWD Files :', os.listdir(os.getcwd()))
    # print('Data Directory :', data_directory)
    # print('Data Directory :', os.listdir(data_directory))


    # retrieve tripdata files
    tickers = glob(os.path.join(os.getcwd(), '*nasdaq_screener*.csv'))
    print('Tickers File: ', tickers)

    df_tickers = pd.read_csv(tickers[0])
    print(df_tickers)

    #  df_tickers = df_tickers[['Symbol', 'Name', 'Market Cap', 'Sector', 'Industry']]
    df_tickers = df_tickers.dropna(subset=['Market Cap'])
    df_tickers['Market Cap'] = df_tickers['Market Cap'].astype(str)
    df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('$', ''))
    df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('B', '0000000'))
    df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('M', '0000'))
    df_tickers['Market Cap'] = df_tickers['Market Cap'].apply(lambda x: x.replace('.', ''))
    df_tickers['Market Cap'] = df_tickers['Market Cap'].astype(int)
    df_tickers['Market Cap'] = df_tickers['Market Cap'].sort_values(ascending=False)
    df_tickers['Last Sale'] = df_tickers['Last Sale'].apply(lambda x: x.replace('$', ''))
    df_tickers['Last Sale'] = df_tickers['Last Sale'].astype(float)
    df_tickers = df_tickers.sort_values(by='Market Cap', ascending=False)

    # get class, and create connections
    stocks_connect = SqlConnect(MYSQL_HOST, MYSQL_USER, MYSQL_ROOT_PASSWORD, MYSQL_PORT, MYSQL_DATABASE)
    connection = stocks_connect.connect_sqlalchemy()

    # send to SQL
    df_tickers.to_sql(name='tickers', con=connection, if_exists="replace", chunksize=1000)
    df_tickers.to_csv('tickers_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)
    print(df_tickers.head())

    return print("Tickers Sent to local MySQL")
