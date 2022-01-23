# load required libraries
from sqlalchemy import create_engine
import os
from glob import glob
import pandas as pd
import time
from datetime import timedelta, date, datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# get stock tickers, save to SQL
def get_tickers():

    # get current parent directory and data folder path
    par_directory = os.path.dirname(os.getcwd())
    data_directory = os.path.join(par_directory, 'data')

    # retrieve tripdata files
    files = glob(os.path.join(data_directory, '*nasdaq_screener*.csv'))

    for f in files:
        df_tickers = pd.read_csv(f)

    #     df_tickers = df_tickers[['Symbol', 'Name', 'Market Cap', 'Sector', 'Industry']]
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

    # specify SQL credentials
    user = 'root'
    pwd = "Nalgene09!"
    host = 'localhost'
    port = int(3306)

    # specify second MySQL database connection (faster read_sql query feature)
    connection_2 = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=user, password=pwd, host=host, port=port,
                                                                      db="stocks_db"))

    # send to SQL
    df_tickers.to_sql(name='tickers', con=connection_2, if_exists="replace", chunksize=1000)
    df_tickers.to_csv('tickers_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)
    print(df_tickers.head())

    return "Tickers Created and Sent to SQL"

get_tickers()