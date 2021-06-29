# import libraries
import pandas as pd
import os
import glob

# import config settings and Setup class
import config_cloud
from setup_cloud import Setup


# call Setup class from setup_cloud.py file
connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()


# extract from news sentiment
def get_news():

    # get current parent directory and data folder path
    par_directory = os.path.dirname(os.path.dirname(os.getcwd()))
    print(par_directory)
    data_directory = os.path.join(par_directory, 'data/raw')
    print(data_directory)

    # specify file names
    files_headlines = glob.glob(os.path.join(data_directory, '*fin_news_headlines*.csv'))

    ## create empty dataframe, loop over files and concatenate data to dataframe. next, reset index and print tail
    for f in files_headlines:

        # read into dataframe
        data = pd.read_csv(f, parse_dates = ['date'])
        print(len(data))

        # # clean news using function
        # data = clean_news(data)

        # append to SQL table
        data.to_sql(name='news_sentiment', con=conn, if_exists='append', index=False)

df_news = get_news()

# close connection
connection.close_connection()