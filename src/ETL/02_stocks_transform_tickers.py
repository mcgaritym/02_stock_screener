# import libraries
import pandas as pd
import glob
import os
import boto3
from src.setup.setup import Setup
from src.setup import config
import json
import requests
import io
from io import BytesIO, StringIO
import s3fs
from nltk.sentiment.vader import SentimentIntensityAnalyzer


# call Setup class as connection
connection = Setup(config.user, config.pwd, config.host, config.port, 'stocks',
                   config.service_name, config.region_name, config.aws_access_key_id,
                   config.aws_secret_access_key, config.local_host, config.local_user,
                   config.local_pwd, config.local_port, 'Stocks')

# create AWS s3 connection
s3_client = connection.s3_client()
s3_resource = connection.s3_resource()

# create AWS RDS connection
rds_database = connection.rds_database()
rds_database
rds = connection.rds_connect()


# function to load from s3
def s3_to_rds(bucket_name):


    # # print objects within S3 bucket
    for object in s3_resource.Bucket(bucket_name).objects.all():

        # load and read object from s3

        object_name = object.key
        table_name = object_name.split('.')[0]

        if object_name.endswith(".csv") == True:

            file = s3_client.get_object(Bucket=bucket_name, Key=object_name)
            df = pd.read_csv(file.get("Body"))
            print(df.head())

        elif object_name.endswith(".parquet") == True:

            # Read the parquet file
            buffer = io.BytesIO()
            file = s3_resource.Object(bucket_name, object_name)
            file.download_fileobj(buffer)
            df = pd.read_parquet(buffer)
            print(df.head())

        else:

            pass

        try:

            # send to RDS db/table
            df.to_sql(name=table_name, con=rds, if_exists='replace', chunksize=50000, index=False)

        except:
            print('Error: Could not send {} to RDS'.format(object_name))
            pass


s3_to_rds("stocks.bucket")

def clean_tickers():

    # drop table
    rds.execute("""DROP TABLE IF EXISTS stock_tickers_CLEAN;""")

    # duplicate table
    rds.execute("""CREATE TABLE stock_tickers_CLEAN LIKE stock_tickers;""")

    # duplicate table
    rds.execute("""ALTER TABLE stock_tickers_CLEAN DROP `Unnamed: 0`;""")

    # fill in new table with data from old table
    rds.execute("""INSERT INTO stock_tickers_CLEAN
                    SELECT * FROM stock_tickers;""")

    # clean column names
    rds.execute("""ALTER TABLE stock_tickers_CLEAN
                      RENAME COLUMN `Symbol` TO symbol,
                      RENAME COLUMN `Name` TO name,
                      RENAME COLUMN `Last Sale` TO price,
                      RENAME COLUMN `Net Change` TO net_change,
                      RENAME COLUMN `% Change` TO percent_change,
                      RENAME COLUMN `Market Cap` TO market_cap,
                      RENAME COLUMN `Country` TO country,
                      RENAME COLUMN `IPO Year` TO ipo_year,
                      RENAME COLUMN `Volume` TO volume,
                      RENAME COLUMN `Sector` TO sector,
                      RENAME COLUMN `Industry` TO industry; """)

    # update table keys
    rds.execute("""ALTER TABLE stock_tickers_CLEAN
                      MODIFY COLUMN symbol varchar(8),
                      MODIFY COLUMN market_cap BIGINT,
                      ADD PRIMARY KEY(symbol); """)

    rds.execute("""SET SQL_SAFE_UPDATES = 0""")

    rds.execute("""UPDATE  stock_tickers
    SET `Last Sale` = REPLACE(REPLACE(`Last Sale`, '$', ''), ',','')""")

    rds.execute("""ALTER TABLE stock_tickers 
    MODIFY COLUMN `Last Sale` DECIMAL(8,2);""")


# call functions
clean_financials()
clean_tickers()
