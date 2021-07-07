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


# call Setup class as connection
connection = Setup(config.user, config.pwd, config.host, config.port, 'stocks',
                   config.service_name, config.region_name, config.aws_access_key_id,
                   config.aws_secret_access_key, config.local_host, config.local_user,
                   config.local_pwd, config.local_port, 'Stocks')

# create AWS s3 connection
s3 = connection.s3_resource()

# create local mySQL connection
connection.local_database()
my_sql = connection.local_connect()

# collect stock tickers in local SQL and upload to s3
def upload_tickers_s3(sql_table, bucket_name, object_name):

    # connect to local SQL
    df_financials = pd.read_sql(sql="SELECT * FROM {};".format(sql_table), con=my_sql)
    print(df_financials.head())

    # convert to csv in memory
    csv_buffer = StringIO()
    df_financials.to_csv(csv_buffer)

    # upload to s3 as csv
    try:
        # upload to s3
        s3 = connection.s3()
        s3.Object(bucket_name, object_name).put(Body=csv_buffer.getvalue())
        print('{} uploaded to s3 successfully'.format(object_name))

    except:
        # error
        print('Error: Did not upload {} to s3'.format(object_name))


# call function
upload_tickers_s3("stock_tickers", "stocks.bucket", "stock_tickers.csv")
#
# print all s3 buckets
for bucket in s3.buckets.all():
    print(bucket.name)

# # print objects within S3 bucket
for object in s3.Bucket('stocks.bucket').objects.all():
    print(object)