# import libraries
import pandas as pd
import glob
import os
from src.setup.setup import Setup
from src.setup import config
import json
import requests
import io
from io import BytesIO, StringIO
import boto3
import boto3.session

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

# # Create your own session
# my_session = boto3.session.Session()

# # Now we can create low-level clients or resource clients from our custom session
# sqs = my_session.client('sqs')
# s3_resource = my_session.resource('s3')
#
# import boto3
#
# session = boto3.Session(
#     aws_access_key_id=ACCESS_KEY,
#     aws_secret_access_key=SECRET_KEY,
#     aws_session_token=SESSION_TOKEN
# )

# collect ride files in local directory and upload to s3
def upload_sentiment_s3(sql_table, bucket_name, object_name):

    # connect to local SQL
    df_sentiment = pd.read_sql(sql="SELECT * FROM {};".format(sql_table), con=my_sql)
    print(df_sentiment.head())

    # convert to parquet in memory
    parquet_buffer = BytesIO()
    df_sentiment.to_parquet(parquet_buffer)

    # upload to s3 as parquet
    try:
        # upload to s3
        s3 = connection.s3()
        s3.Object(bucket_name, object_name).put(Body=parquet_buffer.getvalue())
        print('{} uploaded to s3 successfully'.format(object_name))

    except:
        # error
        print('Error: Did not upload {} to s3'.format(object_name))


# call function
upload_sentiment_s3("news_sentiment", "stocks.bucket", "news_sentiment.parquet")

# print all s3 buckets
for bucket in s3.buckets.all():
    print(bucket.name)

# # print objects within S3 bucket
for object in s3.Bucket('stocks.bucket').objects.all():
    print(object)
