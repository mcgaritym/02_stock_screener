# import libraries
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import pandas as pd
import setup.config
from setup.setup import Setup
from io import BytesIO, StringIO

# call Setup class as connection
connection = Setup(setup.config.user, setup.config.pwd, setup.config.host, setup.config.port, 'stocks',
                   setup.config.service_name, setup.config.region_name, setup.config.aws_access_key_id,
                   setup.config.aws_secret_access_key, setup.config.local_host, setup.config.local_user,
                   setup.config.local_pwd, setup.config.local_port, 'Stocks')

# create AWS s3 connection
s3_client = connection.s3_client()
s3_resource = connection.s3_resource()

# create local mySQL connection
connection.local_database()
my_sql = connection.local_connect()

def empty_s3(bucket_name):

    # specify bucket
    bucket = s3_resource.Bucket(bucket_name)

    # delete all
    bucket.objects.all().delete()

# collect stock financials in local directory and upload to s3
def upload_financials_s3(sql_table, bucket_name, object_name):

    # connect to local SQL
    df_financials = pd.read_sql(sql="SELECT * FROM {};".format(sql_table), con=my_sql)
    print(df_financials.head())

    # convert to csv in memory
    csv_buffer = StringIO()
    df_financials.to_csv(csv_buffer)

    try:

        # upload to s3
        # s3 = connection.s3()
        s3_resource.Object(bucket_name, object_name).put(Body=csv_buffer.getvalue())
        print('{} uploaded to s3 successfully'.format(object_name))

    except:
        # error
        print('Error: Did not upload {} to s3'.format(object_name))

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
        # s3 = connection.s3()
        s3_resource.Object(bucket_name, object_name).put(Body=csv_buffer.getvalue())
        print('{} uploaded to s3 successfully'.format(object_name))

    except:
        # error
        print('Error: Did not upload {} to s3'.format(object_name))


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
        # s3 = connection.s3()
        s3_resource.Object(bucket_name, object_name).put(Body=parquet_buffer.getvalue())
        print('{} uploaded to s3 successfully'.format(object_name))

    except:
        # error
        print('Error: Did not upload {} to s3'.format(object_name))


def main():
    # call functions
    empty_s3("02.stocks.bucket")
    upload_sentiment_s3("news_sentiment", "02.stocks.bucket", "news_sentiment.parquet")
    upload_tickers_s3("stock_tickers", "02.stocks.bucket", "stock_tickers.csv")
    upload_financials_s3("stock_financials", "02.stocks.bucket", "stock_financials.csv")

    # print all s3 buckets
    for bucket in s3_resource.buckets.all():
        print(bucket.name)

    # # print objects within S3 bucket
    for object in s3_resource.Bucket('02.stocks.bucket').objects.all():
        print(object)

if __name__ == "__main__":
    main()




