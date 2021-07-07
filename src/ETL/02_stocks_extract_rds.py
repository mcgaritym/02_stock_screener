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
from sqlalchemy import text, bindparam

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

def empty_rds(db_name):

    # print current tables
    tables = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", con=rds, params={db_name})
    # tables = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = :s", con=rds, params={'s': db_name})
    tables_list = list(tables['TABLE_NAME'])
    print(tables_list)

    # drop tables
    for table_name in tables_list:
        query = "DROP TABLE " + table_name + ";"
        rds.execute(query)

    # print current tables
    tables = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", con=rds, params={db_name})
    print(tables)

# function to load from s3
def s3_to_rds(bucket_name):

    # print objects within S3 bucket
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

# empty rds tables
empty_rds('stocks')

# load s3 objects to rds table
s3_to_rds("stocks.bucket")

# print tables in RDS
tables = pd.read_sql("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'stocks';""", con=rds)
print(tables)
