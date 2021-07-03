# import libraries
import pandas as pd
import glob
import os
import boto3
from src.setup.setup import Setup
from src.setup import config
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# call Setup class as connection
connection = Setup(config.user, config.pwd, config.host, config.port, 'stocks',
                   config.service_name, config.region_name, config.aws_access_key_id,
                   config.aws_secret_access_key, config.local_host, config.local_user,
                   config.local_pwd, config.local_port, 'Stocks')

# create AWS s3 connection
s3 = connection.s3()

# create local mySQL connection
connection.local_database()
my_sql = connection.local_connect()

# collect stock financials in local directory and upload to s3
def upload_financials_s3(sql_table, parquet_name):

    # load into spark, convert to parquet
    conf = SparkConf()  # create the configuration
    conf.set("spark.driver.extraClassPath", "/Users/mcgaritym/server/mysql-connector-java-8.0.25/mysql-connector-java-8.0.25.jar")  # set the spark.jars
    spark = SparkSession.builder.config(conf=conf).master("local").appName("Python Spark SQL basic example").getOrCreate()
    # spark = SparkSession.builder.getOrCreate()
    df = spark.read.jdbc(url="jdbc:mysql://localhost/Stocks", table=sql_table,
                         properties={"user": "root", "password": "Nalgene09!"})
    print(df.show())
    df.write.parquet(parquet_name)



    #
    # # load to AWS s3
    #
    #
    # # get current parent directory and data folder path
    # par_directory = os.path.dirname(os.path.dirname(os.getcwd()))
    # data_directory = os.path.join(par_directory, 'data/raw')
    #
    # # retrieve tripdata files
    # files = glob.glob(os.path.join(data_directory, local_filename))
    # print(files)
    #
    # for f in files:
    #
    #     try:
    #         # upload file to s3
    #         object_name = f.split("/")[-1]
    #         s3.Object(bucket_name, object_name).upload_file(Filename=f)
    #         print('{} uploaded to s3 successfully'.format(object_name))
    #
    #     except:
    #         # error
    #         print('Error: Did not upload {} to s3'.format(object_name))
    #

# call function
upload_financials_s3("stock_financials", "parquet_test2.parquet")
#
# # print all s3 buckets
# for bucket in s3.buckets.all():
#     print(bucket.name)
#
# # # print objects within S3 bucket
# for object in s3.Bucket('bikeshare.bucket').objects.all():
#     print(object)
