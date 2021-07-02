# import libraries
import pandas as pd
import glob
import os
import boto3
from src.setup.setup import Setup
from src.setup import config

# call Setup class as connection
connection = Setup(config.user, config.pwd, config.host, config.port, 'stocks',
                   config.service_name, config.region_name, config.aws_access_key_id,
                   config.aws_secret_access_key)


# create AWS s3 connection
s3 = connection.s3()

# collect ride files in local directory and upload to s3
def upload_files_s3(local_filename, bucket_name):

    # get current parent directory and data folder path
    par_directory = os.path.dirname(os.path.dirname(os.getcwd()))
    data_directory = os.path.join(par_directory, 'data/raw')

    # retrieve tripdata files
    files = glob.glob(os.path.join(data_directory, local_filename))
    print(files)

    for f in files:

        try:
            # upload file to s3
            object_name = f.split("/")[-1]
            s3.Object(bucket_name, object_name).upload_file(Filename=f)
            print('{} uploaded to s3 successfully'.format(object_name))

        except:
            # error
            print('Error: Did not upload {} to s3'.format(object_name))


# call function
upload_files_s3('*tripdata.csv*', 'stocks.bucket')

# print all s3 buckets
for bucket in s3.buckets.all():
    print(bucket.name)

# # print objects within S3 bucket
for object in s3.Bucket('bikeshare.bucket').objects.all():
    print(object)
