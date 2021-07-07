# import libraries
from sqlalchemy import create_engine
from src.setup import config
from mysql import connector
import boto3
import logging


# set up database connection (credentials from config file)
class Setup:

    def __init__(self, user, pwd, host, port, db, service_name, region_name, aws_access_key_id, aws_secret_access_key,
                 local_host, local_user, local_pwd, local_port, local_db):
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.db = db
        self.service_name = service_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.local_host = local_host
        self.local_user = local_user
        self.local_pwd = local_pwd
        self.local_port = local_port
        self.local_db = local_db


    def rds_database(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/",
            echo=False)
        conn = engine.connect()
        conn.execute("CREATE DATABASE IF NOT EXISTS {};".format(self.db))
        conn.close()

    def rds_connect(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}",
            echo=False)
        return engine.connect()

    # def rds_close(self):
    #     engine = create_engine(
    #         f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}",
    #         echo=False)
    #     conn = engine.connect()
    #     conn.close()

    def s3_resource(self):
        s3 = boto3.resource(
            service_name=self.service_name,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)
        return s3

    def s3_client(self):
        s3_session = boto3.Session(
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)
        s3 = s3_session.client('s3')
        return s3

    def local_database(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.local_user}:{self.local_pwd}@{self.local_host}:{self.local_port}/",
            echo=False)
        conn = engine.connect()
        conn.execute("CREATE DATABASE IF NOT EXISTS {};".format(self.local_db))
        conn.close()

    def local_connect(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.local_user}:{self.local_pwd}@{self.local_host}:{self.local_port}/{self.local_db}",
            echo=False)
        return engine.connect()

    # def redshift(self):
    #     # TBD

    # def emr(self):
    #     # TBD

# call Setup class from setup_cloud.py file
# connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)
