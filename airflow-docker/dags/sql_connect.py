# import libraries
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.sqlalchemy import URL

# class for connecting to SQL
class SnowflakeConnect:

    # instantiate
    def __init__(self, SNOW_ACCOUNT, SNOW_USER, SNOW_PASSWORD, SNOW_HOST, SNOW_ROLE, SNOW_WH, SNOW_DB, SNOW_SCHEMA):
        self.SNOW_ACCOUNT = SNOW_ACCOUNT
        self.SNOW_USER = SNOW_USER
        self.SNOW_PASSWORD = SNOW_PASSWORD
        self.SNOW_HOST = SNOW_HOST
        self.SNOW_ROLE= SNOW_ROLE
        self.SNOW_WH = SNOW_WH
        self.SNOW_DB = SNOW_DB
        self.SNOW_SCHEMA = SNOW_SCHEMA

    # connection for mysql connector
    def connect_snow(self):

        connection = snowflake.connector.connect(
            acount = self.SNOW_ACCOUNT,
            user = self.SNOW_USER,
            password = self.SNOW_PASSWORD,
            host = self.SNOW_HOST,
            role = self.SNOW_ROLE,
            warehouse = self.SNOW_WH,
            database = self.SNOW_DB,
            schema = self.SNOW_SCHEMA
            )

        cursor = connection.cursor()
        return connection, cursor

    # connection for sqlalchemy
    def connect_sqlalchemy(self):

        connection = create_engine(URL(
            acount = self.SNOW_ACCOUNT,
            user = self.SNOW_USER,
            password = self.SNOW_PASSWORD,
            host = self.SNOW_HOST,
            role = self.SNOW_ROLE,
            warehouse = self.SNOW_WH,
            database = self.SNOW_DB,
            schema = self.SNOW_SCHEMA
        ))

        return connection
