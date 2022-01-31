# import libraries
from sqlalchemy import create_engine
import mysql.connector as msql
import mysql.connector

# class for connecting to SQL
class SqlConnect:

    # instantiate
    def __init__(self, MYSQL_HOST, MYSQL_USER, MYSQL_ROOT_PASSWORD, MYSQL_PORT, MYSQL_DATABASE):
        self.MYSQL_HOST = MYSQL_HOST
        self.MYSQL_USER = MYSQL_USER
        self.MYSQL_ROOT_PASSWORD = MYSQL_ROOT_PASSWORD
        self.MYSQL_PORT = MYSQL_PORT
        self.MYSQL_DATABASE = MYSQL_DATABASE

    # connection for mysql connector
    def connect_mysql(self):

        connection = mysql.connector.connect(host=self.MYSQL_HOST,
                                             user=self.MYSQL_USER,
                                             password=self.MYSQL_ROOT_PASSWORD,
                                             port=self.MYSQL_PORT,
                                             database=self.MYSQL_DATABASE)
        cursor = connection.cursor()
        return connection, cursor

    # connection for sqlalchemy
    def connect_sqlalchemy(self):

        connection = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(user=self.MYSQL_USER,
                                                                                                password=self.MYSQL_ROOT_PASSWORD,
                                                                                                host=self.MYSQL_HOST,
                                                                                                port=self.MYSQL_PORT,
                                                                                                db=self.MYSQL_DATABASE))
        return connection
