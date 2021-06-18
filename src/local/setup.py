
# import libraries
from sqlalchemy import create_engine
import config
from mysql import connector
# set up database connection (credentials from config file)
class Setup:

    def __init__(self, user, pwd, host, port, db):
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.db = db

    def create_database(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/",
            echo=False)
        conn = engine.connect()
        conn.execute("CREATE DATABASE IF NOT EXISTS Stocks;")
        conn.close()

    def create_connection(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}",
            echo=False)
        return engine.connect()

    def close_connection(self):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}",
            echo=False)
        conn = engine.connect()
        conn.close()


# call Setup class from setup.py file
connection = Setup(config.user, config.pwd, config.host, config.port, config.db)

connection.create_database()
connection.create_connection()
connection.close_connection()
