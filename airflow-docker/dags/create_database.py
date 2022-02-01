# import libraries
from config import *
from SqlConnect import SqlConnect

# function to create database if applicable, and drop tables
def create_database():

    # get class, and create connections
    stocks_connect = SqlConnect(MYSQL_HOST, MYSQL_USER, MYSQL_ROOT_PASSWORD, MYSQL_PORT, MYSQL_DATABASE)
    connection, cursor = stocks_connect.connect_mysql()

    # connect to database
    cursor.execute('set GLOBAL max_allowed_packet=1073741824')
    cursor.execute("set GLOBAL sql_mode=''")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stocks_db;")
    cursor.execute("DROP TABLE IF EXISTS tickers;")
    cursor.execute("DROP TABLE IF EXISTS fundamentals;")
    cursor.execute("DROP TABLE IF EXISTS sentiment;")
    cursor.close()

    return print("Stocks Database Created")


