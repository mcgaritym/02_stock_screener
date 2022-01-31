# sql libraries
import mysql.connector as msql
import mysql.connector
import os
from config import *

# connect to SQL and create database, table
def sql_connect():

    # specify first MySQL database connection (faster executemany write feature)
    connection = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_ROOT_PASSWORD, port=MYSQL_PORT)
    cursor = connection.cursor()
    cursor.execute('set GLOBAL max_allowed_packet=1073741824')
    cursor.execute("set GLOBAL sql_mode=''")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stocks_db;")
    connection = mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_ROOT_PASSWORD, port=MYSQL_PORT, database=MYSQL_DATABASE)

    # specify cursor object, change settings and create rides table
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS tickers;")
    cursor.execute("DROP TABLE IF EXISTS fundamentals;")
    cursor.execute("DROP TABLE IF EXISTS sentiment;")
    cursor.close()

    return print("Stocks Database Created")
