# sql libraries
import mysql.connector as msql
import mysql.connector
import os
from config import *

# connect to SQL and create database, table
def sql_connect_1():

    # for k, v in os.environ.items():
    #     print(f'{k}={v}')

    # # get current parent directory and data folder path
    # par_directory = os.path.dirname(os.getcwd())
    # print('Parent Directory: ', par_directory)
    # print('Current Parent Directory Files: ', os.listdir(par_directory))
    #
    # data_directory = os.path.join(par_directory, 'data')
    # print('Data Directory: ', data_directory)
    # # print('Current Data Directory Files: ', os.listdir(data_directory))
    #
    # dag_directory = os.path.join(par_directory, 'dags')
    # print('Dag Directory: ', dag_directory)
    # print('Current DAG Directory Files: ', os.listdir(dag_directory))
    #
    # cwd = os.getcwd()
    # print('Current Working Directory: ', cwd)
    # print('Current Working Directory Files: ', os.listdir(cwd))

    # print('Current Files in Data Directory: ', os.listdir(data_directory))

    # specify first MySQL database connection (faster executemany write feature)
    connection_1 = mysql.connector.connect(host='host.docker.internal', user=user, password=MYSQL_ROOT_PASSWORD, port=3307)
    cursor = connection_1.cursor()
    cursor.execute("DROP DATABASE IF EXISTS stocks_db;")
    cursor.execute("CREATE DATABASE stocks_db;")
    connection_1 = mysql.connector.connect(host='host.docker.internal', user=user, password=MYSQL_ROOT_PASSWORD, port=3307, database=MYSQL_DATABASE)

    # specify cursor object, change settings and create rides table
    cursor = connection_1.cursor()
    cursor.execute('set GLOBAL max_allowed_packet=1073741824')
    cursor.execute("set GLOBAL sql_mode=''")
    cursor.execute("DROP TABLE IF EXISTS tickers;")
    cursor.execute("DROP TABLE IF EXISTS fundamentals;")
    cursor.execute("DROP TABLE IF EXISTS sentiment;")

    cursor.close()

    return "Stocks Database Created"