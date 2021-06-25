#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 25 13:43:40 2021

@author: mcgaritym
"""

import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import glob
import pymysql
import time
import yfinance as yf
import numpy as np
import mysql.connector
import sys
import boto3
import os

# credentials
user = 'root'
pwd = "Nalgene09!"
host = 'localhost'
port= int(3306)
db = 'stocks_db' 

# connect to existing MySQL database
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}",
    echo=False)
conn = engine.connect()

# get data from MySQL database
tickers_df = pd.read_sql_query("""SELECT * FROM stock_tickers;""", con=engine)
conn.close()


import mysql.connector
import sys
import boto3
import os

ENDPOINT="stocks-database.cj4j9csypvlc.us-east-2.rds.amazonaws.com"
PORT="3306"
USR="admin"
REGION="us-east-2c"
DBNAME="stocks-database"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

#gets the credentials from .aws/credentials
session = boto3.Session(profile_name='default')
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USR, Region=REGION)

try:
    conn =  mysql.connector.connect(host=ENDPOINT, user=USR, passwd='Nalgene09!', port=PORT, database=DBNAME)
    cur = conn.cursor()
    cur.execute("""SELECT now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))  