# import config settings and Setup class
import pandas as pd
import numpy as np
import re
import time
from sqlalchemy import create_engine
import pymysql
import config
from setup import Setup

# call Setup class from setup.py file
connection = Setup(config.user, config.pwd, config.host, config.port, config.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()

# duplicate table
# conn.execute("""CREATE TABLE IF NOT EXISTS stock_financials_CLEAN LIKE stock_financials;""")

# # fill in new table with data from old table
# conn.execute("""INSERT INTO stock_financials_CLEAN
#                 SELECT * FROM stock_financials;""")

# update table keys
conn.execute("""ALTER TABLE stock_financials_CLEAN
                  MODIFY COLUMN symbol varchar(8),
                  MODIFY COLUMN `earnings_Q-0` BIGINT,
                  MODIFY COLUMN `earnings_Q-1` BIGINT,
                  MODIFY COLUMN `earnings_Q-2` BIGINT,
                  MODIFY COLUMN `earnings_Q-3` BIGINT,
                  MODIFY COLUMN `earnings_Y-0` BIGINT,
                  MODIFY COLUMN `earnings_Y-1` BIGINT,
                  MODIFY COLUMN `earnings_Y-2` BIGINT,
                  MODIFY COLUMN `earnings_Y-3` BIGINT,
                  MODIFY COLUMN `revenue_Q-0` BIGINT,
                  MODIFY COLUMN `revenue_Q-1` BIGINT,
                  MODIFY COLUMN `revenue_Q-2` BIGINT,
                  MODIFY COLUMN `revenue_Q-3` BIGINT,
                  MODIFY COLUMN `revenue_Y-0` BIGINT,
                  MODIFY COLUMN `revenue_Y-1` BIGINT,
                  MODIFY COLUMN `revenue_Y-2` BIGINT,
                  MODIFY COLUMN `revenue_Y-3` BIGINT, 
                  MODIFY COLUMN `trailingPE` DECIMAL,
                  MODIFY COLUMN `trailingEps` DECIMAL,
                  MODIFY COLUMN `twoHundredDayAverage` DECIMAL,
                  MODIFY COLUMN `fiftyDayAverage` DECIMAL, 
                  MODIFY COLUMN `dividendRate` DECIMAL, 
                  ADD PRIMARY KEY(symbol); """)

connection.close_connection()