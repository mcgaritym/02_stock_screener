# import config settings and Setup class
import config_local
from setup_local import Setup

# call Setup class from setup_cloud.py file
connection = Setup(config_local.user, config_local.pwd, config_local.host, config_local.port, config_local.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()

# duplicate table
conn.execute("""DROP TABLE stock_tickers_CLEAN;""")

conn.execute("""CREATE TABLE stock_tickers_CLEAN LIKE stock_tickers;""")

# fill in new table with data from old table
conn.execute("""INSERT INTO stock_tickers_CLEAN
                SELECT * FROM stock_tickers;""")

# clean column names
conn.execute("""ALTER TABLE stock_tickers_CLEAN
                  RENAME COLUMN `Symbol` TO symbol,
                  RENAME COLUMN `Name` TO name,
                  RENAME COLUMN `Last Sale` TO price,
                  RENAME COLUMN `Net Change` TO net_change,
                  RENAME COLUMN `% Change` TO percent_change,
                  RENAME COLUMN `Market Cap` TO market_cap,
                  RENAME COLUMN `Country` TO country,
                  RENAME COLUMN `IPO Year` TO ipo_year,
                  RENAME COLUMN `Volume` TO volume,
                  RENAME COLUMN `Sector` TO sector,
                  RENAME COLUMN `Industry` TO industry; """)


# update table keys
conn.execute("""ALTER TABLE stock_tickers_CLEAN
                  MODIFY COLUMN symbol varchar(8),
                  MODIFY COLUMN market_cap BIGINT,
                  ADD PRIMARY KEY(symbol); """)

conn.execute("""SET SQL_SAFE_UPDATES = 0""")

conn.execute("""UPDATE  stock_tickers
SET `Last Sale` = REPLACE(REPLACE(`Last Sale`, '$', ''), ',','')""")

conn.execute("""ALTER TABLE stock_tickers 
MODIFY COLUMN `Last Sale` DECIMAL(8,2);""")


connection.close_connection()

# print('debug')