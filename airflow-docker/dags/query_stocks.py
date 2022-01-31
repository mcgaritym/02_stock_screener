# load required libraries
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, date
from config import *
from SqlConnect import SqlConnect

# query SQL for undervalued stocks
def query_stocks():

    # get class, and create connections
    stocks_connect = SqlConnect(MYSQL_HOST, MYSQL_USER, MYSQL_ROOT_PASSWORD, MYSQL_PORT, MYSQL_DATABASE)
    connection = stocks_connect.connect_sqlalchemy()

    # create sector PE average common table expression, industry PE average common table expression
    undervalued_stocks = pd.read_sql_query("""
    with sector_pe_avg as (
    SELECT fin.sector, AVG(fin.trailingPE) as sector_trailingPE
    FROM stock_financials as fin
    JOIN tickers as tick
    ON fin.symbol = tick.Symbol
    WHERE fin.sector != "nan" AND tick.`Market Cap` > 0
    GROUP BY fin.sector
    ORDER BY AVG(fin.trailingPE) DESC
    ),
    
    industry_pe_avg as (
    SELECT fin.industry, AVG(fin.trailingPE) as industry_trailingPE
    FROM stock_financials as fin
    JOIN tickers as tick
    ON fin.symbol = tick.Symbol
    WHERE fin.industry != "nan" AND tick.`Market Cap` > 0
    GROUP BY fin.industry
    ORDER BY AVG(fin.trailingPE) DESC
    )
    
    SELECT stock_financials.*, sector_pe_avg.*, industry_pe_avg.*, tickers.`Market Cap`, tickers.`Last Sale`
    FROM stock_financials
    JOIN sector_pe_avg 
    ON sector_pe_avg.sector = stock_financials.sector
    JOIN industry_pe_avg
    ON industry_pe_avg.industry = stock_financials.industry
    JOIN tickers 
    ON tickers.Symbol = stock_financials.symbol
    WHERE trailingPE IS NOT NULL
    AND trailingPE < sector_trailingPE
    AND `Last Sale` < fiftyDayAverage
    AND `Last Sale` < fiftyTwoWeekHigh*(.90)
    AND `Market Cap` > 100000000000
    AND Country = "United States"
    ORDER BY `Market Cap` DESC;
    """, con=connection)

    # drop duplicates, send to csv file, and print results
    undervalued_stocks = undervalued_stocks.drop_duplicates(subset=['symbol'])
    undervalued_stocks.to_sql(name='undervalued_stocks', con=connection, if_exists="replace", chunksize=1000)
    undervalued_stocks.to_csv('undervalued_stocks_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)
    print(undervalued_stocks)

    return print("Undervalued Stocks Query Successful")
