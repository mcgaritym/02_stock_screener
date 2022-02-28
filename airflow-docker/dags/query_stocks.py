# load required libraries
import pandas as pd
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account
from glob import glob
import os

# function to query SQL for undervalued stocks
def query_stocks():

    # get credentials for BigQuery API Connection:
    credentials = glob(os.path.join(os.getcwd(), '*credentials.json'))[0]
    print(credentials)

    # get from BigQuery
    undervalued_stocks = pd.read_gbq("""
    with sector_pe_avg as (
    SELECT fin.sector, AVG(fin.trailingPE) as sector_trailingPE
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers as tick
    ON fin.symbol = tick.Symbol
    WHERE fin.sector != "nan" AND tick.market_cap > 0
    GROUP BY fin.sector
    ORDER BY AVG(fin.trailingPE) DESC
    ),
    
    sector_PS_avg as (
    SELECT fin.sector, AVG(fin.priceToSalesTrailing12Months) as sector_trailingPS
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers as tick
    ON fin.symbol = tick.Symbol
    WHERE fin.sector != "nan" AND tick.market_cap > 0
    GROUP BY fin.sector
    ORDER BY AVG(fin.priceToSalesTrailing12Months) DESC
    )
    
    SELECT fin.*, sector_pe_avg.*, sector_PS_avg.*, tick.Name, tick.market_cap, tick.last_sale
    FROM stock_tickers.stock_financials AS fin
    JOIN sector_pe_avg 
    ON sector_pe_avg.sector = fin.sector
    JOIN sector_PS_avg
    ON sector_PS_avg.sector = fin.sector
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE trailingPE IS NOT NULL
    AND priceToSalesTrailing12Months IS NOT NULL
    AND trailingPE < sector_trailingPE
    AND priceToSalesTrailing12Months < sector_trailingPS
    AND last_sale < fiftyDayAverage
    AND last_sale < fiftyTwoWeekHigh*(.80)
    AND market_cap > 100000000000
    AND Country = "United States"
    AND dividendRate > 0
    ORDER BY market_cap DESC;
    """,
    project_id = 'stock-screener-342515',
    credentials = service_account.Credentials.from_service_account_file(credentials))

    # drop duplicates, send to csv file
    undervalued_stocks = undervalued_stocks.drop_duplicates(subset=['symbol'])
    undervalued_stocks.to_csv('undervalued_stocks_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)
    print(undervalued_stocks)

    # send to BigQuery
    undervalued_stocks.to_gbq(destination_table = 'stock_tickers.undervalued_stocks',
                        project_id= 'stock-screener-342515',
                        credentials = service_account.Credentials.from_service_account_file(credentials),
                        if_exists = 'replace')

    # get from BigQuery
    undervalued_stocks = pd.read_gbq('SELECT * FROM {}'.format('stock_tickers.undervalued_stocks'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))

    print('Undervalued Stocks BQ: ', undervalued_stocks.head())

    return print("Undervalued Stocks Query Successful")
