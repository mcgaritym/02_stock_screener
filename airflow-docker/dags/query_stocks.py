# load required libraries
import pandas as pd
from datetime import datetime, date
from google.oauth2 import service_account
from glob import glob
import os

# function to query SQL for undervalued stocks
def query_stocks():

    # get credentials for BigQuery API Connection:
    credentials = glob(os.path.join(os.getcwd(), '*credentials.json'))[0]
    print(credentials)

    # get from BigQuery
    undervalued_sector_stocks = pd.read_gbq("""
    with sector_pe_avg as (
    SELECT fin.sector, AVG(fin.trailingPE) as sector_trailingPE
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY AVG(fin.trailingPE) DESC
    ),
     
    sector_PS_avg as (
    SELECT fin.sector, AVG(fin.priceToSalesRatioTTM) as sector_trailingPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sector_trailingPS DESC
    ),   

    sector_PEG_avg as (
    SELECT fin.sector, AVG(fin.PEGRatio) as sector_PEG
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sector_PEG DESC
    ),  

    sector_ProfitMargin_avg as (
    SELECT fin.sector, AVG(fin.ProfitMargin) as sector_ProfitMargin
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sector_ProfitMargin DESC
    ) 

    SELECT fin.*, sector_pe_avg.*, sector_PS_avg.*, sector_PEG_avg.*, sector_ProfitMargin_avg.*, 
    tick.last_sale, (((tick.last_sale-FiftyTwoWeekHigh)/(FiftyTwoWeekHigh))*100) AS pct_change_offhigh
    FROM stock_tickers.stock_financials AS fin
    JOIN sector_pe_avg 
    ON sector_pe_avg.sector = fin.sector
    JOIN sector_PS_avg
    ON sector_PS_avg.sector = fin.sector
    JOIN sector_PEG_avg
    ON sector_PEG_avg.sector = fin.sector
    JOIN sector_ProfitMargin_avg
    ON sector_ProfitMargin_avg.sector = fin.sector  
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE trailingPE IS NOT NULL
    AND priceToSalesRatioTTM IS NOT NULL
    AND PEGRatio IS NOT NULL
    AND ProfitMargin IS NOT NULL
    AND trailingPE < sector_trailingPE
    AND priceToSalesRatioTTM < sector_trailingPS
    AND PEGRatio < sector_PEG
    AND ProfitMargin >= sector_ProfitMargin
    AND last_sale < FiftyDayMovingAverage
    AND last_sale < FiftyTwoWeekHigh
    AND marketCapitalization > 100000000000
    AND dividendYield > 0
    AND quarterlyEarningsGrowthYOY > 0
    AND quarterlyRevenueGrowthYOY > 0
    ORDER BY pct_change_offhigh ASC;
    """,
    project_id = 'stock-screener-342515',
    credentials = service_account.Credentials.from_service_account_file(credentials))

    undervalued_industry_stocks = pd.read_gbq("""
    with industry_pe_avg as (
    SELECT fin.industry, AVG(fin.trailingPE) as industry_trailingPE
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY AVG(fin.trailingPE) DESC
    ),
        
    industry_PS_avg as (
    SELECT fin.industry, AVG(fin.priceToSalesRatioTTM) as industry_trailingPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industry_trailingPS DESC
    ),   
    
    industry_PEG_avg as (
    SELECT fin.industry, AVG(fin.PEGRatio) as industry_PEG
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industry_PEG DESC
    ),  
    
    industry_ProfitMargin_avg as (
    SELECT fin.industry, AVG(fin.ProfitMargin) as industry_ProfitMargin
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industry_ProfitMargin DESC
    ) 
        
    SELECT fin.*, industry_pe_avg.*, industry_PS_avg.*, industry_PEG_avg.*, industry_ProfitMargin_avg.*, 
    tick.last_sale, (((tick.last_sale-FiftyTwoWeekHigh)/(FiftyTwoWeekHigh))*100) AS pct_change_offhigh
    FROM stock_tickers.stock_financials AS fin
    JOIN industry_pe_avg 
    ON industry_pe_avg.industry = fin.industry
    JOIN industry_PS_avg
    ON industry_PS_avg.industry = fin.industry
    JOIN industry_PEG_avg
    ON industry_PEG_avg.industry = fin.industry
    JOIN industry_ProfitMargin_avg
    ON industry_ProfitMargin_avg.industry = fin.industry  
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE trailingPE IS NOT NULL
    AND priceToSalesRatioTTM IS NOT NULL
    AND PEGRatio IS NOT NULL
    AND ProfitMargin IS NOT NULL
    AND trailingPE < industry_trailingPE
    AND priceToSalesRatioTTM < industry_trailingPS
    AND PEGRatio < industry_PEG
    AND ProfitMargin >= industry_ProfitMargin
    AND last_sale < FiftyDayMovingAverage
    AND last_sale < FiftyTwoWeekHigh
    AND marketCapitalization > 100000000000
    AND dividendYield > 0
    AND quarterlyEarningsGrowthYOY > 0
    AND quarterlyRevenueGrowthYOY > 0
    ORDER BY pct_change_offhigh ASC;
    """,
    project_id = 'stock-screener-342515',
    credentials = service_account.Credentials.from_service_account_file(credentials))


    # drop duplicates, send to csv file
    undervalued_sector_stocks = undervalued_sector_stocks.drop_duplicates(subset=['symbol'])
    undervalued_industry_stocks = undervalued_industry_stocks.drop_duplicates(subset=['symbol'])
    undervalued_sector_stocks.to_csv('undervalued_sector_stocks_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)
    undervalued_industry_stocks.to_csv('undervalued_industry_stocks_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)

    # send to BigQuery
    undervalued_sector_stocks.to_gbq(destination_table = 'stock_tickers.undervalued_sector_stocks',
                        project_id= 'stock-screener-342515',
                        credentials = service_account.Credentials.from_service_account_file(credentials),
                        if_exists = 'replace')

    undervalued_industry_stocks.to_gbq(destination_table = 'stock_tickers.undervalued_industry_stocks',
                        project_id= 'stock-screener-342515',
                        credentials = service_account.Credentials.from_service_account_file(credentials),
                        if_exists = 'replace')

    # get from BigQuery
    undervalued_sector_stocks = pd.read_gbq('SELECT * FROM {}'.format('stock_tickers.undervalued_sector_stocks'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))

    # get from BigQuery
    undervalued_industry_stocks = pd.read_gbq('SELECT * FROM {}'.format('stock_tickers.undervalued_industry_stocks'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))


    print('Undervalued Sector Stocks BQ: ', undervalued_sector_stocks.head())
    print('Undervalued Industry Stocks BQ: ', undervalued_industry_stocks.head())

    return print("Undervalued Stocks Query Successful")
