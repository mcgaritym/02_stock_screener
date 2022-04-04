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
    undervalued_stocks = pd.read_gbq("""
    with sector_pe_avg as (
    SELECT fin.sector, AVG(fin.trailingPE) as sector_trailingPE
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY AVG(fin.trailingPE) DESC
    ),
        
    sector_PS_avg as (
    SELECT fin.sector, AVG(fin.priceToSalesRatioTTM) as sector_trailingPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sector_trailingPS DESC
    ),   
    
    sector_PEG_avg as (
    SELECT fin.sector, AVG(fin.PEGRatio) as sector_PEG
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sector_PEG DESC
    ),  
    
    sector_ProfitMargin_avg as (
    SELECT fin.sector, AVG(fin.ProfitMargin) as sector_ProfitMargin
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sector_ProfitMargin DESC
    ),
    
    sector_quarterlyEarningsGrowthYOY_avg as (
    SELECT fin.sector, AVG(fin.quarterlyEarningsGrowthYOY) as sector_quarterlyEarningsGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sector_quarterlyEarningsGrowthYOY DESC
    ),
    
    sector_quarterlyRevenueGrowthYOY_avg as (
    SELECT fin.sector, AVG(fin.quarterlyRevenueGrowthYOY) as sector_quarterlyRevenueGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sector_quarterlyRevenueGrowthYOY DESC
    ),
    
    industry_pe_avg as (
    SELECT fin.industry, AVG(fin.trailingPE) as industry_trailingPE
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY AVG(fin.trailingPE) DESC
    ),
        
    industry_PS_avg as (
    SELECT fin.industry, AVG(fin.priceToSalesRatioTTM) as industry_trailingPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industry_trailingPS DESC
    ),   
    
    industry_PEG_avg as (
    SELECT fin.industry, AVG(fin.PEGRatio) as industry_PEG
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industry_PEG DESC
    ),  
    
    industry_ProfitMargin_avg as (
    SELECT fin.industry, AVG(fin.ProfitMargin) as industry_ProfitMargin
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industry_ProfitMargin DESC
    ),
    
    industry_quarterlyEarningsGrowthYOY_avg as (
    SELECT fin.industry, AVG(fin.quarterlyEarningsGrowthYOY) as industry_quarterlyEarningsGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industry_quarterlyEarningsGrowthYOY DESC
    ),
    
    industry_quarterlyRevenueGrowthYOY_avg as (
    SELECT fin.industry, AVG(fin.quarterlyRevenueGrowthYOY) as industry_quarterlyRevenueGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industry_quarterlyRevenueGrowthYOY DESC
    )
    
    SELECT fin.*, tick.last_sale, (((tick.last_sale-FiftyTwoWeekHigh)/(FiftyTwoWeekHigh))*100) AS pct_change_offhigh, 
    sector_pe_avg.sector_trailingPE, sector_PS_avg.sector_trailingPS, sector_PEG_avg.sector_PEG, sector_ProfitMargin_avg.sector_ProfitMargin,
    industry_pe_avg.industry_trailingPE, industry_PS_avg.industry_trailingPS, industry_PEG_avg.industry_PEG, industry_ProfitMargin_avg.industry_ProfitMargin, 
    sector_quarterlyEarningsGrowthYOY_avg.sector_quarterlyEarningsGrowthYOY, sector_quarterlyRevenueGrowthYOY_avg.sector_quarterlyRevenueGrowthYOY,
    industry_quarterlyEarningsGrowthYOY_avg.industry_quarterlyEarningsGrowthYOY, industry_quarterlyRevenueGrowthYOY_avg.industry_quarterlyRevenueGrowthYOY
    
    FROM stock_tickers.stock_financials AS fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    JOIN sector_pe_avg 
    ON sector_pe_avg.sector = fin.sector
    JOIN sector_PS_avg
    ON sector_PS_avg.sector = fin.sector
    JOIN sector_PEG_avg
    ON sector_PEG_avg.sector = fin.sector
    JOIN sector_ProfitMargin_avg
    ON sector_ProfitMargin_avg.sector = fin.sector  
    JOIN industry_pe_avg 
    ON industry_pe_avg.industry = fin.industry
    JOIN industry_PS_avg
    ON industry_PS_avg.industry = fin.industry
    JOIN industry_PEG_avg
    ON industry_PEG_avg.industry = fin.industry
    JOIN industry_ProfitMargin_avg
    ON industry_ProfitMargin_avg.industry = fin.industry 
    JOIN sector_quarterlyEarningsGrowthYOY_avg
    ON sector_quarterlyEarningsGrowthYOY_avg.sector = fin.sector
    JOIN sector_quarterlyRevenueGrowthYOY_avg
    ON sector_quarterlyRevenueGrowthYOY_avg.sector = fin.sector
    JOIN industry_quarterlyEarningsGrowthYOY_avg
    ON industry_quarterlyEarningsGrowthYOY_avg.industry = fin.industry
    JOIN industry_quarterlyRevenueGrowthYOY_avg
    ON industry_quarterlyRevenueGrowthYOY_avg.industry = fin.industry
    
    WHERE FiftyTwoWeekHigh != 0
    AND trailingPE IS NOT NULL
    AND priceToSalesRatioTTM IS NOT NULL
    AND PEGRatio IS NOT NULL
    AND ProfitMargin IS NOT NULL
    AND (trailingPE < sector_trailingPE) AND (trailingPE < industry_trailingPE)
    AND (priceToSalesRatioTTM < sector_trailingPS) AND (priceToSalesRatioTTM < industry_trailingPS)
    AND (PEGRatio < sector_PEG) AND (PEGRatio < industry_PEG)
    AND (ProfitMargin >= sector_ProfitMargin) AND (ProfitMargin >= industry_ProfitMargin)
    AND (quarterlyEarningsGrowthYOY >= sector_quarterlyEarningsGrowthYOY) OR (quarterlyEarningsGrowthYOY >= industry_quarterlyEarningsGrowthYOY)
    AND (quarterlyRevenueGrowthYOY >= sector_quarterlyRevenueGrowthYOY) OR (quarterlyRevenueGrowthYOY >= industry_quarterlyRevenueGrowthYOY)
    AND tick.last_sale < FiftyDayMovingAverage
    AND tick.last_sale < FiftyTwoWeekHigh
    AND fin.quarterlyEarningsGrowthYOY > 0
    AND fin.quarterlyRevenueGrowthYOY > 0
    AND dividendYield > 0
    ORDER BY marketCapitalization DESC;
    """,
    project_id = 'stock-screener-342515',
    credentials = service_account.Credentials.from_service_account_file(credentials))

    # drop duplicates, send to csv file
    undervalued_stocks = undervalued_stocks.drop_duplicates(subset=['symbol'])
    undervalued_stocks.to_csv('undervalued_stocks_' + str(datetime.now().strftime("%Y-%m-%d__%H-%M-%S")) + '.csv', index=False)

    # send to BigQuery
    undervalued_stocks.to_gbq(destination_table = 'stock_tickers.undervalued_stocks',
                        project_id= 'stock-screener-342515',
                        credentials = service_account.Credentials.from_service_account_file(credentials),
                        if_exists = 'replace',
                        table_schema=[
                                  {'name': 'marketCapitalization', 'type': 'NUMERIC'},
                                  {'name': 'pe_ratio', 'type': 'Float'},
                                  {'name': 'trailingPE', 'type': 'Float'},
                                  {'name': 'forwardPE', 'type': 'Float'},
                                  {'name': 'priceToSalesRatioTTM', 'type': 'Float'},
                                  {'name': 'dividendYield', 'type': 'Float'},
                                  {'name': 'ePS', 'type': 'Float'},
                                  {'name': 'revenuePerShareTTM', 'type': 'Float'},
                                  {'name': 'quarterlyEarningsGrowthYOY', 'type': 'Float'},
                                  {'name': 'quarterlyRevenueGrowthYOY', 'type': 'Float'},
                                  {'name': 'FiftyTwoWeekHigh', 'type': 'Float'},
                                  {'name': 'FiftyTwoWeekLow', 'type': 'Float'},
                                  {'name': 'FiftyDayMovingAverage', 'type': 'Float'},
                                  {'name': 'ProfitMargin', 'type': 'Float'},
                                  {'name': 'PEGRatio', 'type': 'Float'},
                                  {'name': 'last_sale', 'type': 'Float'},
                                  {'name': 'pct_change_offhigh', 'type': 'Float'},
                                  {'name': 'sector_trailingPE', 'type': 'Float'},
                                  {'name': 'sector_trailingPS', 'type': 'Float'},
                                  {'name': 'sector_PEG', 'type': 'Float'},
                                  {'name': 'sector_ProfitMargin', 'type': 'Float'},
                                  {'name': 'industry_trailingPE', 'type': 'Float'},
                                  {'name': 'industry_trailingPS', 'type': 'Float'},
                                  {'name': 'industry_PEG', 'type': 'Float'},
                                  {'name': 'industry_ProfitMargin', 'type': 'Float'},
                                  {'name': 'sector_quarterlyEarningsGrowthYOY', 'type': 'Float'},
                                  {'name': 'sector_quarterlyRevenueGrowthYOY', 'type': 'Float'},
                                  {'name': 'industry_quarterlyEarningsGrowthYOY', 'type': 'Float'},
                                  {'name': 'industry_quarterlyRevenueGrowthYOY', 'type': 'Float'}
                        ]
                              )

    # get from BigQuery
    undervalued_stocks = pd.read_gbq('SELECT * FROM {}'.format('stock_tickers.undervalued_stocks'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))


    print('Undervalued Stocks BQ: ', undervalued_stocks.head())

    return print("Undervalued Stocks Query Successful")
