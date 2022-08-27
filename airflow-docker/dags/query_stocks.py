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
    with sectorTrailingPE_avg as (
    SELECT fin.sector, AVG(fin.trailingpe) as sectorTrailingPE
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorTrailingPE DESC
    ),

    industryTrailingPE_avg as (
    SELECT fin.industry, AVG(fin.trailingpe) as industryTrailingPE
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryTrailingPE DESC
    ),
        
    sectorTrailingPS_avg as (
    SELECT fin.sector, AVG(fin.priceToSalesRatioTTM) as sectorTrailingPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorTrailingPS DESC
    ),   
    
    industryTrailingPS_avg as (
    SELECT fin.industry, AVG(fin.priceToSalesRatioTTM) as industryTrailingPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryTrailingPS DESC
    ),   
    
    sectorPEG_avg as (
    SELECT fin.sector, AVG(fin.PEGRatio) as sectorPEG
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorPEG DESC
    ),  

    industryPEG_avg as (
    SELECT fin.industry, AVG(fin.PEGRatio) as industryPEG
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryPEG DESC
    ),  

    sectorBookValue_avg as (
    SELECT fin.sector, AVG(fin.bookvalue) as sectorBookValue
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorBookValue DESC
    ),  

    industryBookValue_avg as (
    SELECT fin.industry, AVG(fin.bookvalue) as industryBookValue
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryBookValue DESC
    ),  

    sectorOperatingMarginTTM_avg as (
    SELECT fin.sector, AVG(fin.OperatingMarginTTM) as sectorOperatingMarginTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorOperatingMarginTTM DESC
    ),  

    industryOperatingMarginTTM_avg as (
    SELECT fin.industry, AVG(fin.OperatingMarginTTM) as industryOperatingMarginTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryOperatingMarginTTM DESC
    ), 

    sectorReturnOnAssetsTTM_avg as (
    SELECT fin.sector, AVG(fin.ReturnOnAssetsTTM) as sectorReturnOnAssetsTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorReturnOnAssetsTTM DESC
    ),  

    industryReturnOnAssetsTTM_avg as (
    SELECT fin.industry, AVG(fin.ReturnOnAssetsTTM) as industryReturnOnAssetsTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryReturnOnAssetsTTM DESC
    ), 

    sectorReturnOnEquityTTM_avg as (
    SELECT fin.sector, AVG(fin.ReturnOnEquityTTM) as 
    sectorReturnOnEquityTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorReturnOnEquityTTM DESC
    ),  

    industryReturnOnEquityTTM_avg as (
    SELECT fin.industry, AVG(fin.ReturnOnEquityTTM) as 
    industryReturnOnEquityTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryReturnOnEquityTTM DESC
    ), 

    sectorPriceToBookRatio_avg as (
    SELECT fin.sector, AVG(fin.PriceToBookRatio) as 
    sectorPriceToBookRatio
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorPriceToBookRatio DESC
    ),  

    industryPriceToBookRatio_avg as (
    SELECT fin.industry, AVG(fin.PriceToBookRatio) as     
    industryPriceToBookRatio
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryPriceToBookRatio DESC
    ), 

    sectorDividendYield_avg as (
    SELECT fin.sector, AVG(fin.dividendYield) as 
    sectorDividendYield
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorDividendYield DESC
    ),  

    industryDividendYield_avg as (
    SELECT fin.industry, AVG(fin.dividendYield) as     
    industryDividendYield
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryDividendYield DESC
    ), 

    sectorEPS_avg as (
    SELECT fin.sector, AVG(fin.ePS) as 
    ePS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorEPS DESC
    ),  

    industryEPS_avg as (
    SELECT fin.industry, AVG(fin.ePS) as     
    industryEPS
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryEPS DESC
    ), 


    sectorEVToRevenue_avg as (
    SELECT fin.sector, AVG(fin.EVToRevenue) as sectorEVToRevenue
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorEVToRevenue DESC
    ),
    
    industryEVToRevenue_avg as (
    SELECT fin.industry, AVG(fin.EVToRevenue) as industryEVToRevenue
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryEVToRevenue DESC
    ),

    sectorRevenuePerShareTTM_avg as (
    SELECT fin.sector, AVG(fin.revenuePerShareTTM) as sectorRevenuePerShareTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorRevenuePerShareTTM DESC
    ),
    
    industryRevenuePerShareTTM_avg as (
    SELECT fin.industry, AVG(fin.revenuePerShareTTM) as industryRevenuePerShareTTM
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryRevenuePerShareTTM DESC
    ),

    sectorFiftyTwoWeekHigh_avg as (
    SELECT fin.sector, AVG(((fin.FiftyTwoWeekHigh - tick.last_sale)/(tick.last_sale))*100) as sectorFiftyTwoWeekHigh
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorFiftyTwoWeekHigh DESC
    ),
    
    industryFiftyTwoWeekHigh_avg as (
    SELECT fin.industry, AVG(((fin.FiftyTwoWeekHigh - tick.last_sale)/(tick.last_sale))*100) as industryFiftyTwoWeekHigh
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryFiftyTwoWeekHigh DESC
    ),

    sectorTwoHundredDayMovingAverage_avg as (
    SELECT fin.sector, AVG(((fin.TwoHundredDayMovingAverage - tick.last_sale)/(tick.last_sale))*100) as sectorTwoHundredDayMovingAverage
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorTwoHundredDayMovingAverage DESC
    ),
    
    industryTwoHundredDayMovingAverage_avg as (
    SELECT fin.industry, AVG(((fin.TwoHundredDayMovingAverageTwoHundredDayMovingAverage - tick.last_sale)/(tick.last_sale))*100) as industryTwoHundredDayMovingAverage
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryTwoHundredDayMovingAverage DESC
    ),

    sectorProfitMargin_avg as (
    SELECT fin.sector, AVG(fin.ProfitMargin) as sectorProfitMargin
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorProfitMargin DESC
    ),
    
    industryProfitMargin_avg as (
    SELECT fin.industry, AVG(fin.ProfitMargin) as industryProfitMargin
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryProfitMargin DESC
    ),

    sectorQuarterlyEarningsGrowthYOY_avg as (
    SELECT fin.sector, AVG(fin.quarterlyEarningsGrowthYOY) as sectorQuarterlyEarningsGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorQuarterlyEarningsGrowthYOY DESC
    ),

    industryQuarterlyEarningsGrowthYOY_avg as (
    SELECT fin.industry, AVG(fin.quarterlyEarningsGrowthYOY) as industryQuarterlyEarningsGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryQuarterlyEarningsGrowthYOY DESC
    ),

    sectorQuarterlyRevenueGrowthYOY_avg as (
    SELECT fin.sector, AVG(fin.quarterlyRevenueGrowthYOY) as sectorQuarterlyRevenueGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.sector
    ORDER BY sectorQuarterlyRevenueGrowthYOY DESC
    ),
    
    industryQuarterlyRevenueGrowthYOY_avg as (
    SELECT fin.industry, AVG(fin.quarterlyRevenueGrowthYOY) as industryQuarterlyRevenueGrowthYOY
    FROM stock_tickers.stock_financials as fin
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    GROUP BY fin.industry
    ORDER BY industryQuarterlyRevenueGrowthYOY DESC
    )
    
    SELECT tick.Symbol, 
    SUM(
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    ) AS sector_score,
    SUM(
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    CASE WHEN THEN ELSE END
    ) AS industry_score



    FROM stock_tickers.stock_financials AS fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    JOIN sectorTrailingPE_avg 
    ON sectorTrailingPE.sector = fin.sector
    JOIN sectorTrailingPS_avg
    ON sectorTrailingPS_avg.sector = fin.sector
    JOIN sectorPEG_avg
    ON sectorPEG_avg.sector = fin.sector
    JOIN sectorProfitMargin_avg
    ON sectorProfitMargin_avg.sector = fin.sector  
    JOIN industrytrailingPE_avg 
    ON industryTrailingPE_avg.industry = fin.industry
    JOIN industryTrailingPS_avg
    ON industryTrailingPS_avg.industry = fin.industry
    JOIN industryPEG_avg
    ON industryPEG_avg.industry = fin.industry
    JOIN industryProfitMargin_avg
    ON industryProfitMargin_avg.industry = fin.industry 
    JOIN sectorQuarterlyEarningsGrowthYOY_avg
    ON sectorQuarterlyEarningsGrowthYOY_avg.sector = fin.sector
    JOIN sectorQuarterlyRevenueGrowthYOY_avg
    ON sectorQuarterlyRevenueGrowthYOY_avg.sector = fin.sector
    JOIN industryQuarterlyEarningsGrowthYOY_avg
    ON industryQuarterlyEarningsGrowthYOY_avg.industry = fin.industry
    JOIN industryQuarterlyRevenueGrowthYOY_avg
    ON industryQuarterlyRevenueGrowthYOY_avg.industry = fin.industry
    
    -- WHERE FiftyTwoWeekHigh != 0
    -- AND trailingpe IS NOT NULL
    -- AND priceToSalesRatioTTM IS NOT NULL
    -- AND PEGRatio IS NOT NULL
    -- AND ProfitMargin IS NOT NULL
    -- AND ((trailingpe < sectorTrailingPE) OR (trailingpe < industryTrailingPE))
    -- AND ((priceToSalesRatioTTM < sectorTrailingPS) OR (priceToSalesRatioTTM < industryTrailingPS))
    -- AND ((ProfitMargin >= sectorProfitMargin) OR (ProfitMargin >= industryProfitMargin))
    -- AND ((PEGRatio <= industryPEG) OR (PEGRatio <= sectorPEG))
    -- AND ((quarterlyEarningsGrowthYOY >= sectorQuarterlyEarningsGrowthYOY) OR (quarterlyEarningsGrowthYOY >= industryQuarterlyEarningsGrowthYOY))
    -- AND ((quarterlyRevenueGrowthYOY >= sectorQuarterlyRevenueGrowthYOY) OR (quarterlyRevenueGrowthYOY >= industryQuarterlyRevenueGrowthYOY))
    -- AND tick.last_sale < FiftyDayMovingAverage
    -- AND tick.last_sale < FiftyTwoWeekHigh
    -- AND fin.quarterlyEarningsGrowthYOY > 0
    -- AND fin.quarterlyRevenueGrowthYOY > 0
    -- AND fin.dividendYield > 0
    -- ORDER BY marketCapitalization DESC;
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
