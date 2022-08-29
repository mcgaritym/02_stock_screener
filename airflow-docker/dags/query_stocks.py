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
    sectorEPS
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
    
    offsectorFiftyTwoWeekHigh_avg as (
    SELECT fin.sector, AVG((last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100) as offsectorFiftyTwoWeekHigh
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    AND FiftyTwoWeekHigh > 0
    GROUP BY fin.sector
    ORDER BY offsectorFiftyTwoWeekHigh DESC
    ),
    
    offindustryFiftyTwoWeekHigh_avg as (
    SELECT fin.industry, AVG((last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100) as offindustryFiftyTwoWeekHigh
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    AND FiftyTwoWeekHigh > 0
    GROUP BY fin.industry
    ORDER BY offindustryFiftyTwoWeekHigh DESC
    ),
    
    offsectorTwoHundredDayMovingAverage_avg as (
    SELECT fin.sector, AVG((last_sale - TwoHundredDayMovingAverage)/(TwoHundredDayMovingAverage)*100) as offsectorTwoHundredDayMovingAverage
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.sector != "nan" AND fin.sector IS NOT NULL
    AND fin.marketCapitalization > 0
    AND TwoHundredDayMovingAverage > 0
    GROUP BY fin.sector
    ORDER BY offsectorTwoHundredDayMovingAverage DESC
    ),
    
    offindustryTwoHundredDayMovingAverage_avg as (
    SELECT fin.industry, AVG((last_sale - TwoHundredDayMovingAverage)/(TwoHundredDayMovingAverage)*100) as offindustryTwoHundredDayMovingAverage
    FROM stock_tickers.stock_financials as fin
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    WHERE fin.industry != "nan" AND fin.industry IS NOT NULL
    AND fin.marketCapitalization > 0
    AND TwoHundredDayMovingAverage > 0
    GROUP BY fin.industry
    ORDER BY offindustryTwoHundredDayMovingAverage DESC
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
    ),
    
    value_stocks_cte AS (
    SELECT tick.Symbol, tick.Name, fin.sector, fin.industry, last_sale, (last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100 AS offFiftyTwoWeekHigh,
    (
    (CASE WHEN (trailingpe < sectorTrailingPE) OR (trailingpe < industryTrailingPE) THEN 1 ELSE 0 END) + 
    (CASE WHEN (priceToSalesRatioTTM < sectorTrailingPS) OR (priceToSalesRatioTTM < industryTrailingPS) THEN 1 ELSE 0 END) + 
    (CASE WHEN (ProfitMargin >= sectorProfitMargin) OR (ProfitMargin >= industryProfitMargin) THEN 1 ELSE 0 END) + 
    (CASE WHEN (PEGRatio <= industryPEG) OR (PEGRatio <= sectorPEG) THEN 1 ELSE 0 END) + 
    (CASE WHEN (bookvalue <= sectorBookValue) OR (bookvalue <= industryBookValue) THEN 1 ELSE 0 END) + 
    (CASE WHEN (OperatingMarginTTM >= sectorOperatingMarginTTM) OR (OperatingMarginTTM >= industryOperatingMarginTTM) THEN 1 ELSE 0 END) + 
    (CASE WHEN (ReturnOnAssetsTTM >= sectorReturnOnAssetsTTM) OR (ReturnOnAssetsTTM >= industryReturnOnAssetsTTM) THEN 1 ELSE 0 END) + 
    (CASE WHEN (ReturnOnEquityTTM >= sectorReturnOnEquityTTM) OR (ReturnOnEquityTTM >= industryReturnOnEquityTTM) THEN 1 ELSE 0 END) + 
    (CASE WHEN (PriceToBookRatio <= sectorPriceToBookRatio) OR (PriceToBookRatio <= industryPriceToBookRatio) THEN 1 ELSE 0 END) + 
    (CASE WHEN (dividendYield >= sectorDividendYield) OR (dividendYield >= industryDividendYield) THEN 1 ELSE 0 END) + 
    (CASE WHEN (ePS >= sectorEPS) OR (ePS >= industryEPS) THEN 1 ELSE 0 END) +   
    (CASE WHEN (EVToRevenue <= sectorEVToRevenue) OR (EVToRevenue <= industryEVToRevenue) THEN 1 ELSE 0 END) + 
    (CASE WHEN (revenuePerShareTTM >= sectorRevenuePerShareTTM) OR (revenuePerShareTTM >= industryRevenuePerShareTTM) THEN 1 ELSE 0 END) + 
    (CASE WHEN (quarterlyEarningsGrowthYOY >= sectorQuarterlyEarningsGrowthYOY) OR (quarterlyEarningsGrowthYOY >= industryQuarterlyEarningsGrowthYOY) THEN 1 ELSE 0 END) + 
    (CASE WHEN (quarterlyRevenueGrowthYOY >= sectorQuarterlyRevenueGrowthYOY) OR (quarterlyRevenueGrowthYOY >= industryQuarterlyRevenueGrowthYOY) THEN 1 ELSE 0 END) +     
    (CASE WHEN ((last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100 < offsectorFiftyTwoWeekHigh) OR ((last_sale - FiftyTwoWeekHigh)/(FiftyTwoWeekHigh)*100 < offindustryFiftyTwoWeekHigh) THEN 1 ELSE 0 END) + 
    (CASE WHEN ((last_sale - TwoHundredDayMovingAverage)/(TwoHundredDayMovingAverage)*100 < offsectorTwoHundredDayMovingAverage) OR ((last_sale - TwoHundredDayMovingAverage)/(TwoHundredDayMovingAverage)*100 < offindustryTwoHundredDayMovingAverage) THEN 1 ELSE 0 END)        
    ) AS relative_value_score
    
    FROM stock_tickers.stock_financials AS fin
    
    JOIN stock_tickers.stock_tickers AS tick
    ON tick.Symbol = fin.symbol
    
    JOIN sectorTrailingPE_avg 
    ON sectorTrailingPE_avg.sector = fin.sector
    
    JOIN industrytrailingPE_avg 
    ON industryTrailingPE_avg.industry = fin.industry 
    
    JOIN sectorTrailingPS_avg
    ON sectorTrailingPS_avg.sector = fin.sector
    
    JOIN industryTrailingPS_avg
    ON industryTrailingPS_avg.industry = fin.industry
    
    JOIN sectorProfitMargin_avg
    ON sectorProfitMargin_avg.sector = fin.sector  
    
    JOIN industryProfitMargin_avg
    ON industryProfitMargin_avg.industry = fin.industry 
    
    JOIN sectorPEG_avg
    ON sectorPEG_avg.sector = fin.sector
    
    JOIN industryPEG_avg
    ON industryPEG_avg.industry = fin.industry
    
    JOIN sectorBookValue_avg
    ON sectorBookValue_avg.sector = fin.sector
    
    JOIN industryBookValue_avg
    ON industryBookValue_avg.industry = fin.industry    
    
    JOIN sectorOperatingMarginTTM_avg
    ON sectorOperatingMarginTTM_avg.sector = fin.sector
    
    JOIN industryOperatingMarginTTM_avg
    ON industryOperatingMarginTTM_avg.industry = fin.industry    
    
    JOIN sectorReturnOnAssetsTTM_avg
    ON sectorReturnOnAssetsTTM_avg.sector = fin.sector
    
    JOIN industryReturnOnAssetsTTM_avg
    ON industryReturnOnAssetsTTM_avg.industry = fin.industry    
    
    JOIN sectorReturnOnEquityTTM_avg
    ON sectorReturnOnEquityTTM_avg.sector = fin.sector
    
    JOIN industryReturnOnEquityTTM_avg
    ON industryReturnOnEquityTTM_avg.industry = fin.industry    
        
    JOIN sectorPriceToBookRatio_avg
    ON sectorPriceToBookRatio_avg.sector = fin.sector
    
    JOIN industryPriceToBookRatio_avg
    ON industryPriceToBookRatio_avg.industry = fin.industry    
    
    JOIN sectorDividendYield_avg
    ON sectorDividendYield_avg.sector = fin.sector
    
    JOIN industryDividendYield_avg
    ON industryDividendYield_avg.industry = fin.industry    
              
    JOIN sectorEPS_avg
    ON sectorEPS_avg.sector = fin.sector
    
    JOIN industryEPS_avg
    ON industryEPS_avg.industry = fin.industry    
    
    JOIN sectorEVToRevenue_avg
    ON sectorEVToRevenue_avg.sector = fin.sector
    
    JOIN industryEVToRevenue_avg
    ON industryEVToRevenue_avg.industry = fin.industry    
      
    JOIN sectorRevenuePerShareTTM_avg
    ON sectorRevenuePerShareTTM_avg.sector = fin.sector
    
    JOIN industryRevenuePerShareTTM_avg
    ON industryRevenuePerShareTTM_avg.industry = fin.industry    
    
    JOIN offsectorFiftyTwoWeekHigh_avg
    ON offsectorFiftyTwoWeekHigh_avg.sector = fin.sector
    
    JOIN offindustryFiftyTwoWeekHigh_avg
    ON offindustryFiftyTwoWeekHigh_avg.industry = fin.industry    
    
    JOIN offsectorTwoHundredDayMovingAverage_avg
    ON offsectorTwoHundredDayMovingAverage_avg.sector = fin.sector
    
    JOIN offindustryTwoHundredDayMovingAverage_avg
    ON offindustryTwoHundredDayMovingAverage_avg.industry = fin.industry    
    
    JOIN sectorQuarterlyEarningsGrowthYOY_avg
    ON sectorQuarterlyEarningsGrowthYOY_avg.sector = fin.sector
    
    JOIN industryQuarterlyEarningsGrowthYOY_avg
    ON industryQuarterlyEarningsGrowthYOY_avg.industry = fin.industry  
    
    JOIN sectorQuarterlyRevenueGrowthYOY_avg
    ON sectorQuarterlyRevenueGrowthYOY_avg.sector = fin.sector 
    
    JOIN industryQuarterlyRevenueGrowthYOY_avg
    ON industryQuarterlyRevenueGrowthYOY_avg.industry = fin.industry
    
    WHERE FiftyTwoWeekHigh > 0
    ),
    
    value_stocks_ranking_cte AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY sector, industry ORDER BY relative_value_score DESC, offFiftyTwoWeekHigh ASC) AS sector_value_ranking
    FROM value_stocks_cte
    )
    
    SELECT * 
    FROM value_stocks_ranking_cte
    WHERE sector_value_ranking = 1
    ORDER BY relative_value_score DESC, offFiftyTwoWeekHigh ASC
    """,
    project_id = 'stock-screener-342515',
    credentials = service_account.Credentials.from_service_account_file(credentials))

    # drop duplicates, send to csv file
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
    undervalued_stocks = pd.read_gbq('SELECT * FROM {} ORDER BY relative_value_score DESC, offFiftyTwoWeekHigh ASC'.format('stock_tickers.undervalued_stocks'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))


    print('Undervalued Stocks Query: ', undervalued_stocks.head())

    return print("Undervalued Stocks Query Successful")
