# import config settings and Setup class
import pandas as pd
import config_cloud
from setup_cloud import Setup

# call Setup class from setup_cloud.py file
connection = Setup(config_cloud.user, config_cloud.pwd, config_cloud.host, config_cloud.port, config_cloud.db)

# create database and connection
connection.create_database()
conn = connection.create_connection()

df = pd.read_sql("""

with sector_pe_avg as (
SELECT fin.sector, AVG(fin.trailingPE) as sector_trailingPE
FROM stock_financials_CLEAN as fin
JOIN stock_tickers_CLEAN as tick
ON fin.symbol = tick.symbol
WHERE fin.sector != "nan" AND tick.`market_cap` > 0
GROUP BY fin.sector
ORDER BY AVG(fin.trailingPE) DESC
),

industry_pe_avg as (
SELECT fin.industry, AVG(fin.trailingPE) as industry_trailingPE
FROM stock_financials_CLEAN as fin
JOIN stock_tickers_CLEAN as tick
ON fin.symbol = tick.symbol
WHERE fin.industry != "nan" AND tick.`market_cap` > 0
GROUP BY fin.industry
ORDER BY AVG(fin.trailingPE) DESC
)

SELECT stock_financials_CLEAN.*, sector_pe_avg.*, industry_pe_avg.*, stock_tickers_CLEAN.`market_cap`, stock_tickers_CLEAN.`price`
FROM stock_financials_CLEAN
JOIN sector_pe_avg
ON sector_pe_avg.sector = stock_financials_CLEAN.sector
JOIN industry_pe_avg
ON industry_pe_avg.industry = stock_financials_CLEAN.industry
JOIN stock_tickers_CLEAN
ON stock_tickers_CLEAN.symbol = stock_financials_CLEAN.symbol
WHERE trailingPE IS NOT NULL
AND trailingPE < sector_trailingPE
AND trailingPE < industry_trailingPE
AND `price` < twoHundredDayAverage
ORDER BY `market_cap` DESC;
""", con=conn)

connection.close_connection()

print('debug')







