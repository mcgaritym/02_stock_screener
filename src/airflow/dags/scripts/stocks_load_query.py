# import libraries
import pandas as pd
from setup import config
from setup.setup import Setup

# call Setup class as connection
connection = Setup(config.user, config.pwd, config.host, config.port, 'stocks',
                   config.service_name, config.region_name, config.aws_access_key_id,
                   config.aws_secret_access_key, config.local_host, config.local_user,
                   config.local_pwd, config.local_port, 'Stocks')

# create AWS s3 connection
s3_client = connection.s3_client()
s3_resource = connection.s3_resource()

# create AWS RDS connection
rds = connection.rds_connect()

def query_undervalued_stocks():

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
    
    SELECT stock_financials_CLEAN.symbol, stock_financials_CLEAN.industry, stock_financials_CLEAN.sector, 
    sector_pe_avg.*, industry_pe_avg.*, stock_tickers_CLEAN.`market_cap`, stock_tickers_CLEAN.`price`
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
    """, con=rds)

    return df

def main():
    df = query_undervalued_stocks()
    print(df)

if __name__ == "__main__":
    main()