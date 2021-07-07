# import libraries
import pandas as pd
from src.setup.setup import Setup
from src.setup import config
from nltk.sentiment.vader import SentimentIntensityAnalyzer

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

# clean data and load to clean RDS
def clean_financials():

    # drop table
    rds.execute("""DROP TABLE IF EXISTS stock_financials_CLEAN;""")

    # duplicate table
    rds.execute("""CREATE TABLE stock_financials_CLEAN LIKE stock_financials;""")

    # fill in new table with data from old table
    rds.execute("""INSERT INTO stock_financials_CLEAN
                    SELECT * FROM stock_financials;""")

    # update table keys
    rds.execute("""ALTER TABLE stock_financials_CLEAN
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

def clean_tickers():

    # drop table
    rds.execute("""DROP TABLE IF EXISTS stock_tickers_CLEAN;""")

    # duplicate table
    rds.execute("""CREATE TABLE stock_tickers_CLEAN LIKE stock_tickers;""")

    # fill in new table with data from old table
    rds.execute("""INSERT INTO stock_tickers_CLEAN
                    SELECT * FROM stock_tickers;""")

    # clean column names
    rds.execute("""ALTER TABLE stock_tickers_CLEAN
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
    rds.execute("""ALTER TABLE stock_tickers_CLEAN
                      MODIFY COLUMN symbol varchar(8),
                      MODIFY COLUMN market_cap BIGINT,
                      ADD PRIMARY KEY(symbol); """)

    rds.execute("""SET SQL_SAFE_UPDATES = 0""")

    rds.execute("""UPDATE  stock_tickers
    SET `Last Sale` = REPLACE(REPLACE(`Last Sale`, '$', ''), ',','')""")

    rds.execute("""ALTER TABLE stock_tickers 
    MODIFY COLUMN `Last Sale` DECIMAL(8,2);""")

def clean_sentiment():

    # get data from SQL
    df = pd.read_sql('SELECT * FROM news_sentiment WHERE headline is NOT NULL', con=rds)

    # clean headlines using vectorized operations
    pattern = r"[^a-zA-Z0-9 ]+"
    df['headlines_clean'] = df['headline'].str.replace(pattern, "", regex=True).str.strip()

    # split string
    df['headlines_length'] = df['headlines_clean'].str.split()

    # determine length and filter for headlines > 3 words
    df['headlines_length'] = df['headlines_length'].apply(lambda x: len(x))
    df = df[df['headlines_length'] > 3]
    df['sentiment'] = df['headline'].apply(SentimentIntensityAnalyzer().polarity_scores)
    df['sentiment'] = df['sentiment'].apply(lambda x: x['compound'])

    # resample daily and groupby organization
    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    df = df.set_index('date')
    df_sentiment = df.groupby('org')['sentiment'].resample('D').mean().unstack(level=0)
    df_sentiment = df_sentiment.rename(columns=lambda x: x + '_sentiment')

    # create overall sentiment column
    df_sentiment['overall_sentiment'] = df_sentiment.mean(axis=1)

    # send to SQL table
    df_sentiment.to_sql(name='news_sentiment_CLEAN', con=rds, if_exists='replace', index_label='date')


def main():
    # call functions
    clean_financials()
    clean_tickers()
    clean_sentiment()

    # print tables in RDS
    tables = pd.read_sql("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'stocks';""", con=rds)
    print(tables)

if __name__ == "__main__":
    main()




