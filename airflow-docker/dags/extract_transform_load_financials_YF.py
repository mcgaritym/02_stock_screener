# import libraries
import pandas as pd
import numpy as np
import yfinance as yf
import time
from google.cloud import bigquery
from google.oauth2 import service_account
from glob import glob
import os

# function to get financial info via yfinance API:
def extract_transform_load_financials():

    # get credentials for BigQuery API Connection:
    credentials = glob(os.path.join(os.getcwd(), '*credentials.json'))[0]

    # get from BigQuery
    tickers_list = pd.read_gbq("""SELECT Symbol FROM {} WHERE market_cap > 0 ORDER BY market_cap DESC;""".format('stock_tickers.stock_tickers'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))
    # tickers_list = tickers_list[:50]
    print(tickers_list)

    # create empty list to append dictionary values
    list_financials = []
    counter = 0

    # loop over ticker symbols
    for row in tickers_list.values:

        # get symbol
        counter = counter + 1
        symbol = row[0]
        print(symbol, counter)

        # get ticker information
        try:

            # input various stock tickers
            ticker = yf.Ticker(symbol)
            stock_quarterly_earnings = ticker.quarterly_earnings
            stock_yearly_earnings = ticker.earnings
            stock_info = ticker.info

        except Exception as e:
            print(e)
            pass

        # try to append values to dictionary list
        try:
            list_financials.append(dict({'symbol': symbol,
                                         'earnings_minus0Q': stock_quarterly_earnings['Earnings'].iloc[-1],
                                         'earnings_minus1Q': stock_quarterly_earnings['Earnings'].iloc[-2],
                                         'earnings_minus2Q': stock_quarterly_earnings['Earnings'].iloc[-3],
                                         'earnings_minus3Q': stock_quarterly_earnings['Earnings'].iloc[-4],
                                         'earnings_minus0Y': stock_yearly_earnings['Earnings'].iloc[-1],
                                         'earnings_minus1Y': stock_yearly_earnings['Earnings'].iloc[-2],
                                         'earnings_minus2Y': stock_yearly_earnings['Earnings'].iloc[-3],
                                         'revenue_minus0Q': stock_quarterly_earnings['Revenue'].iloc[-1],
                                         'revenue_minus1Q': stock_quarterly_earnings['Revenue'].iloc[-2],
                                         'revenue_minus2Q': stock_quarterly_earnings['Revenue'].iloc[-3],
                                         'revenue_minus3Q': stock_quarterly_earnings['Revenue'].iloc[-4],
                                         'revenue_minus0Y': stock_yearly_earnings['Revenue'].iloc[-1],
                                         'revenue_minus1Y': stock_yearly_earnings['Revenue'].iloc[-2],
                                         'revenue_minus2Y': stock_yearly_earnings['Revenue'].iloc[-3],
                                         'trailingPE': stock_info['trailingPE'],
                                         'trailingEps': stock_info['trailingEps'],
                                         'priceToSalesTrailing12Months': stock_info['priceToSalesTrailing12Months'],
                                         'quickRatio': stock_info['quickRatio'],
                                         'currentRatio': stock_info['currentRatio'],
                                         'debtToEquity': stock_info['debtToEquity'],
                                         'profitMargins': stock_info['profitMargins'],
                                         'pegRatio': stock_info['pegRatio'],
                                         'twoHundredDayAverage': stock_info['twoHundredDayAverage'],
                                         'fiftyDayAverage': stock_info['fiftyDayAverage'],
                                         'fiftyTwoWeekHigh': stock_info['fiftyTwoWeekHigh'],
                                         'dividendRate': stock_info['dividendRate'],
                                         'industry': stock_info['industry'],
                                         'sector': stock_info['sector']}))

            # if exception occurs, print
        except KeyError as e:
            print('KeyError: ', e)

            # if KeyError exception is trailingPE, append with trailingPE = nan
            if 'trailingPE' in str(e):

                try:

                    list_financials.append(dict({'symbol': symbol,
                                                 'earnings_minus0Q': stock_quarterly_earnings['Earnings'].iloc[-1],
                                                 'earnings_minus1Q': stock_quarterly_earnings['Earnings'].iloc[-2],
                                                 'earnings_minus2Q': stock_quarterly_earnings['Earnings'].iloc[-3],
                                                 'earnings_minus3Q': stock_quarterly_earnings['Earnings'].iloc[-4],
                                                 'earnings_minus0Y': stock_yearly_earnings['Earnings'].iloc[-1],
                                                 'earnings_minus1Y': stock_yearly_earnings['Earnings'].iloc[-2],
                                                 'earnings_minus2Y': stock_yearly_earnings['Earnings'].iloc[-3],
                                                 'revenue_minus0Q': stock_quarterly_earnings['Revenue'].iloc[-1],
                                                 'revenue_minus1Q': stock_quarterly_earnings['Revenue'].iloc[-2],
                                                 'revenue_minus2Q': stock_quarterly_earnings['Revenue'].iloc[-3],
                                                 'revenue_minus3Q': stock_quarterly_earnings['Revenue'].iloc[-4],
                                                 'revenue_minus0Y': stock_yearly_earnings['Revenue'].iloc[-1],
                                                 'revenue_minus1Y': stock_yearly_earnings['Revenue'].iloc[-2],
                                                 'revenue_minus2Y': stock_yearly_earnings['Revenue'].iloc[-3],
                                                 'trailingPE': np.nan,
                                                 'trailingEps': np.nan,
                                                 'priceToSalesTrailing12Months': stock_info['priceToSalesTrailing12Months'],
                                                 'quickRatio': stock_info['quickRatio'],
                                                 'currentRatio': stock_info['currentRatio'],
                                                 'debtToEquity': stock_info['debtToEquity'],
                                                 'profitMargins': stock_info['profitMargins'],
                                                 'pegRatio': stock_info['pegRatio'],
                                                 'twoHundredDayAverage': stock_info['twoHundredDayAverage'],
                                                 'fiftyDayAverage': stock_info['fiftyDayAverage'],
                                                 'fiftyTwoWeekHigh': stock_info['fiftyTwoWeekHigh'],
                                                 'dividendRate': stock_info['dividendRate'],
                                                 'industry': stock_info['industry'],
                                                 'sector': stock_info['sector']}))

                except:
                    pass

            elif 'Earnings' in str(e):

                try:

                    list_financials.append(dict({'symbol': symbol,
                                                 'earnings_minus0Q': np.nan,
                                                 'earnings_minus1Q': np.nan,
                                                 'earnings_minus2Q': np.nan,
                                                 'earnings_minus3Q': np.nan,
                                                 'earnings_minus0Y': np.nan,
                                                 'earnings_minus1Y': np.nan,
                                                 'earnings_minus2Y': np.nan,
                                                 'revenue_minus0Q': np.nan,
                                                 'revenue_minus1Q': np.nan,
                                                 'revenue_minus2Q': np.nan,
                                                 'revenue_minus3Q': np.nan,
                                                 'revenue_minus0Y': np.nan,
                                                 'revenue_minus1Y': np.nan,
                                                 'revenue_minus2Y': np.nan,
                                                 'trailingPE': np.nan,
                                                 'trailingEps': np.nan,
                                                 'priceToSalesTrailing12Months': np.nan,
                                                 'quickRatio': np.nan,
                                                 'currentRatio': np.nan,
                                                 'debtToEquity': np.nan,
                                                 'profitMargins': np.nan,
                                                 'pegRatio': np.nan,
                                                 'twoHundredDayAverage': stock_info['twoHundredDayAverage'],
                                                 'fiftyDayAverage': stock_info['fiftyDayAverage'],
                                                 'fiftyTwoWeekHigh': stock_info['fiftyTwoWeekHigh'],
                                                 'dividendRate': stock_info['dividendRate'],
                                                 'industry': np.nan,
                                                 'sector': np.nan}))

                except:
                    pass

            else:
                pass

        # if exception occurs, print
        except Exception as e:
            print(e)
            pass

        # pass for other exception
        except:
            time.sleep(3)
            pass

    # convert dictionary list to dataframe
    df = pd.DataFrame(list_financials, columns=['symbol',
                                                'earnings_minus0Q', 'earnings_minus1Q', 'earnings_minus2Q', 'earnings_minus3Q',
                                                'earnings_minus0Y', 'earnings_minus1Y', 'earnings_minus2Y',
                                                'revenue_minus0Q', 'revenue_minus1Q', 'revenue_minus2Q', 'revenue_minus3Q',
                                                'revenue_minus0Y', 'revenue_minus1Y', 'revenue_minus2Y',
                                                'trailingPE', 'trailingEps', 'priceToSalesTrailing12Months',
                                                'quickRatio', 'currentRatio', 'debtToEquity',
                                                'profitMargins', 'pegRatio',
                                                'twoHundredDayAverage', 'fiftyDayAverage',
                                                'fiftyTwoWeekHigh', 'dividendRate', 'industry', 'sector'])

    # using dictionary to convert specific columns
    convert_dict = {
        'earnings_minus0Q': float,
        'earnings_minus1Q': float,
        'earnings_minus2Q': float,
        'earnings_minus3Q': float,
        'earnings_minus0Y': float,
        'earnings_minus1Y': float,
        'earnings_minus2Y': float,
        'revenue_minus0Q': float,
        'revenue_minus1Q': float,
        'revenue_minus2Q': float,
        'revenue_minus3Q': float,
        'revenue_minus0Y': float,
        'revenue_minus1Y': float,
        'revenue_minus2Y': float,
        'trailingPE': float,
        'trailingEps': float,
        'priceToSalesTrailing12Months': float,
        'quickRatio': float,
        'currentRatio': float,
        'debtToEquity': float,
        'profitMargins': float,
        'pegRatio': float,
        'twoHundredDayAverage': float,
        'fiftyDayAverage': float,
        'fiftyTwoWeekHigh': float,
        'dividendRate': float,
        'industry': str,
        'sector': str,
    }

    # convert dataframe data types, replace inf to nan
    df_financials = df.astype(convert_dict)
    df_financials = df_financials.replace([np.inf, -np.inf], np.nan)

    # send to BigQuery
    df_financials.to_gbq(destination_table = 'stock_tickers.stock_financials',
                        project_id= 'stock-screener-342515',
                        credentials = service_account.Credentials.from_service_account_file(credentials),
                        if_exists = 'replace')

    # get from BigQuery
    df_financials = pd.read_gbq('SELECT * FROM {} LIMIT 5'.format('stock_tickers.stock_financials'),
                project_id = 'stock-screener-342515',
                credentials = service_account.Credentials.from_service_account_file(credentials))
    print(df_financials)

    return print("Financials Sent to local MySQL")
