# extract from news sentiment
def get_news():

    # get current parent directory and data folder path
    par_directory = os.path.dirname(os.getcwd())
    data_directory = os.path.join(par_directory, 'data/raw')

    # specify file names
    files_headlines = glob.glob(os.path.join(data_directory, '*fin_news_headlines*.csv'))

    ## create empty dataframe, loop over files and concatenate data to dataframe. next, reset index and print tail
    for f in files_headlines:

        # read into dataframe
        data = pd.read_csv(f, parse_dates = ['date'])
        print(len(data))

        # clean news
        data = clean_news(data)

        # append to SQL table
        data.to_sql(name='news_sentiment', con=engine, if_exists='append', index=False)

df_news = get_news()

# conn.close()

# load to SQL database (staging)