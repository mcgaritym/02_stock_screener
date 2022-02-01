Introduction:

The purpose of this project is to a.) create an stock screener pipeline and b.) evaluate stock prices and correlations during COVID-19. The motivation for this project is due to personal interest (investing), and the fact that stocks provide a wealth of historical data to include in a data engineering problems. Also, I am interested in determining the effect of the COVID-19 pandemic on stocks in general. 

The data sources for this project include scraped news headlines from a variety of top financial news sources (for financial news sentiment), the yahoo finance API (for financial fundamentals such as stock prices, earnings, P/E ratios), and the NASDAQ website (for stock tickers, sectors, and industries). 

This project will try and answer these questions:

- Which stocks have the greatest rate of return before vs. during COVID-19?
- Which sectors have the greatest rate of return before vs. during COVID-19?
- Which industries have the greatest rate of return before vs. during COVID-19?
- How have sector stock prices changed over time during COVID-19?
- Can stock price changes be used to cluster groups of individual stocks?
- Which financial news sources have the highest/lowest headline sentiment? 
- Which stocks are correlated to financial news sentiment?
- Which sectors/industries are correlated to financial news sentiment?
- Can a data pipeline be created for performing ETL operations on stock data, pick undervalued stocks, and email the results?


TABLE OF CONTENTS:

0. Introduction
1. Data Input & Cleaning
    * 1.1. Import Libraries
    * 1.2. Create SQL Database Connections and Tables
    * 1.3. Load Stock Tickers & Send to DataFrame and SQL
    * 1.4. Load Stock Fundamentals & Send to DataFrame and SQL
    * 1.5. Load Financial & Send to DataFrame and SQL   
2. Data Processing/Cleaning
3. Data Visualization/EDA
    * 3.1. Stock Individual Returns Before vs. During COVID-19  
    * 3.2. Stock Sector Returns Before vs. During COVID-19 
    * 3.3. Stock Industry Returns Before vs. During COVID-19 
    * 3.4. Stock Sector Prices Over Time     
    * 3.5. Stock Clustering During COVID-19    
    * 3.6. Financial News Sentiment by Source
    * 3.7. Stock Correlation to Financial News Sentiment 
    * 3.8. Industry/Sector Correlation to Financial News Sentiment   
    * 3.9. Stock Screener (by P/E Ratio, Earnings)      
4. Data Pipeline Using Docker, Airflow, MySQL
    * 4.0. Summary
    * 4.1. Dockerfile and Docker Compose
    * 4.2. Rideshare DAG
    * 4.3. SQLConnect Class
    * 4.4. Create Database
    * 4.5. Extract Tickers & Financials
    * 4.6. Transform, Load Tickers & Financials to MySQL
    * 4.7. Query Stocks
    * 4.8. Email Results
5. Conclusion

