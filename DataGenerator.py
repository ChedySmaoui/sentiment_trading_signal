"""
    This python class generates a data frame with historical prices, volumes,
    and sentiment scores for a specified stock symbol.
    
    @author Chedy Smaoui
    @link https://www.linkedin.com/in/chedy-smaoui/
"""
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
from transformers import pipeline
from datetime import date

def get_news_data_with_sentiment(ticker):
    """
    Args:
        ticker (string)

    Returns:
        Pandas DataFrame: date, title, stock (ticker), finbert_sentiment_score [-1,1]
    """
    data = pd.read_csv("analyst_ratings_processed.csv")
    data = data.drop(['Unnamed: 0'], axis=1)
    data = data[data["stock"] == ticker]
    #data["date"] = pd.to_datetime(data['date'], errors='coerce').dt.date
    data["date"] = pd.to_datetime(data['date'], utc=True).dt.date
    
    # sentiment analysis
    sentiment_analysis = pipeline("sentiment-analysis", model="ProsusAI/finbert") #yiyanghkust/finbert-tone / ProsusAI/finbert
    data['finbert_sentiment'] = data['title'].apply(sentiment_analysis)
    data['finbert_sentiment_score'] = data['finbert_sentiment'].apply(lambda x: {x[0]['label']=='negative': -1, x[0]['label']=='positive': 1}.get(True, 0) * x[0]['score'])
    data = data.drop(['finbert_sentiment'], axis=1)

    data = data.drop(['title'], axis=1)
    data = data.drop(['stock'], axis=1)
    data = data.groupby(['date']).mean()
    return data

def get_start_end_dates(ticker_data):
    """
    Args:
        ticker_data (Pandas dataframe): date as index

    Returns:
        dates: start, end
    """
    start_date = ticker_data.first_valid_index()
    end_date = ticker_data.last_valid_index()
    return start_date, end_date

def get_stock_yf_data(symbol, start_date, end_date):
    stock_data = yf.download(symbol, start=start_date, end=end_date)
    return stock_data

def get_data(ticker):
    df_news = get_news_data_with_sentiment(ticker)
    start_date, end_date = get_start_end_dates(df_news)
    data = get_stock_yf_data(ticker, start_date=start_date, end_date=end_date)
    df_news = df_news.rename(columns={"date": "Date"})
    data = data.join(df_news)
    data['finbert_sentiment_score'] = data['finbert_sentiment_score'].fillna(0)#(method='ffill')
    return data