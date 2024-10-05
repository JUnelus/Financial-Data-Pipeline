import os
import requests
import pandas as pd
import io
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Alpha Vantage API configuration
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

# PostgreSQL connection string
engine = create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/financial_data")

# Function to fetch stock data
def fetch_stock_data(symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}&datatype=csv'
    response = requests.get(url)
    data = response.content.decode('utf-8')

    # Convert to DataFrame
    df = pd.read_csv(io.StringIO(data))
    df.columns = ['date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
    df['stock_symbol'] = symbol
    return df

# Function to store data into PostgreSQL using SQLAlchemy engine
def store_data_to_postgresql(df):
    conn = engine.raw_connection()  # Get the raw connection
    try:
        with conn.cursor() as cur:
            data = [tuple(x) for x in df.values]
            cur.executemany("INSERT INTO stock_data (date, open_price, high_price, low_price, close_price, volume, stock_symbol) VALUES (%s, %s, %s, %s, %s, %s, %s)", data)
        conn.commit()
    finally:
        conn.close()  # Ensure the connection is closed in case of errors

if __name__ == '__main__':
    symbol = 'AAPL'  # Example stock symbol
    stock_data_df = fetch_stock_data(symbol)
    store_data_to_postgresql(stock_data_df)
    print(f'Data for {symbol} stored successfully.')
