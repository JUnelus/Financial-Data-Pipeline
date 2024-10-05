import os
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection string
engine = create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/financial_data")

# Query data from PostgreSQL
query = "SELECT date, close_price FROM stock_data WHERE stock_symbol = 'AAPL' ORDER BY date"

conn = engine.raw_connection()
try:
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=['date', 'close_price'])
finally:
    conn.close()

# Plotting the stock prices
plt.figure(figsize=(10,6))
plt.plot(df['date'], df['close_price'], label='AAPL Closing Prices')
plt.xlabel('Date')
plt.ylabel('Closing Price (USD)')
plt.title('AAPL Stock Price Over Time')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
