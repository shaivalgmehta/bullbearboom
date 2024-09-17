import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from data_transformer import get_transformer

# Load environment variables
load_dotenv()

# Twelve Data API key
TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY')

# TimescaleDB connection details
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def fetch_stock_data(symbol):
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1min&apikey={TWELVE_DATA_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching data: {response.status_code}")

def store_stock_data(data):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    # Transform the data
    transformer = get_transformer('twelvedata')
    transformed_data = transformer.transform(data)

    # Prepare data for insertion
    values = [(
        datetime.strptime(item['timestamp'], "%Y-%m-%d %H:%M:%S"),
        item['symbol'],
        item['close'],
        item['volume']
    ) for item in transformed_data]

    # Insert data
    execute_values(cur, """
        INSERT INTO stock_data (time, symbol, price, volume)
        VALUES %s
    """, values)

    conn.commit()
    cur.close()
    conn.close()

def main():
    symbol = "AAPL"  # Example: Apple Inc.
    data = fetch_stock_data(symbol)
    store_stock_data(data)
    print(f"Data for {symbol} has been stored in TimescaleDB")

if __name__ == "__main__":
    main()