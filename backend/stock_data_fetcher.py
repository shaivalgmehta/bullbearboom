import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from twelvedata import TDClient
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

# Initialize TwelveData client
td = TDClient(apikey=TWELVE_DATA_API_KEY)

def fetch_stock_list_twelve_data(symbol):
    url = f"https://api.twelvedata.com/stocks?symbol={symbol}&exchange=NASDAQ&apikey={TWELVE_DATA_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error fetching stock list: {response.status_code}")

def fetch_stock_statistics_twelve_data(symbol):
    statistics = td.get_statistics(symbol=symbol).as_json()
    return statistics

def fetch_technical_indicators_twelve_data(symbol):
    technical_indicator = td.time_series(
        symbol=symbol,
        interval="1day",
        outputsize=1
    ).with_ema(
        time_period=200
    ).without_ohlc().as_json()
    return technical_indicator

def store_screener_data(data):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    # Prepare data for insertion
    values = [(
        datetime.now(),
        data['stock'],
        data['market_cap'],
        data['pe_ratio'],
        data['ev_ebitda'],
        data['pb_ratio'],
        data['peg_ratio'],
        data['current_year_sales'],
        data['current_year_ebitda'],
        data['ema']
    )]

    # Insert data
    execute_values(cur, """
        INSERT INTO screener_table (
            time, stock, market_cap, pe_ratio, ev_ebitda, pb_ratio, 
            peg_ratio, current_year_sales, current_year_ebitda, ema
        ) VALUES %s
    """, values)

    conn.commit()
    cur.close()
    conn.close()

def main():
    symbol = "TSLA"  # Hardcoded for now
    
    # Fetch stock list
    stocks = fetch_stock_list_twelve_data(symbol)
    

    # Get the appropriate transformer
    transformer = get_transformer('twelvedata_screener')
    
    for stock in stocks:
        symbol = stock['symbol']
        
        # Fetch all required data
        stock_data = stock
        statistics = fetch_stock_statistics_twelve_data(symbol)
        technical_indicator = fetch_technical_indicators_twelve_data(symbol)

        # Combine all data
        combined_data = {
            'stock_data': stock_data,
            'statistics': statistics,
            'technical_indicator': technical_indicator
        }
        
        # Transform the data
        transformed_data = transformer.transform(combined_data)[0]
        
        # Store the transformed data
        store_screener_data(transformed_data)
        
        print(f"Data for {symbol} has been stored in TimescaleDB")

if __name__ == "__main__":
    main()