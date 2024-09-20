import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from twelvedata import TDClient
from data_transformer import get_transformer
import json  # Import json for pretty printing


# Load environment variables
load_dotenv()

# Database connection parameters
db_params = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

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

def fetch_williams_r_twelve_data(symbol):
    williams_r = td.time_series(
        symbol=symbol,
        interval="1week",
        outputsize =21
    ).with_willr(
        time_period=52
    ).without_ohlc().as_json()
    return williams_r

def store_williams_r_data(data, symbol):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    values = [(
        datetime.now(),
        symbol,
        data['williams_r'],
        data['williams_r_ema'],
        data['alert_state']
    )]

    execute_values(cur, """
        INSERT INTO williams_r_table (
            time, stock, williams_r, williams_r_ema, alert_state
        ) VALUES %s
    """, values)

    conn.commit()
    cur.close()
    conn.close()

def store_stock_data(data):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

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
        data['ema'],
        data['williams_r'],
        data['williams_r_ema'],
        data['alert_state']
    )]

    execute_values(cur, """
        INSERT INTO screener_table (
            time, stock, market_cap, pe_ratio, ev_ebitda, pb_ratio, 
            peg_ratio, current_year_sales, current_year_ebitda, ema,
            williams_r, williams_r_ema, alert_state
        ) VALUES %s
    """, values)

    conn.commit()
    cur.close()
    conn.close()

def main():
    symbol = "TSLA"  # Hardcoded for now
    
    # Fetch stock list
    stocks = fetch_stock_list_twelve_data(symbol)
    
    # Get the appropriate transformers
    screener_transformer = get_transformer('twelvedata_screener')
    williams_r_transformer = get_transformer('williams_r', db_params)

    
    for stock in stocks:
        symbol = stock['symbol']
        
        # Fetch all required data
        stock_data = stock
        statistics = fetch_stock_statistics_twelve_data(symbol)
        technical_indicator = fetch_technical_indicators_twelve_data(symbol)
        williams_r_data = fetch_williams_r_twelve_data(symbol)

        williams_r_transformed_data = williams_r_transformer.transform(williams_r_data,symbol)
        print(json.dumps(williams_r_transformed_data, indent=2))  # Pretty print the JSON data

        store_williams_r_data(williams_r_transformed_data, symbol)


        # Combine all data
        combined_data = {
            'stock_data': stock_data,
            'statistics': statistics,
            'technical_indicator': technical_indicator,
            'williams_r_transformed_data': williams_r_transformed_data
        }
        
        # Transform the data for screener
        transformed_data = screener_transformer.transform(combined_data)[0]
        print(json.dumps(transformed_data, indent=2))

        # Store the transformed data
        store_stock_data(transformed_data)
        
        print(f"Data for {symbol} has been stored in TimescaleDB")

if __name__ == "__main__":
    main()