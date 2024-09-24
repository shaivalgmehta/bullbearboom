import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from twelvedata import TDClient
from us_stock_data_transformer import get_transformer
import json  # Import json for pretty printing
import time


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

######################### DEFINE FUNCTIONS TO FETCH DATA FROM TWELVE DATA #############################
def fetch_stock_list_twelve_data():
    url = f"https://api.twelvedata.com/stocks?country=United States&type=Common Stock&apikey={TWELVE_DATA_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error fetching stock list: {response.status_code}")

def fetch_stock_statistics_twelve_data(symbol):
    statistics = td.get_statistics(symbol=symbol, country="United States").as_json()
    return statistics

def fetch_technical_indicators_twelve_data(symbol):
    technical_indicator = td.time_series(
        symbol=symbol,
        interval="1day",
        country="United States",
        type="Common Stock",
        outputsize=1
    ).with_ema(
        time_period=200
    ).as_json()
    return technical_indicator

def fetch_williams_r_twelve_data(symbol):
    williams_r = td.time_series(
        symbol=symbol,
        interval="1week",
        country="United States",
        type="Common Stock",
        outputsize =21
    ).with_willr(
        time_period=52
    ).without_ohlc().as_json()
    return williams_r

def fetch_force_index_data(symbol):
    # Fetch 54 weeks of data to calculate both current and last week's averages
    time_series = td.time_series(
        symbol=symbol,
        interval="1week",
        country="United States",
        type="Common Stock",
        outputsize=55
    ).as_json()
    
    return time_series

######################### DEFINE FUNCTIONS TO STORE DATA IN TIMESCALE DB #############################

def store_force_index_data(data, symbol):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    values = [(
        datetime.now(),
        symbol,
        data['force_index_7_week'],
        data['force_index_52_week'],
        data['last_week_force_index_7_week'],
        data['last_week_force_index_52_week'],
        data['force_index_alert_state']
    )]

    execute_values(cur, """
        INSERT INTO force_index_table (
            time, stock, force_index_7_week, force_index_52_week,
            last_week_force_index_7_week, last_week_force_index_52_week, force_index_alert_state
        ) VALUES %s
    """, values)

    conn.commit()
    cur.close()
    conn.close()


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
        data['williams_r_momentum_alert_state']
    )]

    execute_values(cur, """
        INSERT INTO williams_r_table (
            time, stock, williams_r, williams_r_ema, williams_r_momentum_alert_state
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
        data['stock_name'],
        data['market_cap'],
        data['pe_ratio'],
        data['ev_ebitda'],
        data['pb_ratio'],
        data['peg_ratio'],
        data['current_year_sales'],
        data['current_year_ebitda'],
        data['ema'],
        data['closing_price'],
        data['williams_r'],
        data['williams_r_ema'],
        data['williams_r_momentum_alert_state'],
        data['force_index_7_week'],
        data['force_index_52_week'],
        data['force_index_alert_state'],
    )]

    execute_values(cur, """
        INSERT INTO screener_table (
            time, stock, stock_name, market_cap, pe_ratio, ev_ebitda, pb_ratio, 
            peg_ratio, current_year_sales, current_year_ebitda, ema, closing_price,
            williams_r, williams_r_ema, williams_r_momentum_alert_state,
            force_index_7_week, force_index_52_week, force_index_alert_state
        ) VALUES %s
    """, values)

    conn.commit()
    cur.close()
    conn.close()

######################### DEFINE PROCESS TO GET DATA FOR EACH STOCK ##################

def process_stock(stock):
    symbol = stock['symbol']
    
    try:
        # Fetch all required data
        statistics = fetch_stock_statistics_twelve_data(symbol)
        technical_indicator = fetch_technical_indicators_twelve_data(symbol)
        williams_r_data = fetch_williams_r_twelve_data(symbol)
        force_index_data = fetch_force_index_data(symbol)

        williams_r_transformed_data = williams_r_transformer.transform(williams_r_data, symbol)
        force_index_transformed_data = force_index_transformer.transform(force_index_data, symbol)

        # Combine all data
        combined_data = {
            'stock_data': stock,
            'statistics': statistics,
            'technical_indicator': technical_indicator,
            'williams_r_transformed_data': williams_r_transformed_data,
            'force_index_transformed_data': force_index_transformed_data
        }
        
        # Transform the data for screener
        stock_transformed_data = screener_transformer.transform(combined_data)[0]

        # Store the transformed data
        store_force_index_data(force_index_transformed_data, symbol)
        store_williams_r_data(williams_r_transformed_data, symbol)
        store_stock_data(stock_transformed_data)
        
        print(f"Data for {symbol} has been stored in TimescaleDB")
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")

######################### DEFINE MAIN PROCESS TO EXECUTE ############################# 
def process_stock_batch(batch):
    for stock in batch:
        process_stock(stock)

def main():
    # Fetch stock list
    stocks = fetch_stock_list_twelve_data()
    
    # Limit to first 3 stocks
    stocks = stocks[:1]

      # Get the appropriate transformers
    global screener_transformer, williams_r_transformer, force_index_transformer
    screener_transformer = get_transformer('twelvedata_screener')
    williams_r_transformer = get_transformer('williams_r', db_params)
    force_index_transformer = get_transformer('force_index', db_params)

    # Process stocks in batches of 10
    batch_size = 10
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i+batch_size]
        
        print(f"Processing batch {i//batch_size + 1} of {len(stocks)//batch_size + 1}")
        print("Stocks in this batch:")
        for stock in batch:
            print(f"- {stock['symbol']}: {stock['name']}")
        
        start_time = time.time()
        
        process_stock_batch(batch)
        
        # Calculate time spent processing the batch
        elapsed_time = time.time() - start_time
        
        # If processing took less than 60 seconds, wait for the remainder of the minute
        if elapsed_time < 60:
            time.sleep(60 - elapsed_time)
        
        print(f"Finished processing batch. Moving to next batch.\n")

    print("All stocks have been processed.")

if __name__ == "__main__":
    main()