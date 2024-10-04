import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from twelvedata import TDClient
from us_stock_get_data_functions import fetch_stock_list_twelve_data, fetch_stock_statistics_twelve_data, fetch_technical_indicators_twelve_data, fetch_williams_r_twelve_data, fetch_force_index_data, store_force_index_data, store_williams_r_data, store_stock_data
from us_stock_data_transformer_new import get_transformer
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

######################### FUNCTIONS TO PREPARE THE DATA ############################# 

def process_stock(stock):
    symbol = stock['symbol']
    
    try:
        # Fetch all required data
        statistics = fetch_stock_statistics_twelve_data(symbol)
        technical_indicator = fetch_technical_indicators_twelve_data(symbol)

        # Combine data for daily table
        combined_data = {
            'stock_data': stock,
            'statistics': statistics,
            'technical_indicator': technical_indicator
        }
        
        # Transform the data for daily table
        stock_transformed_data = stock_data_transformer.transform(combined_data)[0]

        # Store the transformed data
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
    # stocks = stocks[:5]

      # Get the appropriate transformers
    global stock_data_transformer
    stock_data_transformer = get_transformer('core_data')
  
    # Process stocks in batches of 10
    batch_size = 11
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