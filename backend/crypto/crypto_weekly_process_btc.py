#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from twelvedata import TDClient
from crypto_get_data_functions import fetch_stock_list_polygon_btc, fetch_technical_indicators_polygon_btc, fetch_williams_r_polygon_btc, fetch_force_index_data_btc, store_force_index_data_btc, store_williams_r_data_btc, store_stock_data_btc
from crypto_data_transformer_new import get_transformer
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
        williams_r_data = fetch_williams_r_polygon_btc(symbol)
        force_index_data = fetch_force_index_data_btc(symbol)

        williams_r_transformed_data = williams_r_transformer.transform(williams_r_data, symbol)
        force_index_transformed_data = force_index_transformer.transform(force_index_data, symbol)


        # Store the transformed data
        store_force_index_data_btc(force_index_transformed_data, symbol)
        store_williams_r_data_btc(williams_r_transformed_data, symbol)
        
        print(f"Data for {symbol} has been stored in TimescaleDB")
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")

######################### DEFINE MAIN PROCESS TO EXECUTE ############################# 

def process_stock_batch(batch):
    for stock in batch:
        process_stock(stock)

def main():
    # Fetch stock list
    stocks = fetch_stock_list_polygon_btc()
    
    # Limit to first 3 stocks
    # stocks = stocks[:1]

      # Get the appropriate transformers
    global williams_r_transformer, force_index_transformer
    williams_r_transformer = get_transformer('williams_r_btc', db_params)
    force_index_transformer = get_transformer('force_index_btc', db_params)

    # Process stocks in batches of 10
    batch_size = 800
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