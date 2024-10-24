#!/usr/bin/env python3

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
import pytz
import pandas as pd


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

def is_empty_or_none(data):
    return data is None or (isinstance(data, list) and len(data) == 0)

def process_stock(stock, end_date):
    symbol = stock['symbol']
    try:
        # Fetch all required data
        williams_r_data = fetch_williams_r_twelve_data(symbol, db_params, end_date)

        # Process Williams %R data
        if not is_empty_or_none(williams_r_data):
            williams_r_transformed_data = williams_r_transformer.transform(williams_r_data, symbol)
            store_williams_r_data(williams_r_transformed_data, symbol)
            print(f"Williams %R data for {symbol} has been processed and stored")
        else:
            print(f"Skipping Williams %R for {symbol} due to insufficient data or other criteria not met")

        force_index_data = fetch_force_index_data(symbol, db_params, end_date)
        # Process Force Index data
        if not is_empty_or_none(force_index_data):
            force_index_transformed_data = force_index_transformer.transform(force_index_data, symbol)
            store_force_index_data(force_index_transformed_data, symbol)
            print(f"Force Index data for {symbol} has been processed and stored")
        else:
            print(f"Skipping Force Index for {symbol} due to insufficient data or other criteria not met")

        if is_empty_or_none(williams_r_data) and is_empty_or_none(force_index_data):
            print(f"No data processed for {symbol} due to insufficient data or other criteria not met")
        else:
            print(f"Data for {symbol} has been stored in TimescaleDB")

    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")

def process_stock_batch(batch, end_date):
    for stock in batch:
        process_stock(stock, end_date)

def main():
    # Fetch stock list
    stocks = fetch_stock_list_twelve_data()
    # Calculate the date range for the last X weeks
    current = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    # Calculate days since Sunday (weekday() returns 0 for Monday, 6 for Sunday)
    days_since_sunday = (current.weekday() + 1) % 7

    # Subtract those days to get to the most recent Sunday
    end_date = current - timedelta(days=days_since_sunday)
    dates_to_process = [end_date - timedelta(weeks=i) for i in range(1)]
    # # Limit to first 3 stocks
    # stocks = stocks[:1]

      # Get the appropriate transformers
    global williams_r_transformer, force_index_transformer
    williams_r_transformer = get_transformer('williams_r', db_params)
    force_index_transformer = get_transformer('force_index', db_params)

    for end_date in dates_to_process:
        print(f"\n{'='*40}")
        print(f"Starting processing for week {end_date.date()}")
        print(f"{'='*40}\n")

        # Process stocks in batches of 10
        batch_size = 600
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            
            print(f"Processing batch {i//batch_size + 1} of {len(stocks)//batch_size + 1}")
            print("Stocks in this batch:")
            for stock in batch:
                print(f"- {stock['symbol']}: {stock['name']}")
            
            start_time = time.time()
            
            process_stock_batch(batch, end_date)
            
            # Calculate time spent processing the batch
            elapsed_time = time.time() - start_time
            
            # If processing took less than 60 seconds, wait for the remainder of the minute
            if elapsed_time < 60:
                time.sleep(60 - elapsed_time)
            
            print(f"Finished processing batch. Moving to next batch.\n")

        print("All stocks have been processed.")

if __name__ == "__main__":
    main()