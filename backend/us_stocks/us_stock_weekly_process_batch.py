#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from twelvedata import TDClient
from us_stock_get_data_functions import (
    fetch_stock_list_twelve_data, 
    fetch_stock_statistics_twelve_data, 
    fetch_technical_indicators_twelve_data, 
    fetch_williams_r_twelve_data, 
    fetch_force_index_data, 
    store_force_index_data, 
    store_williams_r_data, 
    store_stock_data
)
from us_stock_data_transformer_new import get_transformer
import json
import time
import pytz
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial

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

def process_stock(stock, end_date, transformers):
    williams_r_transformer, force_index_transformer = transformers
    symbol = stock['symbol']
    try:
        # Fetch all required data
        williams_r_data = fetch_williams_r_twelve_data(symbol, db_params, end_date)

        # Process Williams %R data
        if not is_empty_or_none(williams_r_data):
            williams_r_transformed_data = williams_r_transformer.transform(williams_r_data, symbol)
            store_williams_r_data(williams_r_transformed_data, symbol)
            print(f"Williams %R data for {symbol} at {end_date.date()} has been processed and stored")
        else:
            print(f"Skipping Williams %R for {symbol} at {end_date.date()} due to insufficient data")

        force_index_data = fetch_force_index_data(symbol, db_params, end_date)
        # Process Force Index data
        if not is_empty_or_none(force_index_data):
            force_index_transformed_data = force_index_transformer.transform(force_index_data, symbol)
            store_force_index_data(force_index_transformed_data, symbol)
            print(f"Force Index data for {symbol} at {end_date.date()} has been processed and stored")
        else:
            print(f"Skipping Force Index for {symbol} at {end_date.date()} due to insufficient data")

    except Exception as e:
        print(f"Error processing {symbol} at {end_date.date()}: {str(e)}")

def process_date(args):
    stocks, end_date, batch_size = args
    
    # Get the transformers for this process
    williams_r_transformer = get_transformer('williams_r', db_params)
    force_index_transformer = get_transformer('force_index', db_params)
    transformers = (williams_r_transformer, force_index_transformer)
    
    print(f"\n{'='*40}")
    print(f"Processing date: {end_date.date()}")
    print(f"{'='*40}\n")
    
    # Process stocks in batches
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1} of {len(stocks)//batch_size + 1} for date {end_date.date()}")
        
        for stock in batch:
            process_stock(stock, end_date, transformers)

def main():
    # Fetch stock list
    stocks = fetch_stock_list_twelve_data()
    
    # Calculate the date range
    current = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = current - timedelta(days=1)
    dates_to_process = [end_date - timedelta(days=i) for i in range(5)]
    dates_to_process.reverse()

    # Batch size for processing stocks
    batch_size = 5000

    # Prepare arguments for multiprocessing
    process_args = [(stocks, date, batch_size) for date in dates_to_process]
    
    # Calculate optimal number of processes
    num_processes = min(len(dates_to_process), cpu_count())
    
    print(f"Starting processing with {num_processes} processes")
    start_time = time.time()
    
    # Process dates in parallel using multiprocessing
    with Pool(processes=num_processes) as pool:
        pool.map(process_date, process_args)
    
    elapsed_time = time.time() - start_time
    print(f"\nAll processing completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()