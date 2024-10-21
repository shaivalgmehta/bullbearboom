#!/usr/bin/env python3

import os
import time
from dotenv import load_dotenv
from typing import Dict, Any, List
from datetime import datetime, timedelta
from crypto_get_data_functions import fetch_stock_list_polygon, store_stock_daily_data, fetch_daily_data_polygon
from crypto_data_transformer_new import get_transformer
import pytz


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

def process_stock(stock: Dict[str, Any], start_date: str, end_date: str, daily_data_transformer):
    symbol = stock['symbol']
    try:
        # Fetch daily data for the last 73 weeks
        daily_data = fetch_daily_data_polygon(symbol, start_date, end_date)
        
        if daily_data:
            # Combine data for daily table
            combined_data = {
                'stock_data': stock,
                'daily_data': daily_data
            }
            
            # Transform the data for daily table
            transformed_data = daily_data_transformer.transform(combined_data)

            store_stock_daily_data(transformed_data)
            
            print(f"Data for {symbol} has been stored in TimescaleDB")
        else:
            print(f"No daily data available for {symbol}")
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")

def get_crypto_data(batch: List[Dict[str, Any]], start_date: str, end_date: str, daily_data_transformer):
    for stock in batch:
        process_stock(stock, start_date, end_date, daily_data_transformer)

################## FUNCTION TO GET RAW DATA FROM POLYGON FOR A SET DATE RANGE #############################

def run_crypto_data_process(end_date: datetime, batch_size: int = 800):
    # Calculate start and end dates
    # end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    # start_date = (datetime.now() - timedelta(days=1) - timedelta(weeks=104)).strftime('%Y-%m-%d')

    end_date = end_date.astimezone(pytz.UTC).strftime('%Y-%m-%d')
    start_date = end_date

    # Fetch stock list
    stocks = fetch_stock_list_polygon()
    
    # Uncomment the next line to limit processing to the first few stocks (for testing)
    # stocks = stocks[:3]

    # Get the appropriate transformer
    daily_data_transformer = get_transformer('daily_data')
  
    # Process stocks in batches
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i+batch_size]
        
        print(f"Processing batch {i//batch_size + 1} of {(len(stocks)-1)//batch_size + 1}")
        print("Stocks in this batch:")
        for stock in batch:
            print(f"- {stock['symbol']}: {stock['name']}")
                
        get_crypto_data(batch, start_date, end_date, daily_data_transformer)

        
        print(f"Finished processing batch. Moving to next batch.\n")

    print("All stocks have been processed.")

################## FUNCTION TO GET RAW DATA FROM POLYGON FOR A USER DEFINED DATE RANGE ###################

def run_crypto_data_process_custom_dates():
    # Get start date from user
    while True:
        start_date_str = input("Enter start date (DD-MM-YYYY): ")
        try:
            start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
            break
        except ValueError:
            print("Invalid date format. Please use DD-MM-YYYY.")

    # Get end date from user
    while True:
        end_date_str = input("Enter end date (DD-MM-YYYY): ")
        try:
            end_date = datetime.strptime(end_date_str, "%d-%m-%Y")
            if end_date < start_date:
                print("End date must be after start date.")
                continue
            break
        except ValueError:
            print("Invalid date format. Please use DD-MM-YYYY.")

    # Convert dates to the format expected by get_crypto_data
    start_date_formatted = start_date.strftime('%Y-%m-%d')
    end_date_formatted = end_date.strftime('%Y-%m-%d')

    print(f"Fetching crypto data from {start_date_formatted} to {end_date_formatted}")

    # Fetch stock list
    stocks = fetch_stock_list_polygon()
    
    # Get the appropriate transformer
    daily_data_transformer = get_transformer('daily_data')
  
    # Process stocks in batches
    batch_size = 800
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i+batch_size]
        
        print(f"Processing batch {i//batch_size + 1} of {(len(stocks)-1)//batch_size + 1}")
        print("Stocks in this batch:")
        for stock in batch:
            print(f"- {stock['symbol']}: {stock['name']}")
                
        get_crypto_data(batch, start_date_formatted, end_date_formatted, daily_data_transformer)
        
        
        print(f"Finished processing batch. Moving to next batch.\n")

    print("All stocks have been processed.")

if __name__ == "__main__":
    run_crypto_data_process_custom_dates()