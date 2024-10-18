#!/usr/bin/env python3

import os
import time
from dotenv import load_dotenv
from typing import Dict, Any, List
from datetime import datetime, timedelta
from crypto_get_data_functions import fetch_stock_list_polygon, store_stock_daily_data, fetch_daily_data_polygon
from crypto_data_transformer_new import get_transformer
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

# Calculate start and end dates
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=1) - timedelta(weeks=104)).strftime('%Y-%m-%d')

def process_stock(stock: Dict[str, Any]):
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

######################### DEFINE MAIN PROCESS TO EXECUTE ############################# 

def process_stock_batch(batch: List[Dict[str, Any]]):
    for stock in batch:
        process_stock(stock)

def main():
    # Fetch stock list
    stocks = fetch_stock_list_polygon()
    
    # Uncomment the next line to limit processing to the first stock (for testing)
    # stocks = stocks[:2]

    # Get the appropriate transformer
    global daily_data_transformer
    daily_data_transformer = get_transformer('daily_data')
  
    # Process stocks in batches
    batch_size = 800
    for i in range(0, len(stocks), batch_size):
        batch = stocks[i:i+batch_size]
        
        print(f"Processing batch {i//batch_size + 1} of {(len(stocks)-1)//batch_size + 1}")
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