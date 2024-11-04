#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from twelvedata import TDClient
from us_stock_get_data_functions import fetch_stock_list_twelve_data, fetch_stock_statistics_twelve_data, fetch_technical_indicators_twelve_data, fetch_williams_r_twelve_data, fetch_force_index_data, store_force_index_data, store_williams_r_data, store_stock_data, store_stock_daily_data
from us_stock_data_transformer_new import get_transformer
from get_us_stock_data import run_stock_data_process
import json  # Import json for pretty printing
import time
import pytz
from typing import Dict, Any, List


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

TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY')

######################### FUNCTION TO CALCULATE PRICE CHANGE #######################

def fetch_price_changes(symbol: str, current_date: datetime, db_params: Dict[str, Any]) -> Dict[str, float]:
    """
    Calculate 3-month, 6-month, and 12-month price changes for a given stock
    """
    try:
        conn = psycopg2.connect(**db_params)
        with conn.cursor() as cur:
            # Get current price
            cur.execute("""
                SELECT close
                FROM us_daily_table
                WHERE stock = %s AND DATE(datetime) = DATE(%s)
            """, (symbol, current_date))
            result = cur.fetchone()
            if not result:
                return {}
            current_price = float(result[0])

            # Get historical prices (3, 6, and 12 months ago)
            intervals = {
                'price_change_3m': 3,
                'price_change_6m': 6,
                'price_change_12m': 12
            }
            
            price_changes = {}
            for change_type, months in intervals.items():
                cur.execute("""
                    SELECT close
                    FROM us_daily_table
                    WHERE stock = %s 
                    AND datetime <= %s - INTERVAL '%s months'
                    ORDER BY datetime DESC
                    LIMIT 1
                """, (symbol, current_date, months))
                
                result = cur.fetchone()
                if result and result[0] != 0:
                    historical_price = float(result[0])
                    price_change = ((current_price - historical_price) / historical_price)
                    price_changes[change_type] = price_change
                else:
                    price_changes[change_type] = None
        return price_changes

    except Exception as e:
        print(f"Error calculating price changes for {symbol}: {str(e)}")
        return {}
    finally:
        if 'conn' in locals():
            conn.close()


######################### FUNCTIONS TO PREPARE THE DATA ############################# 

def process_stock(stock, end_date):
    symbol = stock['symbol']
    
    try:
        # Fetch statistics data
        statistics = fetch_stock_statistics_twelve_data(symbol)

        # Fetch technical indicators
        technical_indicator = fetch_technical_indicators_twelve_data(
            symbol, db_params, TWELVE_DATA_API_KEY, store_stock_daily_data, end_date
        )

        # Calculate price changes
        price_changes = fetch_price_changes(symbol, end_date, db_params)

        # Log if technical data is missing but continue processing
        if technical_indicator is None:
            print(f"Warning: No technical data available for {symbol}, proceeding with available data")

        # Combine whatever data we have
        combined_data = {
            'stock_data': stock,
            'statistics': statistics,
            'technical_indicator': technical_indicator if technical_indicator is not None else [],
            'price_changes': price_changes
        }

        # Transform the data for daily table
        stock_transformed_data = stock_data_transformer.transform(combined_data)[0]
        
        # Store the transformed data
        store_stock_data(stock_transformed_data)
        
        print(f"Data for {symbol} has been stored in TimescaleDB")
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")

######################### DEFINE MAIN PROCESS TO EXECUTE ############################# 

def process_stock_batch(batch, end_date):
    for stock in batch:
        process_stock(stock, end_date)

def main():
    # Fetch stock list
    stocks = fetch_stock_list_twelve_data()
    end_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    # Limit to first 3 stocks
    # stocks = stocks[:1]

 #################### Run daily us stocks data process  #####################################
    print(f"\nStarting daily US stocks data process for date {end_date}...")
    run_stock_data_process(end_date)

##################### Run statistics and EMA process ################################
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