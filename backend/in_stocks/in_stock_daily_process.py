#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from twelvedata import TDClient
from in_stock_get_data_functions import fetch_stock_list_twelve_data, fetch_stock_statistics_twelve_data, fetch_technical_indicators_twelve_data, fetch_williams_r_twelve_data, fetch_force_index_data, store_force_index_data, store_williams_r_data, store_stock_data, store_stock_daily_data
from in_stock_data_transformer_new import get_transformer
from get_in_stock_data import run_stock_data_process
import json  # Import json for pretty printing
import time
import pytz
import pandas as pd
from typing import Dict, Any, List
import sys
sys.path.append(os.path.abspath('..'))


# Import Heikin-Ashi transformer
from heikin_ashi_transformer import HeikinAshiTransformer

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
                FROM in_daily_table
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
                    FROM in_daily_table
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

######################### FUNCTION TO CALCULATE HEIKIN-ASHI VALUES #######################

def calculate_heikin_ashi(symbol: str, current_date: datetime, db_params: Dict[str, Any]) -> bool:
    """
    Calculate Heikin-Ashi values for a stock and update the database
    
    Args:
        symbol: Stock symbol
        current_date: Date to calculate for
        db_params: Database connection parameters
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get at least 30 days of data for accurate Heikin-Ashi calculation
        start_date = current_date - timedelta(days=60)
        
        conn = psycopg2.connect(**db_params)
        with conn.cursor() as cur:
            # Fetch OHLC data
            cur.execute("""
                SELECT datetime, open, high, low, close
                FROM in_daily_table
                WHERE stock = %s
                AND datetime BETWEEN %s AND %s
                ORDER BY datetime
            """, (symbol, start_date, current_date))
            
            rows = cur.fetchall()
            
            if not rows:
                print(f"No data found for {symbol} for Heikin-Ashi calculation. Skipping.")
                return False
            
            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=['datetime', 'open', 'high', 'low', 'close'])
            
            # Calculate Heikin-Ashi values
            ha_df = HeikinAshiTransformer.transform_dataframe(df)
            
            # Update the database with calculated values, but only for the specific date
            update_data = []
            for idx, row in ha_df.iterrows():
                # Only update the specified date
                row_date = pd.to_datetime(row['datetime']).date()
                if row_date == current_date.date():
                    update_data.append((
                        float(row['ha_open']),
                        float(row['ha_high']),
                        float(row['ha_low']),
                        float(row['ha_close']),
                        row['ha_color'],
                        row['datetime'],
                        symbol
                    ))
            
            if update_data:
                execute_values(cur, """
                    UPDATE in_daily_table
                    SET 
                        ha_open = data.ha_open,
                        ha_high = data.ha_high,
                        ha_low = data.ha_low,
                        ha_close = data.ha_close,
                        ha_color = data.ha_color
                    FROM (VALUES %s) AS data(ha_open, ha_high, ha_low, ha_close, ha_color, datetime, stock)
                    WHERE in_daily_table.datetime = data.datetime 
                    AND in_daily_table.stock = data.stock
                """, update_data)
                
                conn.commit()
                return True
            
            return False

    except Exception as e:
        print(f"Error calculating Heikin-Ashi values for {symbol}: {str(e)}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

######################### FUNCTIONS TO PREPARE THE DATA ############################# 

def process_stock(stock, end_date):
    symbol = stock['symbol']
    
    try:
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
            'technical_indicator': technical_indicator if technical_indicator is not None else [],
            'price_changes': price_changes
        }
        
        # Transform the data for daily table
        stock_transformed_data = stock_data_transformer.transform(combined_data)[0]
        
        # Store the transformed data
        store_stock_data(stock_transformed_data)
        
        # Calculate and store Heikin-Ashi values
        calculate_heikin_ashi(symbol, end_date, db_params)
        
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
    
    # Uncomment to limit processing to a few stocks for testing
    stocks = stocks[:5]

 #################### Run daily Indian stocks data process  #####################################
    print(f"\nStarting daily Indian stocks data process for date {end_date}...")
    #run_stock_data_process(end_date)

##################### Run statistics and EMA process ################################
    # Get the appropriate transformers
    global stock_data_transformer
    stock_data_transformer = get_transformer('core_data', db_params)
  
    # Process stocks in batches
    batch_size = 800
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