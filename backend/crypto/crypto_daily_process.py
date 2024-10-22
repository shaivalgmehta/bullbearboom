#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from datetime import datetime, timedelta
from twelvedata import TDClient
from crypto_get_data_functions import fetch_stock_list_polygon, fetch_technical_indicators_polygon, fetch_williams_r_polygon, fetch_force_index_data, store_force_index_data, store_williams_r_data, store_stock_data, store_stock_daily_data
from crypto_data_transformer_new import get_transformer
from crypto_conversion_process import run_crypto_conversion_process
from get_crypto_data import run_crypto_data_process
import json
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

# Polygon.io API key
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

def process_stock(stock, base, end_date):
    symbol = stock['symbol']
    try:
        # Fetch technical indicators
        technical_indicator = fetch_technical_indicators_polygon(symbol, db_params, POLYGON_API_KEY, store_stock_daily_data, end_date, base)
        
        # If technical_indicator is None, end the process for this symbol
        if technical_indicator is None:
            print(f"Skipping {symbol}/{base} due to insufficient data or other criteria not met.")
            return  # This will end the function and move to the next symbol
        
        # Combine data for daily table
        combined_data = {
            'stock_data': stock,
            'technical_indicator': technical_indicator
        }
        
        # Transform the data for daily table
        stock_transformed_data = stock_data_transformer.transform(combined_data)[0]
        
        # Store the transformed data
        store_stock_data(stock_transformed_data, base)
        
        print(f"Data for {symbol}/{base} has been stored in TimescaleDB")
    except Exception as e:
        print(f"Error processing {symbol}/{base}: {str(e)}")

def process_stock_batch(batch, base, end_date):
    for stock in batch:
        process_stock(stock, base, end_date)


def calculate_and_rank_ema_metric_for_base(conn, base, end_date):
    table_name = "crypto_daily_table" if base == "usd" else f"crypto_daily_table_{base}"
    
    # First, calculate and update the EMA metric
    with conn.cursor() as cur:
        update_metric_query = f"""
            UPDATE {table_name}
            SET ema_metric = (ema - close) / close
            WHERE datetime = %s AND ema IS NOT NULL AND close != 0
        """
        
        cur.execute(update_metric_query, (end_date,))
    
    conn.commit()
    print(f"EMA metric calculated for {base.upper()} base on {end_date}")

    # Now, query to get the EMA metric for each stock on the specific date
    query = f"""
        SELECT stock, ema_metric
        FROM {table_name}
        WHERE datetime = %s AND ema_metric IS NOT NULL
    """

    # Execute query and fetch results
    with conn.cursor() as cur:
        cur.execute(query, (end_date,))
        results = cur.fetchall()

    # Create DataFrame from results
    df = pd.DataFrame(results, columns=['stock', 'ema_metric'])
    
    # Calculate rankings (higher values ranked better)
    df['ema_rank'] = df['ema_metric'].rank(method='min', ascending=False)
    print(f"{df['ema_rank']}")

    # Update the table with the rankings
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            update_query = f"""
                UPDATE {table_name}
                SET ema_rank = %s
                WHERE stock = %s AND datetime = %s
            """
            
            cur.execute(update_query, (int(row['ema_rank']), row['stock'], end_date))
    
    conn.commit()
    print(f"EMA rankings updated for {base.upper()} base on {end_date}")


#################        MAIN PROCESS        #########################################################
def main():
    # Calculate the date range
    end_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    last_end_date = end_date - timedelta(days=0)  # This will give us as many days as we want from the end_date

    # Fetch stock list (assuming this doesn't change daily)
    stocks = fetch_stock_list_polygon()
    
    # Uncomment the next line to limit processing to the first few stocks (for testing)
    # stocks = stocks[:3]


    # Define base currencies
    bases = ['usd', 'eth', 'btc']

    # Get the appropriate transformer
    global stock_data_transformer
    stock_data_transformer = get_transformer('core_data')


    #################### Run daily crypto data process  #####################################
    print(f"\nStarting daily crypto data process for date {end_date}...")
    run_crypto_data_process(end_date)

    #################### Run crypto conversion process ######################################
    print(f"\nStarting crypto conversion process for date {end_date}...")
    run_crypto_conversion_process(end_date)


    ################### Run EMA process #####################################################

    #Process data for each day
    current_date = last_end_date
    while current_date <= end_date:
        print(f"\n{'='*60}")
        print(f"Processing data for date: {current_date.date()}")
        print(f"{'='*60}")

        # Process stocks for each base currency
        for base in bases:
            print(f"\n{'='*40}")
            print(f"Starting processing for base currency: {base.upper()} for date {current_date}")
            print(f"{'='*40}\n")

            # Process stocks in batches
            batch_size = 100
            for i in range(0, len(stocks), batch_size):
                batch = stocks[i:i+batch_size]
                
                print(f"Processing batch {i//batch_size + 1} of {len(stocks)//batch_size + 1} for {base.upper()}")
                print("Stocks in this batch:")
                for stock in batch:
                    print(f"- {stock['symbol']}: {stock['name']}")
                
                start_time = time.time()
                
                process_stock_batch(batch, base, current_date)
                
                print(f"Finished processing batch for {base.upper()}. Moving to next batch.\n")

            print(f"Completed processing all stocks for {base.upper()} on {current_date.date()}")

        print(f"\nCompleted processing for date: {current_date.date()}")
        
        # Move to the next day
        current_date += timedelta(days=1)

    print("\nAll stocks have been processed for all base currencies for the last 10 days.")

    ################## RANK EMA #############################################################

    print("\nCalculating EMA metric and updating rankings for all bases")
    
    # Establish a new connection for ranking updates
    conn = psycopg2.connect(**db_params)
    
    try:
        # Update EMA metric and rankings for each base
        for base in ['usd', 'eth', 'btc']:
            calculate_and_rank_ema_metric_for_base(conn, base, end_date)
    finally:
        conn.close()

    print(f"EMA metrics and rankings have been updated for all bases for date: {end_date}")


if __name__ == "__main__":
    main()