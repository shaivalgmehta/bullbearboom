#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from datetime import datetime, timedelta
from twelvedata import TDClient
from crypto_get_data_functions import fetch_stock_list_polygon, fetch_technical_indicators_polygon, fetch_williams_r_polygon, fetch_force_index_data, store_force_index_data, store_williams_r_data, store_stock_data, store_stock_daily_data, fetch_all_time_high
from crypto_data_transformer_new import get_transformer
from crypto_conversion_process import run_crypto_conversion_process
from get_crypto_data import run_crypto_data_process
import json
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

# Polygon.io API key
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')


######################### FUNCTION TO CALCULATE PRICE CHANGE #######################

def fetch_price_changes(symbol: str, current_date: datetime, current_price: float, db_params: Dict[str, Any], base: str = 'usd') -> Dict[str, float]:
    """
    Calculate 3-month, 6-month, and 12-month price changes for a given crypto
    Args:
        symbol: Crypto symbol
        current_date: Date for current price
        current_price: Current closing price from API response
        db_params: Database connection parameters
        base: Base currency (usd, eth, btc)
    """
    try:
        conn = psycopg2.connect(**db_params)
        with conn.cursor() as cur:
            # Get table name based on base currency
            table_name = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
            
            # Get historical prices (3, 6, and 12 months ago)
            intervals = {
                'price_change_3m': 3,
                'price_change_6m': 6,
                'price_change_12m': 12
            }
            
            price_changes = {}
            
            for change_type, months in intervals.items():
                cur.execute(f"""
                    SELECT close
                    FROM {table_name}
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
        print(f"Error calculating price changes for {symbol}/{base}: {str(e)}")
        return {}
    finally:
        if 'conn' in locals():
            conn.close()

######################### FUNCTION TO CALCULATE HEIKIN-ASHI VALUES #######################

def calculate_heikin_ashi(symbol: str, current_date: datetime, db_params: Dict[str, Any], base: str = 'usd') -> bool:
    """
    Calculate Heikin-Ashi values for a crypto and update the database
    
    Args:
        symbol: Crypto symbol
        current_date: Date to calculate for
        db_params: Database connection parameters
        base: Base currency (usd, eth, btc)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get at least 30 days of data for accurate Heikin-Ashi calculation
        start_date = current_date - timedelta(days=60)
        
        # Get table name based on base currency
        table_name = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
        
        conn = psycopg2.connect(**db_params)
        with conn.cursor() as cur:
            # Fetch OHLC data
            cur.execute(f"""
                SELECT datetime, open, high, low, close
                FROM {table_name}
                WHERE stock = %s
                AND datetime BETWEEN %s AND %s
                ORDER BY datetime
            """, (symbol, start_date, current_date))
            
            rows = cur.fetchall()
            
            if not rows:
                print(f"No data found for {symbol}/{base} for Heikin-Ashi calculation. Skipping.")
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
                execute_values(cur, f"""
                    UPDATE {table_name}
                    SET 
                        ha_open = data.ha_open,
                        ha_high = data.ha_high,
                        ha_low = data.ha_low,
                        ha_close = data.ha_close,
                        ha_color = data.ha_color
                    FROM (VALUES %s) AS data(ha_open, ha_high, ha_low, ha_close, ha_color, datetime, stock)
                    WHERE {table_name}.datetime = data.datetime 
                    AND {table_name}.stock = data.stock
                """, update_data)
                
                conn.commit()
                return True
            
            return False

    except Exception as e:
        print(f"Error calculating Heikin-Ashi values for {symbol}/{base}: {str(e)}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

######################### FUNCTIONS TO PREPARE THE DATA #############################           

def process_stock(stock, base, end_date):
    symbol = stock['symbol']
    try:
        # Fetch technical indicators
        technical_indicator = fetch_technical_indicators_polygon(
            symbol, db_params, POLYGON_API_KEY, store_stock_daily_data, end_date, base
        )
        
        # If technical_indicator is None, end the process for this symbol
        if technical_indicator is None:
            print(f"Skipping {symbol}/{base} due to insufficient data or other criteria not met.")
            return  # This will end the function and move to the next symbol
        
        # Get current price from technical indicator data
        current_price = float(technical_indicator['close'])
        
        # Calculate price changes using the current price
        price_changes = fetch_price_changes(symbol, end_date, current_price, db_params, base)

        # Get ATH data for the last 3 years
        ath_data = fetch_all_time_high(symbol, db_params, end_date, base)
        
        # Combine data for daily table
        combined_data = {
            'stock_data': stock,
            'technical_indicator': technical_indicator,
            'price_changes': price_changes,
            'ath_data': ath_data
        }
        
        # Transform the data for daily table
        stock_transformed_data = stock_data_transformer.transform(combined_data)[0]
        
        # Store the transformed data
        store_stock_data(stock_transformed_data, base)
        
        # Calculate and store Heikin-Ashi values
        calculate_heikin_ashi(symbol, end_date, db_params, base)
        
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

    print("\nAll stocks have been processed for all base currencies")

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