#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from twelvedata import TDClient
from crypto_get_data_functions import fetch_stock_list_polygon, fetch_technical_indicators_polygon, fetch_williams_r_polygon, fetch_force_index_data, store_force_index_data, store_williams_r_data, store_stock_data, store_stock_daily_data, fetch_daily_data_polygon
from crypto_data_transformer_new import get_transformer
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

def is_empty_or_none(data):
    return data is None or (isinstance(data, list) and len(data) == 0)

def process_stock(stock, base, end_date):
    symbol = stock['symbol']
    try:
        # Fetch all required data
        williams_r_data = fetch_williams_r_polygon(symbol, db_params, end_date, base)

        # Process Williams %R data
        if not is_empty_or_none(williams_r_data):
            williams_r_transformed_data = williams_r_transformer.transform(williams_r_data, symbol)
            store_williams_r_data(williams_r_transformed_data, symbol, base)
            print(f"Williams %R data for {symbol}/{base} has been processed and stored")
        else:
            print(f"Skipping Williams %R for {symbol}/{base} due to insufficient data or other criteria not met")

        force_index_data = fetch_force_index_data(symbol, db_params, end_date, base)
        # Process Force Index data
        if not is_empty_or_none(force_index_data):
            force_index_transformed_data = force_index_transformer.transform(force_index_data, symbol)
            store_force_index_data(force_index_transformed_data, symbol, base)
            print(f"Force Index data for {symbol}/{base} has been processed and stored")
        else:
            print(f"Skipping Force Index for {symbol}/{base} due to insufficient data or other criteria not met")

        if is_empty_or_none(williams_r_data) and is_empty_or_none(force_index_data):
            print(f"No data processed for {symbol}/{base} due to insufficient data or other criteria not met")
        else:
            print(f"Data for {symbol}/{base} has been stored in TimescaleDB")

    except Exception as e:
        print(f"Error processing {symbol}/{base}: {str(e)}")

def process_stock_batch(batch, base, end_date):
    for stock in batch:
        process_stock(stock, base, end_date)


def get_previous_monday(date):
    """Find the Monday of the previous week."""
    previous_monday = date - timedelta(days=(date.weekday() or 7))

    return previous_monday.replace(hour=0, minute=0, second=0, microsecond=0)

def rank_weekly_metrics(conn, base, end_date):
    table_name = f"crypto_weekly_table{'_' + base if base != 'usd' else ''}"
    
    # Get the Monday of the previous week
    previous_monday = get_previous_monday(end_date)
    
    # Fields to rank
    fields = ['williams_r', 'williams_r_ema', 'force_index_7_week', 'force_index_52_week']
    
    # Query to get the data
    query = f"""
        SELECT stock, {', '.join(fields)}
        FROM {table_name}
        WHERE datetime = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (previous_monday,))
        results = cur.fetchall()
    
    # Create DataFrame
    df = pd.DataFrame(results, columns=['stock'] + fields)
    
    # Calculate rankings for each field independently
    for field in fields:
        rank_col = f"{field}_rank"
        # Rank only non-NaN values
        df[rank_col] = df[field].rank(method='min', ascending=True, na_option='keep')

    # Update the table with rankings
    update_query = f"""
        UPDATE {table_name}
        SET {', '.join([f"{field}_rank = %s" for field in fields])}
        WHERE stock = %s AND datetime = %s
    """
    
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            ranks = [int(row[f"{field}_rank"]) if pd.notnull(row[f"{field}_rank"]) else None for field in fields]
            cur.execute(update_query, ranks + [row['stock'], previous_monday])
    
    conn.commit()
    
    # Print statistics
    for field in fields:
        ranked_count = df[f"{field}_rank"].notna().sum()
        print(f"Rankings updated for {base.upper()} base on {previous_monday}")
        print(f"Ranked {ranked_count} out of {len(df)} stocks for {field}.")

    #################### MAIN PROCESS ########################################################

def main():
    # Fetch stock list
    stocks = fetch_stock_list_polygon()

    # Calculate the date range for the last X weeks
    end_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    dates_to_process = [end_date - timedelta(weeks=i) for i in range(1)]
    # print(f"{dates_to_process}")

    # Uncomment the next line to limit processing to the first few stocks (for testing)
    # stocks = stocks[:2]

    # Define base currencies
    bases = ['usd', 'eth', 'btc']

    # Get the appropriate transformers
    global williams_r_transformer, force_index_transformer
    williams_r_transformer = get_transformer('williams_r', db_params)
    force_index_transformer = get_transformer('force_index', db_params)

    # Process data for each week
    # for end_date in dates_to_process:
    #     print(f"\n{'='*60}")
    #     print(f"Processing data for week: {end_date.date()}")
    #     print(f"{'='*60}")

    #     # Process stocks for each base currency
    #     for base in bases:
    #         print(f"\n{'='*40}")
    #         print(f"Starting processing for base currency: {base.upper()} for week {end_date.date()}")
    #         print(f"{'='*40}\n")

    #         # Process stocks in batches
    #         batch_size = 800
    #         for i in range(0, len(stocks), batch_size):
    #             batch = stocks[i:i+batch_size]
                
    #             print(f"Processing batch {i//batch_size + 1} of {len(stocks)//batch_size + 1} for {base.upper()}")
    #             print("Stocks in this batch:")
    #             for stock in batch:
    #                 print(f"- {stock['symbol']}: {stock['name']}")
                
    #             start_time = time.time()
                
    #             process_stock_batch(batch, base, end_date)
                
    #             elapsed_time = time.time() - start_time
    #             print(f"Batch processing time: {elapsed_time:.2f} seconds")
                
    #             print(f"Finished processing batch for {base.upper()}. Moving to next batch.\n")

    #         print(f"Completed processing all stocks for {base.upper()} for week {end_date.date()}")

    #     print(f"\nCompleted processing for week: {end_date.date()}")

    # print("\nAll stocks have been processed for all base currencies")

    #################### RANK WILLIAM R AND FORCE INDEX ######################################

    print("\nRanking metrics for all bases")
    conn = psycopg2.connect(**db_params)
    try:
        for base in bases:
            rank_weekly_metrics(conn, base, end_date)
    finally:
        conn.close()
    
    print("All rankings have been updated")

if __name__ == "__main__":
    main()