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
    store_statistics_data,
    fetch_stock_cashflow_twelve_data
)
from us_stock_data_transformer_new import get_transformer
import json
import time
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

def get_db_connection():
    return psycopg2.connect(**db_params)

def update_quarterly_rankings(date):
    """Update rankings for all metrics in quarterly table"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get all stocks for the given date
            cur.execute("""
                SELECT stock, 
                       return_on_equity,
                       return_on_assets,
                       price_to_sales,
                       free_cash_flow_yield,
                       shareholder_yield
                FROM us_quarterly_table
                WHERE DATE(datetime) = DATE(%s)
            """, (date,))
            
            stocks_data = [
                {
                    'stock': row[0],
                    'return_on_equity': row[1],
                    'return_on_assets': row[2],
                    'price_to_sales': row[3],
                    'free_cash_flow_yield': row[4],
                    'shareholder_yield': row[5]
                }
                for row in cur.fetchall()
            ]

            # Calculate rankings for each metric
            metrics_to_rank = [
                # (field_name, reverse_sort, filter_func)
                ('return_on_equity', True, lambda x: x is not None),  # Higher is better
                ('return_on_assets', True, lambda x: x is not None),  # Higher is better
                ('price_to_sales', False, lambda x: x is not None and x > 0),  # Lower is better, exclude negative
                ('free_cash_flow_yield', True, lambda x: x is not None),  # Higher is better
                ('shareholder_yield', True, lambda x: x is not None)  # Higher is better
            ]

            for metric, reverse_sort, filter_func in metrics_to_rank:
                # Filter valid values and sort
                valid_stocks = [s for s in stocks_data if filter_func(s[metric])]
                valid_stocks.sort(key=lambda x: x[metric], reverse=reverse_sort)
                
                # Assign ranks
                rank = 1
                for stock in valid_stocks:
                    cur.execute(f"""
                        UPDATE us_quarterly_table
                        SET {metric}_rank = %s
                        WHERE stock = %s AND DATE(datetime) = DATE(%s)
                    """, (rank, stock['stock'], date))
                    rank += 1
                
                # Set NULL rank for stocks that didn't meet criteria
                invalid_stocks = [s['stock'] for s in stocks_data if not filter_func(s[metric])]
                if invalid_stocks:
                    cur.execute(f"""
                        UPDATE us_quarterly_table
                        SET {metric}_rank = NULL
                        WHERE stock = ANY(%s) AND DATE(datetime) = DATE(%s)
                    """, (invalid_stocks, date))
            
            conn.commit()

def process_stock(stock):
    symbol = stock['symbol']
    
    try:
        # Fetch all required data
        statistics = fetch_stock_statistics_twelve_data(symbol)
        cashflow = fetch_stock_cashflow_twelve_data(symbol)
    
        # Combine data for quarterly table
        combined_data = {
            'stock_data': stock,
            'statistics': statistics,
            'cashflow': cashflow
        }
        
        # Transform the data for quarterly table
        statistics_transformed_data = statistics_transformer.transform(combined_data)[0]
        store_statistics_data(statistics_transformed_data)
        
        print(f"Data for {symbol} has been stored in TimescaleDB")
    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")

def process_stock_batch(batch):
    for stock in batch:
        process_stock(stock)

def main():
    # Fetch stock list
    stocks = fetch_stock_list_twelve_data()
    
    # Get the appropriate transformers
    global statistics_transformer
    statistics_transformer = get_transformer('statistics')

    # Process stocks in batches
    batch_size = 4
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

    # After processing all stocks, update rankings for the most recent date
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT DATE(datetime)
                FROM us_quarterly_table
                ORDER BY DATE(datetime) DESC
                LIMIT 1
            """)
            latest_date = cur.fetchone()
            
            if latest_date:
                update_quarterly_rankings(latest_date[0])
                print(f"Updated rankings for date: {latest_date[0]}")
            else:
                print("No data found in quarterly table")

    print("All stocks have been processed.")

if __name__ == "__main__":
    main()