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

def update_quarterly_rankings(date=None):
    """
    Update rankings for quarterly metrics by grouping stocks into their respective quarters
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First, identify all distinct quarters in our data
            cur.execute("""
                SELECT DISTINCT 
                    date_trunc('quarter', datetime) as quarter
                FROM us_quarterly_table
                ORDER BY quarter DESC
                LIMIT 4  -- Look at last year of quarters
            """)
            quarters = [row[0] for row in cur.fetchall()]

            for quarter in quarters:
                # Get stocks reporting in this quarter
                cur.execute("""
                    WITH quarterly_data AS (
                        SELECT DISTINCT ON (stock)
                            stock,
                            return_on_equity,
                            return_on_assets,
                            price_to_sales,
                            free_cash_flow_yield,
                            shareholder_yield,
                            datetime
                        FROM us_quarterly_table
                        WHERE date_trunc('quarter', datetime) = %s
                        ORDER BY stock, datetime DESC
                    )
                    SELECT * FROM quarterly_data
                """, (quarter,))
                
                stocks_data = cur.fetchall()
                
                if not stocks_data:
                    continue

                # Create dictionary of stocks and their metrics
                metrics_to_rank = [
                    ('return_on_equity', True),      # Higher is better
                    ('return_on_assets', True),      # Higher is better
                    ('price_to_sales', False),       # Lower is better
                    ('free_cash_flow_yield', True),  # Higher is better
                    ('shareholder_yield', True)      # Higher is better
                ]

                for metric_name, higher_is_better in metrics_to_rank:
                    # Filter valid values and sort
                    valid_stocks = [
                        (stock[0], stock[metrics_to_rank.index((metric_name, higher_is_better)) + 1])
                        for stock in stocks_data
                        if stock[metrics_to_rank.index((metric_name, higher_is_better)) + 1] is not None
                    ]

                    if not valid_stocks:
                        continue

                    # Sort based on whether higher or lower is better
                    valid_stocks.sort(key=lambda x: x[1], reverse=higher_is_better)
                    
                    # Calculate rankings with proper tie handling
                    current_rank = 1
                    previous_value = None
                    rank_sum = 0
                    count = 0
                    
                    for i, (stock, value) in enumerate(valid_stocks):
                        if value != previous_value:
                            if count > 0:
                                # Assign average rank to tied values
                                avg_rank = rank_sum / count
                                for j in range(count):
                                    cur.execute(f"""
                                        UPDATE us_quarterly_table
                                        SET {metric_name}_rank = %s
                                        WHERE stock = %s AND date_trunc('quarter', datetime) = %s
                                    """, (avg_rank, valid_stocks[i-j-1][0], quarter))
                            current_rank = i + 1
                            rank_sum = current_rank
                            count = 1
                        else:
                            rank_sum += current_rank
                            count += 1
                        
                        previous_value = value
                        
                        # Handle last group
                        if i == len(valid_stocks) - 1:
                            avg_rank = rank_sum / count
                            for j in range(count):
                                cur.execute(f"""
                                    UPDATE us_quarterly_table
                                    SET {metric_name}_rank = %s
                                    WHERE stock = %s AND date_trunc('quarter', datetime) = %s
                                """, (avg_rank, valid_stocks[i-j][0], quarter))

                    # Set NULL rank for stocks that didn't meet criteria
                    invalid_stocks = [
                        stock[0] for stock in stocks_data 
                        if stock[0] not in [s[0] for s in valid_stocks]
                    ]
                    if invalid_stocks:
                        cur.execute(f"""
                            UPDATE us_quarterly_table
                            SET {metric_name}_rank = NULL
                            WHERE stock = ANY(%s) AND date_trunc('quarter', datetime) = %s
                        """, (invalid_stocks, quarter))

                conn.commit()
                print(f"Updated rankings for quarter {quarter}")

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

    # stocks = stocks[-1666:]

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

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            print("\nUpdating quarterly rankings...")
            update_quarterly_rankings()  # No date parameter needed now
            print("Quarterly rankings updated")

    print("All stocks have been processed.")

if __name__ == "__main__":
    main()