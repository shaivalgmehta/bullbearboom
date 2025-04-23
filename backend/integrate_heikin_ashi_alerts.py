#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import json
import argparse
import time

# Import the Heikin-Ashi transformer and alert detector
from heikin_ashi_transformer import HeikinAshiTransformer
from heikin_ashi_alert_detector import HeikinAshiAlertDetector

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

def fetch_stock_list(market='us'):
    """
    Fetch the list of symbols for the specified market.
    
    Args:
        market: Market type ('us', 'in', or 'crypto')
        
    Returns:
        List of symbol dictionaries
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if market == 'us':
                # Get US stocks
                cur.execute("""
                    SELECT DISTINCT stock as symbol, stock_name as name
                    FROM us_screener_table
                    ORDER BY stock
                """)
            elif market == 'in':
                # Get Indian stocks
                cur.execute("""
                    SELECT DISTINCT stock as symbol, stock_name as name
                    FROM in_screener_table
                    ORDER BY stock
                """)
            elif market == 'crypto':
                # Get cryptocurrencies
                cur.execute("""
                    SELECT DISTINCT stock as symbol, crypto_name as name
                    FROM crypto_screener_table
                    ORDER BY stock
                """)
            else:
                raise ValueError(f"Unsupported market: {market}")
            
            # Fetch all results
            results = cur.fetchall()
            
            # Convert to list of dictionaries
            symbols = [{'symbol': row[0], 'name': row[1]} for row in results]
            
            return symbols

def process_market_heikin_ashi_alerts(market, date=None, limit=None):
    """
    Process Heikin-Ashi alerts for all symbols in a market.
    
    Args:
        market: Market type ('us', 'in', or 'crypto')
        date: Date to process alerts for (defaults to yesterday)
        limit: Optional limit on number of symbols to process (for testing)
    """
    # Set the date (default to yesterday)
    if date is None:
        date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    
    print(f"Processing Heikin-Ashi alerts for {market.upper()} market for date: {date.date()}")
    
    # Fetch symbols
    symbols = fetch_stock_list(market)
    print(f"Found {len(symbols)} symbols for {market.upper()} market")
    
    # Apply limit if specified
    if limit and limit > 0:
        symbols = symbols[:limit]
        print(f"Limited to first {limit} symbols for testing")
    
    # Create the alert detector
    detector = HeikinAshiAlertDetector(db_params)
    
    # Process symbols in batches to avoid overwhelming the database
    batch_size = 50
    total_alerts = 0
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1} of {(len(symbols) + batch_size - 1) // batch_size}")
        
        start_time = time.time()
        
        # Process the batch
        for symbol_dict in batch:
            symbol = symbol_dict['symbol']
            try:
                print(f"  Processing {symbol}...")
                alerts = detector.detect_all_alerts(symbol, market, date)
                
                # Count alerts
                alert_count = sum(len(timeframe_alerts) for timeframe_alerts in alerts.values())
                total_alerts += alert_count
                
                if alert_count > 0:
                    print(f"    Found {alert_count} alerts for {symbol}")
                    detector.store_alerts_in_db(symbol, market, alerts, date)
            except Exception as e:
                print(f"    Error processing {symbol}: {str(e)}")
        
        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        print(f"Batch completed in {elapsed_time:.2f} seconds")
        
        # Sleep to avoid overwhelming the database
        if elapsed_time < 5:
            time.sleep(5 - elapsed_time)
    
    print(f"Processed {len(symbols)} symbols. Found {total_alerts} total alerts.")

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Process Heikin-Ashi alerts for different markets')
    parser.add_argument('--market', type=str, choices=['us', 'in', 'crypto', 'all'], default='all',
                        help='Market to process (us, in, crypto, or all)')
    parser.add_argument('--date', type=str, help='Date to process (YYYY-MM-DD), defaults to yesterday')
    parser.add_argument('--limit', type=int, help='Limit number of symbols to process (for testing)')
    
    args = parser.parse_args()
    
    # Parse date if provided
    if args.date:
        date = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    else:
        date = None
    
    # Process the specified market(s)
    if args.market == 'all':
        markets = ['us', 'in', 'crypto']
    else:
        markets = [args.market]
    
    for market in markets:
        try:
            process_market_heikin_ashi_alerts(market, date, args.limit)
        except Exception as e:
            print(f"Error processing {market} market: {str(e)}")
    
    print("All markets processed successfully!")

if __name__ == "__main__":
    main()