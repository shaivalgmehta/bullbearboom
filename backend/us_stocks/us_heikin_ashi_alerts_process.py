#!/usr/bin/env python3

import os
import argparse
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath('..'))

# Import the necessary tools from your existing code
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

def fetch_stock_list(market='us'):
    """Fetch the list of symbols for the specified market."""
    # Reuse your existing code for fetching stocks
    # This function should return a list of dictionaries with 'symbol' and 'name' keys
    from us_stock_get_data_functions import fetch_stock_list_twelve_data
    return fetch_stock_list_twelve_data()

def smart_heikin_ashi_scheduler(market='us', date=None, limit=None, force_timeframes=None):
    """
    Smart scheduler for Heikin-Ashi alerts.
    Determines which timeframes to process based on the date.
    
    Args:
        market: Market to process ('us', 'in', 'crypto')
        date: Date to process (defaults to yesterday)
        limit: Optional limit on number of symbols to process
        force_timeframes: Force processing specific timeframes regardless of date
    """
    # Set the date (default to yesterday)
    if date is None:
        date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    
    # Determine which timeframes to process based on the date
    timeframes = force_timeframes or []
    
    if not timeframes:
        today = date.date()
        print(f'{today.weekday()}')
       
        # Process 3-day timeframe every 3 days
        # Check if the day of month is divisible by 3
        if today.day % 1 == 0:
            timeframes.append('3d')
        
        # Process 2-week timeframe on Mondays
        if today.weekday() == 6:  # Monday is 0
            timeframes.append('2w')
    
    if not timeframes:
        print(f"No timeframes to process for date {date.date()}. Exiting.")
        return
    
    print(f"Processing Heikin-Ashi alerts for {market.upper()} market for date: {date.date()}")
    print(f"Timeframes to process: {timeframes}")
    
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
        
        # Process the batch
        for symbol_dict in batch:
            symbol = symbol_dict['symbol']
            try:
                print(f"  Processing {symbol}...")
                alerts = {}
                
                # Process only the timeframes we determined earlier
                if '3d' in timeframes:
                    alerts['3d'] = detector.detect_3d_color_changes(symbol, market, date)
                
                if '2w' in timeframes:
                    alerts['2w'] = detector.detect_2w_color_changes(symbol, market, date)
                
                # Count alerts
                alert_count = sum(len(timeframe_alerts) for timeframe_alerts in alerts.values())
                total_alerts += alert_count
                
                if alert_count > 0:
                    print(f"    Found {alert_count} alerts for {symbol}")
                    detector.store_alerts_in_db(symbol, market, alerts, date)
            except Exception as e:
                print(f"    Error processing {symbol}: {str(e)}")
    
    print(f"Processed {len(symbols)} symbols. Found {total_alerts} total alerts for timeframes: {timeframes}")

def main():
    parser = argparse.ArgumentParser(description='Smart scheduler for Heikin-Ashi alerts')
    parser.add_argument('--market', type=str, choices=['us', 'in', 'crypto', 'all'], default='us',
                        help='Market to process (us, in, crypto, or all)')
    parser.add_argument('--date', type=str, help='Date to process (YYYY-MM-DD), defaults to yesterday')
    parser.add_argument('--limit', type=int, help='Limit number of symbols to process (for testing)')
    parser.add_argument('--timeframe', type=str, choices=['3d', '2w', 'all'],
                        help='Force processing specific timeframe(s)')
    
    args = parser.parse_args()
    
    # Parse date if provided
    if args.date:
        date = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    else:
        date = None
    
    # Determine forced timeframes if specified
    force_timeframes = None
    if args.timeframe:
        if args.timeframe == 'all':
            force_timeframes = ['3d', '2w']
        else:
            force_timeframes = [args.timeframe]
    
    # Process the specified market(s)
    if args.market == 'all':
        markets = ['us', 'in', 'crypto']
    else:
        markets = [args.market]
    
    for market in markets:
        try:
            smart_heikin_ashi_scheduler(market, date, args.limit, force_timeframes)
        except Exception as e:
            print(f"Error processing {market} market: {str(e)}")
    
    print("All markets processed successfully!")

if __name__ == "__main__":
    main()