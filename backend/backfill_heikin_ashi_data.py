#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
import time
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

def get_db_connection():
    return psycopg2.connect(**db_params)

def get_symbols(market='us'):
    """Get list of unique symbols for a market"""
    table_map = {
        'us': 'us_daily_table',
        'in': 'in_daily_table',
        'crypto': 'crypto_daily_table',
        'crypto_eth': 'crypto_daily_table_eth',
        'crypto_btc': 'crypto_daily_table_btc'
    }
    
    table = table_map.get(market)
    if not table:
        raise ValueError(f"Unknown market: {market}")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT stock FROM {table}")
            return [row[0] for row in cur.fetchall()]

def backfill_heikin_ashi_data(market='us', symbol=None, batch_size=1000, days=365):
    """
    Backfill Heikin-Ashi data for a market or specific symbol
    
    Args:
        market: Market to process ('us', 'in', 'crypto', 'crypto_eth', 'crypto_btc')
        symbol: Optional specific symbol to process
        batch_size: Number of rows to update in a single batch
        days: Number of days of historical data to process
    """
    table_map = {
        'us': 'us_daily_table',
        'in': 'in_daily_table',
        'crypto': 'crypto_daily_table',
        'crypto_eth': 'crypto_daily_table_eth',
        'crypto_btc': 'crypto_daily_table_btc'
    }
    
    table = table_map.get(market)
    if not table:
        raise ValueError(f"Unknown market: {market}")
    
    # Get symbols to process
    symbols = [symbol] if symbol else get_symbols(market)
    total_symbols = len(symbols)
    
    print(f"Processing {total_symbols} symbols for {market} market")
    
    # Process each symbol
    for i, sym in enumerate(symbols):
        print(f"Processing symbol {i+1}/{total_symbols}: {sym}")
        
        try:
            # Get OHLC data for this symbol
            with get_db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=days)
                    
                    cur.execute(f"""
                        SELECT 
                            datetime, 
                            open, 
                            high, 
                            low, 
                            close 
                        FROM {table}
                        WHERE stock = %s
                        AND datetime BETWEEN %s AND %s
                        ORDER BY datetime
                    """, (sym, start_date, end_date))
                    
                    data = cur.fetchall()
            
            if not data:
                print(f"No data found for {sym}")
                continue
                
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Calculate Heikin-Ashi values
            ha_df = HeikinAshiTransformer.transform_dataframe(df)
            
            # Update database in batches
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    for i in range(0, len(ha_df), batch_size):
                        batch = ha_df.iloc[i:i+batch_size]
                        
                        update_data = []
                        for _, row in batch.iterrows():
                            update_data.append((
                                float(row['ha_open']),
                                float(row['ha_high']),
                                float(row['ha_low']),
                                float(row['ha_close']),
                                row['ha_color'],
                                row['datetime'],
                                sym
                            ))
                        
                        execute_values(cur, f"""
                            UPDATE {table}
                            SET 
                                ha_open = data.ha_open,
                                ha_high = data.ha_high,
                                ha_low = data.ha_low,
                                ha_close = data.ha_close,
                                ha_color = data.ha_color
                            FROM (VALUES %s) AS data(ha_open, ha_high, ha_low, ha_close, ha_color, datetime, stock)
                            WHERE {table}.datetime = data.datetime AND {table}.stock = data.stock
                        """, update_data)
                    
                    conn.commit()
            
            print(f"Updated {len(ha_df)} rows for {sym}")
            
        except Exception as e:
            print(f"Error processing {sym}: {str(e)}")
    
    print(f"Completed backfilling Heikin-Ashi data for {market} market")

def backfill_all_markets():
    """Backfill Heikin-Ashi data for all markets"""
    markets = ['us', 'in', 'crypto', 'crypto_eth', 'crypto_btc']
    
    for market in markets:
        print(f"\n{'=' * 50}")
        print(f"Processing {market.upper()} market")
        print(f"{'=' * 50}\n")
        
        backfill_heikin_ashi_data(market)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Backfill Heikin-Ashi data')
    parser.add_argument('--market', type=str, choices=['us', 'in', 'crypto', 'crypto_eth', 'crypto_btc', 'all'], 
                        default='all', help='Market to process')
    parser.add_argument('--symbol', type=str, help='Specific symbol to process')
    parser.add_argument('--days', type=int, default=365, help='Number of days of historical data to process')
    
    args = parser.parse_args()
    
    if args.market == 'all':
        backfill_all_markets()
    else:
        backfill_heikin_ashi_data(args.market, args.symbol, days=args.days)