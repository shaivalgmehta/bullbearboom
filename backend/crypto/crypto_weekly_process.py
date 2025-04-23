#!/usr/bin/env python3

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from datetime import datetime, timedelta
from twelvedata import TDClient
from crypto_get_data_functions import fetch_stock_list_polygon, fetch_technical_indicators_polygon, fetch_williams_r_polygon, fetch_force_index_data, store_force_index_data, store_williams_r_data, store_stock_data, store_stock_daily_data, fetch_daily_data_polygon, fetch_obv_data, store_obv_data
from crypto_data_transformer_new import get_transformer
import json
import time
import pytz
import pandas as pd
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

def is_empty_or_none(data):
    return data is None or (isinstance(data, list) and len(data) == 0)

def detect_3day_ha_color_changes(symbol, end_date, base):
    """
    Detect color changes in 3-day Heikin-Ashi data for a specific crypto
    
    Args:
        symbol: Crypto symbol
        end_date: Date to check for color changes
        base: Base currency (usd, eth, btc)
        
    Returns:
        List of alerts or empty list if no alerts
    """
    start_date = end_date - timedelta(days=90)  # Get 90 days of data for reliable calculations
    table_name = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
    
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT datetime, open, high, low, close
                    FROM {table_name}
                    WHERE stock = %s
                    AND datetime BETWEEN %s AND %s
                    ORDER BY datetime
                """, (symbol, start_date, end_date))
                
                rows = cur.fetchall()
        
        if not rows or len(rows) < 6:  # Need at least 6 days for two 3-day periods
            return []
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        
        # Aggregate to 3-day periods
        agg_3d = HeikinAshiTransformer.aggregate_to_custom_periods(df, 3)
        
        # Apply Heikin-Ashi transformation
        ha_3d = HeikinAshiTransformer.transform_dataframe(agg_3d)
        
        # Check for color changes
        if len(ha_3d) < 2:
            return []
        
        # Get latest two periods
        current = ha_3d.iloc[-1].to_dict()
        previous = ha_3d.iloc[-2].to_dict()
        
        # Check if the last aggregated period includes our target date
        current_period_end = pd.to_datetime(current['datetime']).date()
        if current_period_end != end_date.date():
            return []
        
        # Detect color change
        change = HeikinAshiTransformer.detect_color_change(current, previous)
        if not change:
            return []
        
        # Create alert based on change type
        if change == "red_to_green":
            return [{
                "type": "heikin_ashi_3d_bullish",
                "value": change,
                "description": "3-day Heikin-Ashi color change: Bearish to Bullish"
            }]
        elif change == "green_to_red":
            return [{
                "type": "heikin_ashi_3d_bearish",
                "value": change,
                "description": "3-day Heikin-Ashi color change: Bullish to Bearish"
            }]
    except Exception as e:
        print(f"Error detecting 3-day Heikin-Ashi color changes for {symbol}/{base}: {str(e)}")
    
    return []

def detect_2week_ha_color_changes(symbol, end_date, base):
    """
    Detect color changes in 2-week Heikin-Ashi data for a specific crypto
    
    Args:
        symbol: Crypto symbol
        end_date: Date to check for color changes
        base: Base currency (usd, eth, btc)
        
    Returns:
        List of alerts or empty list if no alerts
    """
    start_date = end_date - timedelta(days=200)  # Get 200 days of data for reliable calculations
    table_name = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
    
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT datetime, open, high, low, close
                    FROM {table_name}
                    WHERE stock = %s
                    AND datetime BETWEEN %s AND %s
                    ORDER BY datetime
                """, (symbol, start_date, end_date))
                
                rows = cur.fetchall()
        
        if not rows or len(rows) < 20:  # Need at least 20 days for two 2-week periods (assuming 5 days/week)
            return []
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        
        # First aggregate to weekly (5 trading days)
        weekly_df = HeikinAshiTransformer.aggregate_to_custom_periods(df, 5)
        
        # Then aggregate to 2-week periods
        agg_2w = HeikinAshiTransformer.aggregate_to_custom_periods(weekly_df, 2)
        
        # Apply Heikin-Ashi transformation
        ha_2w = HeikinAshiTransformer.transform_dataframe(agg_2w)
        
        # Check for color changes
        if len(ha_2w) < 2:
            return []
        
        # Get latest two periods
        current = ha_2w.iloc[-1].to_dict()
        previous = ha_2w.iloc[-2].to_dict()
        
        # Check if the last aggregated period includes our target date
        current_period_end = pd.to_datetime(current['datetime']).date()
        # Allow a few days of tolerance since it's a 2-week period
        date_diff = abs((current_period_end - end_date.date()).days)
        if date_diff > 5:  # More than 5 days difference, probably not the right period
            return []
        
        # Detect color change
        change = HeikinAshiTransformer.detect_color_change(current, previous)
        if not change:
            return []
        
        # Create alert based on change type
        if change == "red_to_green":
            return [{
                "type": "heikin_ashi_2w_bullish",
                "value": change,
                "description": "2-week Heikin-Ashi color change: Bearish to Bullish"
            }]
        elif change == "green_to_red":
            return [{
                "type": "heikin_ashi_2w_bearish",
                "value": change,
                "description": "2-week Heikin-Ashi color change: Bullish to Bearish"
            }]
    except Exception as e:
        print(f"Error detecting 2-week Heikin-Ashi color changes for {symbol}/{base}: {str(e)}")
    
    return []

def process_stock(stock, base, end_date):
    symbol = stock['symbol']
    try:
        # Fetch all required data
        williams_r_data = fetch_williams_r_polygon(symbol, db_params, end_date, base)

        # Process Williams %R data
        if not is_empty_or_none(williams_r_data):
            williams_r_transformed_data = williams_r_transformer.transform(williams_r_data, symbol, base)
            store_williams_r_data(williams_r_transformed_data, symbol, base)
            print(f"Williams %R data for {symbol}/{base} has been processed and stored")
        else:
            print(f"Skipping Williams %R for {symbol}/{base} due to insufficient data or other criteria not met")

        force_index_data = fetch_force_index_data(symbol, db_params, end_date, base)
        # Process Force Index data
        if not is_empty_or_none(force_index_data):
            force_index_transformed_data = force_index_transformer.transform(force_index_data, symbol, base)
            store_force_index_data(force_index_transformed_data, symbol, base)
            print(f"Force Index data for {symbol}/{base} has been processed and stored")
        else:
            print(f"Skipping Force Index for {symbol}/{base} due to insufficient data or other criteria not met")

        obv_data = fetch_obv_data(symbol, db_params, end_date, base)
        if not is_empty_or_none(obv_data):
            obv_transformed_data = obv_transformer.transform(obv_data, symbol, base)
            store_obv_data(obv_transformed_data, symbol, base)
            print(f"OBV data for {symbol}/{base} has been processed and stored")
        else:
            print(f"Skipping OBV for {symbol}/{base} due to insufficient data or other criteria not met")

        # Process Heikin-Ashi alerts
        ha_3d_alerts = detect_3day_ha_color_changes(symbol, end_date, base)
        ha_2w_alerts = detect_2week_ha_color_changes(symbol, end_date, base)
        
        # Combine all Heikin-Ashi alerts
        ha_alerts = ha_3d_alerts + ha_2w_alerts
        
        if ha_alerts:
            print(f"Found {len(ha_alerts)} Heikin-Ashi alerts for {symbol}/{base}")
            
            # Get crypto name
            alerts_table = f"crypto_alerts_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
            daily_table = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
            
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT crypto_name
                        FROM {daily_table}
                        WHERE stock = %s
                        ORDER BY datetime DESC
                        LIMIT 1
                    """, (symbol,))
                    
                    crypto_name_result = cur.fetchone()
                    crypto_name = crypto_name_result[0] if crypto_name_result else symbol
                    
                    # Check if an alert already exists for this symbol on this date
                    cur.execute(f"""
                        SELECT alerts
                        FROM {alerts_table}
                        WHERE stock = %s AND DATE(datetime) = DATE(%s)
                    """, (symbol, end_date))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        # Update existing alerts
                        existing_alerts = json.loads(existing[0]) if existing[0] else []
                        
                        # Add new alerts, avoiding duplicates
                        existing_alert_types = {a['type'] for a in existing_alerts}
                        for alert in ha_alerts:
                            if alert['type'] not in existing_alert_types:
                                existing_alerts.append(alert)
                                existing_alert_types.add(alert['type'])
                        
                        cur.execute(f"""
                            UPDATE {alerts_table}
                            SET alerts = %s
                            WHERE stock = %s AND DATE(datetime) = DATE(%s)
                        """, (json.dumps(existing_alerts), symbol, end_date))
                    else:
                        # Insert new alert
                        cur.execute(f"""
                            INSERT INTO {alerts_table} (datetime, stock, crypto_name, alerts)
                            VALUES (%s, %s, %s, %s)
                        """, (end_date, symbol, crypto_name, json.dumps(ha_alerts)))
                    
                    conn.commit()

        if (is_empty_or_none(williams_r_data) and 
            is_empty_or_none(force_index_data) and
            is_empty_or_none(obv_data) and
            not ha_alerts):
            print(f"No data processed for {symbol}/{base} due to insufficient data or other criteria not met")
        else:
            print(f"Data for {symbol}/{base} has been stored in TimescaleDB")

    except Exception as e:
        print(f"Error processing {symbol}/{base}: {str(e)}")

def process_stock_batch(batch, base, end_date):
    for stock in batch:
        process_stock(stock, base, end_date)


def rank_weekly_metrics(conn, base, end_date):
    table_name = f"crypto_weekly_table{'_' + base if base != 'usd' else ''}"
    
    # Fields to rank
    fields = ['williams_r', 'williams_r_ema', 'force_index_7_week', 'force_index_52_week']
    
    # Query to get the data
    query = f"""
        SELECT stock, {', '.join(fields)}
        FROM {table_name}
        WHERE datetime = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (end_date,))
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
            cur.execute(update_query, ranks + [row['stock'], end_date])
    
    conn.commit()
    
    # Print statistics
    for field in fields:
        ranked_count = df[f"{field}_rank"].notna().sum()
        print(f"Rankings updated for {base.upper()} base on {end_date}")
        print(f"Ranked {ranked_count} out of {len(df)} stocks for {field}.")

#################### MAIN PROCESS ########################################################

def main():
    # Fetch stock list
    stocks = fetch_stock_list_polygon()

    # Calculate the date range for the last X weeks
    current = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    # Calculate days since Sunday (weekday() returns 0 for Monday, 6 for Sunday)
    days_since_sunday = (current.weekday() + 1) % 7

    # Subtract those days to get to the most recent Sunday
    end_date = current - timedelta(days=days_since_sunday)
    dates_to_process = [end_date - timedelta(weeks=i) for i in range(1)]
    dates_to_process.reverse()

    # Define base currencies
    bases = ['usd', 'eth', 'btc']

    # Get the appropriate transformers
    global williams_r_transformer, force_index_transformer, obv_transformer
    williams_r_transformer = get_transformer('williams_r', db_params)
    force_index_transformer = get_transformer('force_index', db_params)
    obv_transformer = get_transformer('anchored_obv', db_params)

    # Process data for each week
    for end_date in dates_to_process:
        print(f"\n{'='*60}")
        print(f"Processing data for week: {end_date.date()}")
        print(f"{'='*60}")

        # Process stocks for each base currency
        for base in bases:
            print(f"\n{'='*40}")
            print(f"Starting processing for base currency: {base.upper()} for week {end_date.date()}")
            print(f"{'='*40}\n")

            # Process stocks in batches
            batch_size = 800
            for i in range(0, len(stocks), batch_size):
                batch = stocks[i:i+batch_size]
                
                print(f"Processing batch {i//batch_size + 1} of {len(stocks)//batch_size + 1} for {base.upper()}")
                print("Stocks in this batch:")
                for stock in batch:
                    print(f"- {stock['symbol']}: {stock['name']}")
                
                start_time = time.time()
                
                process_stock_batch(batch, base, end_date)
                
                elapsed_time = time.time() - start_time
                print(f"Batch processing time: {elapsed_time:.2f} seconds")
                
                print(f"Finished processing batch for {base.upper()}. Moving to next batch.\n")

            print(f"Completed processing all stocks for {base.upper()} for week {end_date.date()}")

        print(f"\nCompleted processing for week: {end_date.date()}")

    print("\nAll stocks have been processed for all base currencies")

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