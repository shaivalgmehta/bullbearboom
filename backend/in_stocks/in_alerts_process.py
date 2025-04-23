import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import json
import pandas as pd
import sys
sys.path.append(os.path.abspath('..'))

# Import Heikin-Ashi transformer for alert detection
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

def detect_3day_ha_color_changes(symbol, date, lookback_days=90):
    """
    Detect color changes in 3-day Heikin-Ashi data for a specific stock
    
    Args:
        symbol: Stock symbol
        date: Date to check for color changes
        lookback_days: Number of days to look back
        
    Returns:
        List of alerts or empty list if no alerts
    """
    start_date = date - timedelta(days=lookback_days)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT datetime, open, high, low, close
                    FROM in_daily_table
                    WHERE stock = %s
                    AND datetime BETWEEN %s AND %s
                    ORDER BY datetime
                """, (symbol, start_date, date))
                
                rows = cur.fetchall()
        
        if not rows or len(rows) < 6:  # Need at least 6 days for two 3-day periods
            return []
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=['datetime', 'open', 'high', 'low', 'close'])
        
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
        if current_period_end != date.date():
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
        print(f"Error detecting 3-day Heikin-Ashi color changes for {symbol}: {str(e)}")
    
    return []

def detect_2week_ha_color_changes(symbol, date, lookback_days=200):
    """
    Detect color changes in 2-week Heikin-Ashi data for a specific stock
    
    Args:
        symbol: Stock symbol
        date: Date to check for color changes
        lookback_days: Number of days to look back
        
    Returns:
        List of alerts or empty list if no alerts
    """
    start_date = date - timedelta(days=lookback_days)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT datetime, open, high, low, close
                    FROM in_daily_table
                    WHERE stock = %s
                    AND datetime BETWEEN %s AND %s
                    ORDER BY datetime
                """, (symbol, start_date, date))
                
                rows = cur.fetchall()
        
        if not rows or len(rows) < 20:  # Need at least 20 days for two 2-week periods (assuming 5 days/week)
            return []
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=['datetime', 'open', 'high', 'low', 'close'])
        
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
        date_diff = abs((current_period_end - date.date()).days)
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
        print(f"Error detecting 2-week Heikin-Ashi color changes for {symbol}: {str(e)}")
    
    return []

def process_alerts(date):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First, clear existing alerts for this date
            cur.execute("""
                DELETE FROM in_alerts_table
                WHERE DATE(datetime) = DATE(%s)
            """, (date,))
            
            # Find all potential alert conditions
            cur.execute("""
                SELECT 
                    w.datetime, 
                    w.stock, 
                    d.stock_name, 
                    w.williams_r_momentum_alert_state,
                    w.force_index_alert_state,
                    w.anchored_obv_alert_state
                FROM in_weekly_table w
                LEFT JOIN (
                    SELECT DISTINCT ON (stock) stock, stock_name
                    FROM in_daily_table
                    ORDER BY stock, datetime DESC
                ) d ON w.stock = d.stock
                WHERE DATE(w.datetime) = DATE(%s)
            """, (date,))
            
            stock_alerts = cur.fetchall()
            alerts_to_insert = []
            
            for alert in stock_alerts:
                datetime_val, stock, stock_name, williams_alert, force_index_alert, obv_alert = alert
                
                # Only process stocks that have at least one active alert
                active_alerts = []
                
                # Check for existing alert conditions
                if williams_alert == '$$$' and force_index_alert == '$$$':
                    active_alerts.append({
                        "type": "oversold",
                        "value": '$$$',
                        "description": "Force Index Alert & Williams R Alert triggered"
                    })
                
                if obv_alert == '$$$' and williams_alert == '$$$' and force_index_alert == '$$$':
                    active_alerts.append({
                        "type": "obv_positive",
                        "value": obv_alert,
                        "description": "OBV Positive Crossover"
                    })
                
                if obv_alert == '-$$$' and williams_alert == '$$$' and force_index_alert == '$$$':
                    active_alerts.append({
                        "type": "obv_negative",
                        "value": obv_alert,
                        "description": "OBV Negative Crossover"
                    })
                
                # Check for Heikin-Ashi alerts
                ha_3d_alerts = detect_3day_ha_color_changes(stock, date)
                ha_2w_alerts = detect_2week_ha_color_changes(stock, date)
                
                # Add Heikin-Ashi alerts to active alerts
                active_alerts.extend(ha_3d_alerts)
                active_alerts.extend(ha_2w_alerts)
                
                # Only insert if there are active alerts
                if active_alerts:
                    alerts_to_insert.append((
                        datetime_val,
                        stock,
                        stock_name,
                        json.dumps(active_alerts)
                    ))
            
            if alerts_to_insert:
                execute_values(cur, """
                    INSERT INTO in_alerts_table (
                        datetime, stock, stock_name, alerts
                    )
                    VALUES %s
                """, alerts_to_insert)
            
            conn.commit()
            return len(alerts_to_insert)

def main():
    current_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    dates_to_process = [current_date - timedelta(days=i) for i in range(9)]
    dates_to_process.reverse()

    for date in dates_to_process:
        alerts_count = process_alerts(date)
        print(f"Processed alerts for {date.date()}: {alerts_count} alerts generated")

if __name__ == "__main__":
    main()