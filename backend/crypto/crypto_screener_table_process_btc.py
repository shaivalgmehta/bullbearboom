import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime, timedelta

#!/usr/bin/env python3

# Load environment variables
load_dotenv()

# Database connection details
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def update_screener_table():
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get today's date and yesterday's date
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            # Step 1: Clear the screener table
            cur.execute("TRUNCATE TABLE crypto_screener_table_btc")

            # Step 2: Insert latest daily data (today or yesterday)
            cur.execute("""
                INSERT INTO crypto_screener_table_btc (
                    datetime, stock, crypto_name, close, ema
                )
                SELECT DISTINCT ON (stock)
                    datetime, stock, crypto_name, close, ema
                FROM 
                    crypto_daily_table_btc
                WHERE 
                    DATE(datetime) BETWEEN %s AND %s
                    AND LOWER(stock_name) LIKE '%%united states dollar%%'
                ORDER BY 
                    stock, datetime DESC
            """, (yesterday,today))

            # Step 3: Update with weekly data only for stocks that have daily data
            cur.execute("""
                WITH latest_weekly AS (
                    SELECT DISTINCT ON (stock)
                        stock, datetime, williams_r, williams_r_ema, williams_r_momentum_alert_state,
                        force_index_7_week, force_index_52_week, force_index_alert_state
                    FROM crypto_weekly_table_btc
                    WHERE datetime > %s
                    AND stock IN (SELECT stock FROM crypto_screener_table_btc)
                    ORDER BY stock, datetime DESC
                )
                UPDATE crypto_screener_table_btc s
                SET 
                    williams_r = w.williams_r,
                    williams_r_ema = w.williams_r_ema,
                    williams_r_momentum_alert_state = w.williams_r_momentum_alert_state,
                    force_index_7_week = w.force_index_7_week,
                    force_index_52_week = w.force_index_52_week,
                    force_index_alert_state = w.force_index_alert_state
                FROM latest_weekly w
                WHERE s.stock = w.stock
            """, (today - timedelta(days=7),))

            conn.commit()

if __name__ == "__main__":
    update_screener_table()
    print("crypto_screener_table_btc has been updated successfully.")