import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime, timedelta
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

def process_alerts_for_base(base, date):
    table_suffix = f"_{base.lower()}" if base.lower() != "usd" else ""
    alerts_table = f"crypto_alerts_table{table_suffix}"
    weekly_table = f"crypto_weekly_table{table_suffix}"
    daily_table = f"crypto_daily_table{table_suffix}"
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First, clear existing alerts for this date and base
            cur.execute(f"""
                DELETE FROM {alerts_table}
                WHERE DATE(datetime) = DATE(%s)
            """, (date,))
            
            # Find cryptos where both alerts are triggered and join with daily table for name
            cur.execute(f"""
                SELECT w.datetime, w.stock, d.crypto_name, w.anchored_obv_alert_state
                FROM {weekly_table} w
                LEFT JOIN (
                    SELECT DISTINCT ON (stock) stock, crypto_name
                    FROM crypto_daily_table{table_suffix}
                    ORDER BY stock, datetime DESC
                ) d ON w.stock = d.stock
                WHERE DATE(w.datetime) = DATE(%s)
                AND w.williams_r_momentum_alert_state = '$$$'
                AND w.force_index_alert_state = '$$$'
                AND LOWER(w.stock) LIKE '%%usd%%'
            """, (date,))
            
            alerts = cur.fetchall()
            if alerts:
                values = [(
                    alert[0],  # datetime
                    alert[1],  # stock
                    alert[2],  # crypto_name
                    '$$$',     # oversold_alert (for non-base pairs)
                    alert[3]   # anchored_obv_alert_state
                ) for alert in alerts]
                
                execute_values(cur, f"""
                    INSERT INTO {alerts_table} (
                        datetime, stock, crypto_name,
                        oversold_alert, anchored_obv_alert_state
                    ) VALUES %s
                """, values)
            
            conn.commit()
            return len(alerts)

def main():
    current_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    dates_to_process = [current_date - timedelta(days=i) for i in range(9)]
    dates_to_process.reverse()
    
    bases = ['usd', 'eth', 'btc']

    for date in dates_to_process:
        for base in bases:
            alerts_count = process_alerts_for_base(base, date)
            print(f"Processed alerts for {base.upper()} base on {date.date()}: {alerts_count} alerts generated")

if __name__ == "__main__":
    main()