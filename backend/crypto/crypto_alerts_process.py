import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import json

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
            
            # Find all potential alert conditions for this date and base
            cur.execute(f"""
                SELECT 
                    w.datetime, 
                    w.stock, 
                    d.crypto_name, 
                    w.williams_r_momentum_alert_state,
                    w.force_index_alert_state,
                    w.anchored_obv_alert_state
                FROM {weekly_table} w
                LEFT JOIN (
                    SELECT DISTINCT ON (stock) stock, crypto_name
                    FROM {daily_table}
                    ORDER BY stock, datetime DESC
                ) d ON w.stock = d.stock
                WHERE DATE(w.datetime) = DATE(%s)
                AND LOWER(w.stock) LIKE '%%{base.lower()}%%'
            """, (date,))
            
            crypto_alerts = cur.fetchall()
            alerts_to_insert = []
            
            for alert in crypto_alerts:
                datetime_val, stock, crypto_name, williams_alert, force_index_alert, obv_alert = alert
                
                # Only process cryptos that have at least one active alert
                active_alerts = []
                
                # Check for oversold alert (original criteria: williams_r_momentum_alert = $$$ AND force_index_alert = $$$)
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
                
                # Only insert if there are active alerts
                if active_alerts:
                    alerts_to_insert.append((
                        datetime_val,
                        stock,
                        crypto_name,
                        json.dumps(active_alerts)
                    ))
            
            if alerts_to_insert:
                execute_values(cur, f"""
                    INSERT INTO {alerts_table} (
                        datetime, stock, crypto_name, alerts
                    )
                    VALUES %s
                """, alerts_to_insert)
            
            conn.commit()
            return len(alerts_to_insert)

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