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

def process_alerts(date):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First, clear existing alerts for this date
            cur.execute("""
                DELETE FROM us_alerts_table
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
                FROM us_weekly_table w
                LEFT JOIN (
                    SELECT DISTINCT ON (stock) stock, stock_name
                    FROM us_daily_table
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
                
                if williams_alert == '$$$':
                    active_alerts.append({
                        "type": "oversold",
                        "value": williams_alert,
                        "description": "Williams %R Oversold Alert"
                    })
                
                if williams_alert == '$' and force_index_alert in ['$', '$$$']:
                    active_alerts.append({
                        "type": "momentum_continuation",
                        "value": williams_alert,
                        "description": "Williams %R Momentum Continuation"
                    })
                
                if force_index_alert == '$$$':
                    active_alerts.append({
                        "type": "force_index",
                        "value": force_index_alert,
                        "description": "Force Index Crossover Alert"
                    })
                
                if obv_alert == '$$$':
                    active_alerts.append({
                        "type": "obv_positive",
                        "value": obv_alert,
                        "description": "OBV Positive Crossover"
                    })
                
                if obv_alert == '-$$$':
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
                        stock_name,
                        json.dumps(active_alerts)
                    ))
            
            if alerts_to_insert:
                execute_values(cur, """
                    INSERT INTO us_alerts_table (
                        datetime, stock, stock_name, alerts
                    )
                    VALUES %s
                """, alerts_to_insert)
            
            conn.commit()
            return len(alerts_to_insert)

def main():
    current_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    dates_to_process = [current_date - timedelta(days=i) for i in range(7)]
    dates_to_process.reverse()

    for date in dates_to_process:
        alerts_count = process_alerts(date)
        print(f"Processed alerts for {date.date()}: {alerts_count} alerts generated")

if __name__ == "__main__":
    main()