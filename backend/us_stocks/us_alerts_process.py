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
            new_alerts_by_stock = {}
            
            for alert in stock_alerts:
                datetime_val, stock, stock_name, williams_alert, force_index_alert, obv_alert = alert
                
                # Only process stocks that have at least one active alert
                active_alerts = []
                
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
                
                # Only include stocks with active alerts
                if active_alerts:
                    new_alerts_by_stock[stock] = {
                        "datetime_val": datetime_val,
                        "stock": stock,
                        "stock_name": stock_name,
                        "alerts": active_alerts
                    }
            
            # Now fetch existing alerts to merge with new ones
            existing_alerts = {}
            cur.execute("""
                SELECT stock, alerts 
                FROM us_alerts_table
                WHERE DATE(datetime) = DATE(%s)
            """, (date,))
            
            for row in cur.fetchall():
                existing_alerts[row[0]] = json.loads(row[1]) if row[1] else []
            
            # Prepare final data with merged alerts
            alerts_to_insert = []
            alerts_to_update = []
            
            # Process stocks with new alerts
            for stock, data in new_alerts_by_stock.items():
                if stock in existing_alerts:
                    # Merge alerts, avoiding duplicates
                    existing_types = {alert['type'] for alert in existing_alerts[stock]}
                    merged_alerts = existing_alerts[stock].copy()
                    
                    for new_alert in data['alerts']:
                        if new_alert['type'] not in existing_types:
                            merged_alerts.append(new_alert)
                    
                    alerts_to_update.append((
                        json.dumps(merged_alerts),
                        stock,
                        date
                    ))
                else:
                    # Insert new record
                    alerts_to_insert.append((
                        data['datetime_val'],
                        stock,
                        data['stock_name'],
                        json.dumps(data['alerts'])
                    ))
            
            # Execute updates
            if alerts_to_update:
                for alerts_json, stock_symbol, alert_date in alerts_to_update:
                    cur.execute("""
                        UPDATE us_alerts_table
                        SET alerts = %s
                        WHERE stock = %s AND DATE(datetime) = DATE(%s)
                    """, (alerts_json, stock_symbol, alert_date))
            
            # Execute inserts
            if alerts_to_insert:
                execute_values(cur, """
                    INSERT INTO us_alerts_table (
                        datetime, stock, stock_name, alerts
                    )
                    VALUES %s
                """, alerts_to_insert)
            
            conn.commit()
            return len(alerts_to_insert) + len(alerts_to_update)

def main():
    current_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    dates_to_process = [current_date - timedelta(days=i) for i in range(9)]
    dates_to_process.reverse()

    for date in dates_to_process:
        alerts_count = process_alerts(date)
        print(f"Processed alerts for {date.date()}: {alerts_count} alerts generated")

if __name__ == "__main__":
    main()