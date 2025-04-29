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

def process_alerts(date, base='usd'):
    # Determine table names based on base currency
    alerts_table = f"crypto_alerts_table{'_' + base if base != 'usd' else ''}"
    weekly_table = f"crypto_weekly_table{'_' + base if base != 'usd' else ''}"
    daily_table = f"crypto_daily_table{'_' + base if base != 'usd' else ''}"
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Find all potential alert conditions
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
            """, (date,))
            
            stock_alerts = cur.fetchall()
            new_alerts_by_stock = {}
            
            for alert in stock_alerts:
                datetime_val, stock, crypto_name, williams_alert, force_index_alert, obv_alert = alert
                
                # Only process cryptos that have at least one active alert
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
                
                # Only include cryptos with active alerts
                if active_alerts:
                    new_alerts_by_stock[stock] = {
                        "datetime_val": datetime_val,
                        "stock": stock,
                        "crypto_name": crypto_name,
                        "alerts": active_alerts
                    }
            
            # Now fetch existing alerts to merge with new ones
            existing_alerts = {}
            cur.execute(f"""
                SELECT stock, alerts 
                FROM {alerts_table}
                WHERE DATE(datetime) = DATE(%s)
            """, (date,))
            
            for row in cur.fetchall():
                # Fix: Handle the case when alerts is already a list or still a JSON string
                alerts_data = row[1]
                if alerts_data:
                    if isinstance(alerts_data, str):
                        try:
                            existing_alerts[row[0]] = json.loads(alerts_data)
                        except json.JSONDecodeError:
                            print(f"Warning: Invalid JSON for crypto {row[0]}: {alerts_data}")
                            existing_alerts[row[0]] = []
                    else:
                        # It's already a list or other Python object
                        existing_alerts[row[0]] = alerts_data
                else:
                    existing_alerts[row[0]] = []
            
            # Prepare final data with merged alerts
            alerts_to_insert = []
            alerts_to_update = []
            
            # Process cryptos with new alerts
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
                        data['crypto_name'],
                        json.dumps(data['alerts'])
                    ))
            
            # Execute updates
            if alerts_to_update:
                for alerts_json, stock_symbol, alert_date in alerts_to_update:
                    cur.execute(f"""
                        UPDATE {alerts_table}
                        SET alerts = %s
                        WHERE stock = %s AND DATE(datetime) = DATE(%s)
                    """, (alerts_json, stock_symbol, alert_date))
            
            # Execute inserts
            if alerts_to_insert:
                execute_values(cur, f"""
                    INSERT INTO {alerts_table} (
                        datetime, stock, crypto_name, alerts
                    )
                    VALUES %s
                """, alerts_to_insert)
            
            conn.commit()
            return len(alerts_to_insert) + len(alerts_to_update)

def main():
    current_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    dates_to_process = [current_date - timedelta(days=i) for i in range(9)]
    dates_to_process.reverse()

    # Process for all base currencies
    bases = ['usd', 'eth', 'btc']
    
    for date in dates_to_process:
        for base in bases:
            alerts_count = process_alerts(date, base)
            print(f"Processed {base.upper()} alerts for {date.date()}: {alerts_count} alerts generated")

if __name__ == "__main__":
    main()