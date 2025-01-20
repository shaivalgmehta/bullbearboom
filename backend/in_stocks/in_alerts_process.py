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

def process_oversold_alerts(date):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First, clear existing alerts for this date
            cur.execute("""
                DELETE FROM in_alerts_table
                WHERE DATE(datetime) = DATE(%s)
            """, (date,))
            
            # Find stocks where both alerts are triggered
            # and get the OBV alert state
            cur.execute("""
                SELECT w.datetime, w.stock, d.stock_name, w.anchored_obv_alert_state
                FROM in_weekly_table w
                LEFT JOIN (
                    SELECT DISTINCT ON (stock) stock, stock_name
                    FROM in_daily_table
                    ORDER BY stock, datetime DESC
                ) d ON w.stock = d.stock
                WHERE DATE(w.datetime) = DATE(%s)
                AND w.williams_r_momentum_alert_state = '$$$'
                AND w.force_index_alert_state = '$$$'
            """, (date,))
            
            alerts = cur.fetchall()
            
            if alerts:
                values = [(
                    alert[0],  # datetime
                    alert[1],  # stock
                    alert[2],  # stock name
                    '$$$',     # oversold_alert
                    alert[3]   # anchored_obv_alert_state
                ) for alert in alerts]
                
                execute_values(cur, """
                    INSERT INTO in_alerts_table (
                        datetime, stock, stock_name, 
                        oversold_alert, anchored_obv_alert_state
                    )
                    VALUES %s
                """, values)
            
            conn.commit()
            return len(alerts)

def main():
    current_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    dates_to_process = [current_date - timedelta(days=i) for i in range(7)]
    dates_to_process.reverse()

    for date in dates_to_process:
        alerts_count = process_oversold_alerts(date)
        print(f"Processed alerts for {date.date()}: {alerts_count} oversold alerts generated")

if __name__ == "__main__":
    main()