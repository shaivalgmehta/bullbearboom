import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
            yesterday = today - timedelta(days=3)
            
            # Step 1: Clear the screener table
            cur.execute("TRUNCATE TABLE us_screener_table")

            # Step 2: Insert latest daily data (today or yesterday)
            cur.execute("""
                INSERT INTO us_screener_table (
                    datetime, stock, stock_name, close, market_cap, pe_ratio, ev_ebitda, pb_ratio, peg_ratio, ema
                )
                SELECT DISTINCT ON (stock)
                    datetime, stock, stock_name, close, market_cap, pe_ratio, ev_ebitda, pb_ratio, peg_ratio, ema
                FROM 
                    us_daily_table
                WHERE 
                    DATE(datetime) IN (%s, %s)
                ORDER BY 
                    stock, datetime DESC
            """, (today, yesterday))

            # Step 3: Update with weekly data only for stocks that have daily data
            cur.execute("""
                WITH latest_weekly AS (
                    SELECT DISTINCT ON (stock)
                        stock, datetime, williams_r, williams_r_ema, williams_r_momentum_alert_state,
                        force_index_7_week, force_index_52_week, force_index_alert_state
                    FROM us_weekly_table
                    WHERE datetime > %s
                    ORDER BY stock, datetime DESC
                )
                UPDATE us_screener_table s
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

            # Step 4: Update with quarterly data only for stocks that have daily data
            cur.execute("""
                WITH latest_quarters AS (
                    SELECT 
                        stock,
                        MAX(CASE WHEN rn = 1 THEN datetime END) as latest_quarter,
                        MAX(CASE WHEN rn = 2 THEN datetime END) as previous_quarter,
                        MAX(CASE WHEN rn = 1 THEN sales END) as current_quarter_sales,
                        MAX(CASE WHEN rn = 2 THEN sales END) as last_quarter_sales,
                        MAX(CASE WHEN rn = 1 THEN ebitda END) as current_quarter_ebitda,
                        MAX(CASE WHEN rn = 2 THEN ebitda END) as last_quarter_ebitda,
                        MAX(CASE WHEN rn = 1 THEN roce END) as roce,
                        MAX(CASE WHEN rn = 1 THEN free_cash_flow END) as free_cash_flow,
                        MAX(CASE WHEN rn = 1 THEN discounted_cash_flow END) as discounted_cash_flow
                    FROM (
                        SELECT 
                            *, 
                            ROW_NUMBER() OVER (PARTITION BY stock ORDER BY datetime DESC) as rn
                        FROM us_quarterly_table
                        WHERE datetime > %s
                    ) sq
                    WHERE rn <= 2
                    GROUP BY stock
                )
                UPDATE us_screener_table s
                SET 
                    last_quarter_sales = lq.last_quarter_sales,
                    current_quarter_sales = lq.current_quarter_sales,
                    sales_change_percent = CASE 
                        WHEN lq.last_quarter_sales != 0 
                        THEN ((lq.current_quarter_sales - lq.last_quarter_sales) / lq.last_quarter_sales) * 100 
                        ELSE NULL 
                    END,
                    last_quarter_ebitda = lq.last_quarter_ebitda,
                    current_quarter_ebitda = lq.current_quarter_ebitda,
                    ebitda_change_percent = CASE 
                        WHEN lq.last_quarter_ebitda != 0 
                        THEN ((lq.current_quarter_ebitda - lq.last_quarter_ebitda) / lq.last_quarter_ebitda) * 100 
                        ELSE NULL 
                    END,
                    roce = lq.roce,
                    free_cash_flow = lq.free_cash_flow,
                    discounted_cash_flow = lq.discounted_cash_flow
                FROM latest_quarters lq
                WHERE s.stock = lq.stock
            """, (today - timedelta(days=120),))

            conn.commit()

if __name__ == "__main__":
    update_screener_table()
    print("us_screener_table has been updated successfully.")