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

def update_in_screener_table(selected_date=None):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get today's date and yesterday's date
            target_date = selected_date if selected_date else datetime.now().date() - timedelta(days=1)

            # today = datetime.now().date()
            # yesterday = today - timedelta(days=1)
            
            # Step 1: Clear the screener table
            cur.execute("TRUNCATE TABLE in_screener_table")

            # Step 2: Insert latest daily data (today or yesterday)
            cur.execute("""
                INSERT INTO in_screener_table (
                    datetime, stock, stock_name, close, pe_ratio, pb_ratio, peg_ratio, ema, pe_ratio_rank,
                pb_ratio_rank, peg_ratio_rank, earnings_yield, book_to_price, earnings_yield_rank, book_to_price_rank,
                price_change_3m, price_change_6m, price_change_12m, erp5_rank
                )
                SELECT DISTINCT ON (stock)
                    datetime, stock, stock_name, close, pe_ratio, pb_ratio, peg_ratio, ema, pe_ratio_rank,
                pb_ratio_rank, peg_ratio_rank, earnings_yield, book_to_price, earnings_yield_rank, book_to_price_rank,
                price_change_3m, price_change_6m, price_change_12m, erp5_rank
                FROM 
                    in_daily_table
                WHERE 
                    DATE(datetime) BETWEEN %s AND %s
                ORDER BY 
                    stock, datetime DESC
            """, (target_date, target_date))

            # Step 3: Update with weekly data only for stocks that have daily data
            cur.execute("""
                WITH latest_weekly AS (
                    SELECT DISTINCT ON (stock)
                        stock, datetime, williams_r, williams_r_ema, williams_r_momentum_alert_state,
                        force_index_7_week, force_index_52_week, force_index_alert_state, anchored_obv_alert_state
                    FROM in_weekly_table
                    WHERE datetime > %s
                    ORDER BY stock, datetime DESC
                )
                UPDATE in_screener_table s
                SET 
                    williams_r = w.williams_r,
                    williams_r_ema = w.williams_r_ema,
                    williams_r_momentum_alert_state = w.williams_r_momentum_alert_state,
                    force_index_7_week = w.force_index_7_week,
                    force_index_52_week = w.force_index_52_week,
                    force_index_alert_state = w.force_index_alert_state,
                    anchored_obv_alert_state = w.anchored_obv_alert_state
                FROM latest_weekly w
                WHERE s.stock = w.stock
            """, (target_date - timedelta(days=13),))

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
                        MAX(CASE WHEN rn = 1 THEN discounted_cash_flow END) as discounted_cash_flow,
                        -- New fields
                        MAX(CASE WHEN rn = 1 THEN market_cap END) as market_cap,
                        MAX(CASE WHEN rn = 1 THEN ev_ebitda END) as ev_ebitda,                        
                        MAX(CASE WHEN rn = 1 THEN return_on_equity END) as return_on_equity,
                        MAX(CASE WHEN rn = 1 THEN return_on_assets END) as return_on_assets,
                        MAX(CASE WHEN rn = 1 THEN price_to_sales END) as price_to_sales,
                        MAX(CASE WHEN rn = 1 THEN free_cash_flow_yield END) as free_cash_flow_yield,
                        MAX(CASE WHEN rn = 1 THEN shareholder_yield END) as shareholder_yield,
                        MAX(CASE WHEN rn = 1 THEN return_on_equity_rank END) as return_on_equity_rank,
                        MAX(CASE WHEN rn = 1 THEN return_on_assets_rank END) as return_on_assets_rank,
                        MAX(CASE WHEN rn = 1 THEN price_to_sales_rank END) as price_to_sales_rank,
                        MAX(CASE WHEN rn = 1 THEN free_cash_flow_yield_rank END) as free_cash_flow_yield_rank,
                        MAX(CASE WHEN rn = 1 THEN shareholder_yield_rank END) as shareholder_yield_rank,
                        MAX(CASE WHEN rn = 1 THEN ev_ebitda_rank END) as ev_ebitda_rank
                    FROM (
                        SELECT 
                            *, 
                            ROW_NUMBER() OVER (PARTITION BY stock ORDER BY datetime DESC) as rn
                        FROM in_quarterly_table
                        WHERE datetime > %s
                    ) sq
                    WHERE rn <= 2
                    GROUP BY stock
                )
                UPDATE in_screener_table s
                SET 
                    last_quarter_sales = lq.last_quarter_sales,
                    current_quarter_sales = lq.current_quarter_sales,
                    sales_change_percent = CASE 
                        WHEN lq.last_quarter_sales != 0 
                        THEN ((lq.current_quarter_sales - lq.last_quarter_sales) / lq.last_quarter_sales) 
                        ELSE NULL 
                    END,
                    last_quarter_ebitda = lq.last_quarter_ebitda,
                    current_quarter_ebitda = lq.current_quarter_ebitda,
                    ebitda_change_percent = CASE 
                        WHEN lq.last_quarter_ebitda != 0 
                        THEN ((lq.current_quarter_ebitda - lq.last_quarter_ebitda) / lq.last_quarter_ebitda) 
                        ELSE NULL 
                    END,
                    roce = lq.roce,
                    free_cash_flow = lq.free_cash_flow,
                    discounted_cash_flow = lq.discounted_cash_flow,
                    -- Set new fields
                    market_cap = lq.market_cap,
                    ev_ebitda = lq.ev_ebitda,            
                    return_on_equity = lq.return_on_equity,
                    return_on_assets = lq.return_on_assets,
                    price_to_sales = lq.price_to_sales,
                    free_cash_flow_yield = lq.free_cash_flow_yield,
                    shareholder_yield = lq.shareholder_yield,
                    return_on_equity_rank = lq.return_on_equity_rank,
                    return_on_assets_rank = lq.return_on_assets_rank,
                    price_to_sales_rank = lq.price_to_sales_rank,
                    free_cash_flow_yield_rank = lq.free_cash_flow_yield_rank,
                    shareholder_yield_rank = lq.shareholder_yield_rank,
                    ev_ebitda_rank = lq.ev_ebitda_rank                    
                FROM latest_quarters lq
                WHERE s.stock = lq.stock
            """, (target_date - timedelta(days=200),))

            conn.commit()

if __name__ == "__main__":
    update_in_screener_table()
    print("in_screener_table has been updated successfully.")