import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# TimescaleDB connection details
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def execute_query(cur, query):
    cur.execute(query)

def create_table(cur, table_name, columns):
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    execute_query(cur, query)
    execute_query(cur, f"SELECT create_hypertable('{table_name}', 'datetime', if_not_exists => TRUE);")

def create_index(cur, table_name, column):
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{column} ON {table_name} ({column});"
    execute_query(cur, query)

def setup_database():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Create US screener_table
            create_table(cur, 'us_screener_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                stock_name TEXT,
                close NUMERIC,
                market_cap NUMERIC,
                pe_ratio NUMERIC,
                ev_ebitda NUMERIC,
                pb_ratio NUMERIC,
                peg_ratio NUMERIC,
                earnings_yield NUMERIC,
                book_to_price NUMERIC,
                last_quarter_sales NUMERIC,
                current_quarter_sales NUMERIC,
                sales_change_percent NUMERIC,
                last_quarter_ebitda NUMERIC,
                current_quarter_ebitda NUMERIC,
                ebitda_change_percent NUMERIC,
                roce NUMERIC,
                free_cash_flow NUMERIC,
                discounted_cash_flow NUMERIC,
                ema NUMERIC,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                anchored_obv_alert_state TEXT,        
                pe_ratio_rank INTEGER,
                ev_ebitda_rank INTEGER,
                pb_ratio_rank INTEGER,
                peg_ratio_rank INTEGER,
                earnings_yield_rank INTEGER,
                book_to_price_rank INTEGER,
                price_change_3m NUMERIC,
                price_change_6m NUMERIC,
                price_change_12m NUMERIC,                
                CONSTRAINT us_screener_pkey PRIMARY KEY (datetime, stock)

            """)

            create_table(cur, 'us_alerts_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                stock_name TEXT,
                oversold_alert TEXT,
                anchored_obv_alert_state TEXT,
                CONSTRAINT us_alerts_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create US daily time series table
            create_table(cur, 'us_daily_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                stock_name TEXT,
                open NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                high NUMERIC,
                low NUMERIC,
                market_cap NUMERIC,
                pe_ratio NUMERIC,
                ev_ebitda NUMERIC,
                pb_ratio NUMERIC,
                peg_ratio NUMERIC,
                earnings_yield NUMERIC,
                book_to_price NUMERIC,                
                ema NUMERIC,
                pe_ratio_rank INTEGER,
                ev_ebitda_rank INTEGER,
                pb_ratio_rank INTEGER,
                earnings_yield_rank INTEGER,
                book_to_price_rank INTEGER,                
                peg_ratio_rank INTEGER,
                price_change_3m NUMERIC,
                price_change_6m NUMERIC,
                price_change_12m NUMERIC,                 
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT us_daily_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create US weekly time series table
            create_table(cur, 'us_weekly_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                last_week_force_index_7_week NUMERIC,
                last_week_force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                anchored_obv NUMERIC,
                anchored_obv_alert_state TEXT,
                anchor_date TIMESTAMPTZ,
                obv_confidence NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT us_weekly_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create US quarterly data table
            create_table(cur, 'us_quarterly_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                sales NUMERIC,
                ebitda NUMERIC,
                roce NUMERIC,
                free_cash_flow NUMERIC,
                discounted_cash_flow NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT us_quarterly_pkey PRIMARY KEY (datetime, stock)
            """)

            # Create indexes for all tables
            for table, columns in {
                'us_screener_table': ['stock', 'market_cap', 'pe_ratio', 'pb_ratio', 'peg_ratio', 
                             'sales_change_percent', 'ebitda_change_percent'],
                'us_alerts_table': ['stock'],
                'us_daily_table': ['stock'],
                'us_weekly_table': ['stock'],
                'us_quarterly_table': ['stock']
            }.items():
                for column in columns:
                    create_index(cur, table, column)

    print("Database schema set up successfully.")

if __name__ == "__main__":
    setup_database()