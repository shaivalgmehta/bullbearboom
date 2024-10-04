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
    execute_query(cur, f"SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE);")

def create_index(cur, table_name, column):
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{column} ON {table_name} ({column});"
    execute_query(cur, query)

def setup_database():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Create US screener_table
            create_table(cur, 'screener_table', """
                time TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                market_cap NUMERIC,
                pe_ratio NUMERIC,
                ev_ebitda NUMERIC,
                pb_ratio NUMERIC,
                peg_ratio NUMERIC,
                last_year_sales NUMERIC,
                current_year_sales NUMERIC,
                sales_change_percent NUMERIC,
                last_year_ebitda NUMERIC,
                current_year_ebitda NUMERIC,
                ebitda_change_percent NUMERIC,
                roce NUMERIC,
                discounted_cash_flow NUMERIC,
                ema NUMERIC,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                closing_price NUMERIC,
                stock_name TEXT
            """)

            # Create Crypto screener_table
            create_table(cur, 'crypto_screener_table', """
                time TIMESTAMPTZ NOT NULL,
                crypto TEXT NOT NULL,
                market_cap NUMERIC,
                ema NUMERIC,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                closing_price NUMERIC,
                crypto_name TEXT
            """)

            # Create williams_r_table
            create_table(cur, 'williams_r_table', """
                time TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT
            """)

            # Create force_index_table
            create_table(cur, 'force_index_table', """
                time TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                last_week_force_index_7_week NUMERIC,
                last_week_force_index_52_week NUMERIC,
                force_index_alert_state TEXT
            """)

            # Create indexes for all tables
            for table, columns in {
                'screener_table': ['stock', 'market_cap', 'pe_ratio', 'pb_ratio', 'peg_ratio', 
                             'sales_change_percent', 'ebitda_change_percent'],
                'williams_r_table': ['stock'],
                'force_index_table': ['stock'],
                'crypto_screener_table': ['crypto', 'market_cap']
            }.items():
                for column in columns:
                    create_index(cur, table, column)

    print("Database schema set up successfully.")

if __name__ == "__main__":
    setup_database()