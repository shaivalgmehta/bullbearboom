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
            # Create Crypto screener_table
            create_table(cur, 'crypto_screener_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                crypto_name TEXT,
                close NUMERIC,
                ema NUMERIC,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                CONSTRAINT crypto_screener_pkey PRIMARY KEY (datetime, stock)

            """)


            # Create Crypto daily time series table
            create_table(cur, 'crypto_daily_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                stock_name TEXT,
                crypto_name TEXT,
                open NUMERIC,
                close NUMERIC,
                ema NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT crypto_daily_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create Crypto weekly time series table
            create_table(cur, 'crypto_weekly_table', """
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
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT crypto_weekly_pkey PRIMARY KEY (datetime, stock)
            """)

            # Create indexes for all tables
            for table, columns in {
                'crypto_screener_table': ['stock'],
                'crypto_daily_table': ['stock', 'crypto_name'],
                'crypto_weekly_table': ['stock']
            }.items():
                for column in columns:
                    create_index(cur, table, column)

    print("Database schema set up successfully.")

if __name__ == "__main__":
    setup_database()