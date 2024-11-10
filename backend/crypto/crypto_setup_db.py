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
                all_time_high NUMERIC,
                ath_percentage NUMERIC,
                ema NUMERIC,
                ema_metric NUMERIC,
                ema_rank NUMERIC,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                anchored_obv_alert_state TEXT,                
                ema_rank NUMERIC,
                williams_r_rank NUMERIC,
                williams_r_ema_rank NUMERIC,
                force_index_7_week_rank NUMERIC,
                force_index_52_week_rank NUMERIC,
                price_change_3m NUMERIC,
                price_change_6m NUMERIC,
                price_change_12m NUMERIC,   
                anchored_obv_alert_state TEXT,                
                CONSTRAINT crypto_screener_pkey PRIMARY KEY (datetime, stock)

            """)

            create_table(cur, 'crypto_alerts_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                crypto_name TEXT,
                oversold_alert TEXT,
                anchored_obv_alert_state TEXT,
                CONSTRAINT crypto_alerts_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create Crypto daily time series table
            create_table(cur, 'crypto_daily_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                stock_name TEXT,
                crypto_name TEXT,
                open NUMERIC,
                close NUMERIC,
                all_time_high NUMERIC,
                ath_percentage NUMERIC,
                volume NUMERIC,
                high NUMERIC,
                low NUMERIC,
                ema NUMERIC,
                ema_metric NUMERIC,
                ema_rank NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                price_change_3m NUMERIC,
                price_change_6m NUMERIC,
                price_change_12m NUMERIC,                   
                CONSTRAINT crypto_daily_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create Crypto weekly time series table
            create_table(cur, 'crypto_weekly_table', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_rank NUMERIC,
                williams_r_ema_rank NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_7_week_rank NUMERIC,
                force_index_52_week_rank NUMERIC,
                last_week_force_index_7_week NUMERIC,
                last_week_force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                anchored_obv NUMERIC,
                anchored_obv_alert_state TEXT,
                anchor_date TIMESTAMPTZ,
                obv_confidence NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT crypto_weekly_pkey PRIMARY KEY (datetime, stock)
            """)

            # Create Crypto ETH screener_table
            create_table(cur, 'crypto_screener_table_eth', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                crypto_name TEXT,
                close NUMERIC,
                all_time_high NUMERIC,
                ath_percentage NUMERIC,
                ema NUMERIC,
                ema_metric NUMERIC,
                ema_rank NUMERIC,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                anchored_obv_alert_state TEXT,                
                ema_rank NUMERIC,
                williams_r_rank NUMERIC,
                williams_r_ema_rank NUMERIC,
                force_index_7_week_rank NUMERIC,
                force_index_52_week_rank NUMERIC,
                price_change_3m NUMERIC,
                price_change_6m NUMERIC,
                price_change_12m NUMERIC,   
                anchored_obv_alert_state TEXT,               
                CONSTRAINT crypto_screener_eth_pkey PRIMARY KEY (datetime, stock)

            """)

            create_table(cur, 'crypto_alerts_table_eth', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                crypto_name TEXT,
                oversold_alert TEXT,
                anchored_obv_alert_state TEXT,
                CONSTRAINT crypto_alerts_eth_pkey PRIMARY KEY (datetime, stock)
            """)

            # Create Crypto ETH daily time series table
            create_table(cur, 'crypto_daily_table_eth', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                stock_name TEXT,
                crypto_name TEXT,
                open NUMERIC,
                close NUMERIC,
                all_time_high NUMERIC,
                ath_percentage NUMERIC,
                volume NUMERIC,
                high NUMERIC,
                low NUMERIC,
                ema NUMERIC,
                ema_metric NUMERIC,
                ema_rank NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                price_change_3m NUMERIC,
                price_change_6m NUMERIC,
                price_change_12m NUMERIC,    
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT crypto_daily_eth_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create Crypto ETH weekly time series table
            create_table(cur, 'crypto_weekly_table_eth', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_rank NUMERIC,
                williams_r_ema_rank NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_7_week_rank NUMERIC,
                force_index_52_week_rank NUMERIC,
                last_week_force_index_7_week NUMERIC,
                last_week_force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                anchored_obv NUMERIC,
                anchored_obv_alert_state TEXT,
                anchor_date TIMESTAMPTZ,
                obv_confidence NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT crypto_weekly_eth_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create Crypto BTC screener_table
            create_table(cur, 'crypto_screener_table_btc', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                crypto_name TEXT,
                close NUMERIC,
                all_time_high NUMERIC,
                ath_percentage NUMERIC,
                ema NUMERIC,
                ema_metric NUMERIC,
                ema_rank NUMERIC,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                anchored_obv_alert_state TEXT,                
                ema_rank NUMERIC,
                williams_r_rank NUMERIC,
                williams_r_ema_rank NUMERIC,
                force_index_7_week_rank NUMERIC,
                force_index_52_week_rank NUMERIC,
                price_change_3m NUMERIC,
                price_change_6m NUMERIC,
                price_change_12m NUMERIC,   
                anchored_obv_alert_state TEXT,                                 
                CONSTRAINT crypto_screener_btc_pkey PRIMARY KEY (datetime, stock)

            """)

            create_table(cur, 'crypto_alerts_table_btc', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                crypto_name TEXT,
                oversold_alert TEXT,
                anchored_obv_alert_state TEXT,
                CONSTRAINT crypto_alerts_btc_pkey PRIMARY KEY (datetime, stock)
            """)

            # Create Crypto BTC daily time series table
            create_table(cur, 'crypto_daily_table_btc', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                stock_name TEXT,
                crypto_name TEXT,
                open NUMERIC,
                close NUMERIC,
                all_time_high NUMERIC,
                ath_percentage NUMERIC,
                volume NUMERIC,
                high NUMERIC,
                low NUMERIC,
                ema NUMERIC,
                ema_metric NUMERIC,
                ema_rank NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                price_change_3m NUMERIC,
                price_change_6m NUMERIC,
                price_change_12m NUMERIC,    
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT crypto_daily_btc_pkey PRIMARY KEY (datetime, stock)
            """)


            # Create Crypto BTC weekly time series table
            create_table(cur, 'crypto_weekly_table_btc', """
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                williams_r NUMERIC,
                williams_r_ema NUMERIC,
                williams_r_rank NUMERIC,
                williams_r_ema_rank NUMERIC,
                williams_r_momentum_alert_state TEXT,
                force_index_7_week NUMERIC,
                force_index_52_week NUMERIC,
                force_index_7_week_rank NUMERIC,
                force_index_52_week_rank NUMERIC,
                last_week_force_index_7_week NUMERIC,
                last_week_force_index_52_week NUMERIC,
                force_index_alert_state TEXT,
                anchored_obv NUMERIC,
                anchored_obv_alert_state TEXT,
                anchor_date TIMESTAMPTZ,
                obv_confidence NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT crypto_weekly_btc_pkey PRIMARY KEY (datetime, stock)
            """)

            # Create indexes for all tables
            for table, columns in {
                'crypto_screener_table': ['stock'],
                'crypto_alerts_table': ['stock'],
                'crypto_daily_table': ['stock', 'crypto_name'],
                'crypto_weekly_table': ['stock'],
                'crypto_screener_table_eth': ['stock'],
                'crypto_alerts_table_eth': ['stock'],
                'crypto_daily_table_eth': ['stock', 'crypto_name'],
                'crypto_weekly_table_eth': ['stock'],
                'crypto_screener_table_btc': ['stock'],
                'crypto_alerts_table_btc': ['stock'],
                'crypto_daily_table_btc': ['stock', 'crypto_name'],
                'crypto_weekly_table_btc': ['stock']
            }.items():
                for column in columns:
                    create_index(cur, table, column)

    print("Database schema set up successfully.")

if __name__ == "__main__":
    setup_database()