import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# TimescaleDB connection details
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def setup_database():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    # Create screener_table (existing table)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS screener_table (
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
            williams_r NUMERIC
        );
    """)

    # Create hypertable for screener_table
    cur.execute("""
        SELECT create_hypertable('screener_table', 'time', if_not_exists => TRUE);
    """)

    # Create indexes for efficient querying on screener_table (existing indexes)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_screener_stock ON screener_table (stock);
        CREATE INDEX IF NOT EXISTS idx_screener_market_cap ON screener_table (market_cap);
        CREATE INDEX IF NOT EXISTS idx_screener_pe_ratio ON screener_table (pe_ratio);
        CREATE INDEX IF NOT EXISTS idx_screener_ev_ebitda ON screener_table (ev_ebitda);
        CREATE INDEX IF NOT EXISTS idx_screener_pb_ratio ON screener_table (pb_ratio);
        CREATE INDEX IF NOT EXISTS idx_screener_peg_ratio ON screener_table (peg_ratio);
        CREATE INDEX IF NOT EXISTS idx_screener_sales_change ON screener_table (sales_change_percent);
        CREATE INDEX IF NOT EXISTS idx_screener_ebitda_change ON screener_table (ebitda_change_percent);
        CREATE INDEX IF NOT EXISTS idx_screener_roce ON screener_table (roce);
        CREATE INDEX IF NOT EXISTS idx_screener_dcf ON screener_table (discounted_cash_flow);
        CREATE INDEX IF NOT EXISTS idx_screener_ema ON screener_table (ema);
        CREATE INDEX IF NOT EXISTS idx_screener_williams_r ON screener_table (williams_r);
    """)

    # Create williams_r_table (new table)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS williams_r_table (
            time TIMESTAMPTZ NOT NULL,
            stock TEXT NOT NULL,
            williams_r NUMERIC,
            williams_r_ema NUMERIC,
            williams_r_momentum_alert_state TEXT
        );
    """)

    # Create hypertable for williams_r_table
    cur.execute("""
        SELECT create_hypertable('williams_r_table', 'time', if_not_exists => TRUE);
    """)

    # Create index for efficient querying on williams_r_table
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_williams_r_stock ON williams_r_table (stock);
    """)

   # Create force_index_table (new table)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS force_index_table (
            time TIMESTAMPTZ NOT NULL,
            stock TEXT NOT NULL,
            force_index_7_week NUMERIC,
            force_index_52_week NUMERIC,
            last_week_force_index_7_week NUMERIC,
            last_week_force_index_52_week NUMERIC,
            force_index_alert_state TEXT
        );
    """)

    # Create hypertable for force_index_table
    cur.execute("""
        SELECT create_hypertable('force_index_table', 'time', if_not_exists => TRUE);
    """)

    # Create index for efficient querying on force_index_table
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_force_index_stock ON force_index_table (stock);
    """)

    # Modify screener_table to include williams_r_ema, force_index_alert_state, and force index columns
    cur.execute("""
        ALTER TABLE screener_table
        ADD COLUMN IF NOT EXISTS williams_r_ema NUMERIC,
        ADD COLUMN IF NOT EXISTS williams_r_momentum_alert_state TEXT,
        ADD COLUMN IF NOT EXISTS force_index_7_week NUMERIC,
        ADD COLUMN IF NOT EXISTS force_index_52_week NUMERIC,
        ADD COLUMN IF NOT EXISTS force_index_alert_state TEXT;
    """)

    # Create indexes for new columns in screener_table
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_screener_williams_r_ema ON screener_table (williams_r_ema);
        CREATE INDEX IF NOT EXISTS idx_screener_williams_r_momentum_alert_state ON screener_table (williams_r_momentum_alert_state);
        CREATE INDEX IF NOT EXISTS idx_screener_force_index_7_week ON screener_table (force_index_7_week);
        CREATE INDEX IF NOT EXISTS idx_screener_force_index_52_week ON screener_table (force_index_52_week);
        CREATE INDEX IF NOT EXISTS idx_screener_force_index_alert_state ON screener_table (force_index_alert_state);
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Database schema set up successfully.")

if __name__ == "__main__":
    setup_database()