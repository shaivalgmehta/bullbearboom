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

    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            price NUMERIC,
            volume NUMERIC
        );
    """)

    # Create hypertable
    cur.execute("""
        SELECT create_hypertable('stock_data', 'time', if_not_exists => TRUE);
    """)

    # Create index on symbol for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_stock_data_symbol ON stock_data (symbol);
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Database schema set up successfully.")

if __name__ == "__main__":
    setup_database()