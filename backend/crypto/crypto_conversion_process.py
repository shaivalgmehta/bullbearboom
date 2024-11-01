import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv
import pytz



class CryptoBaseConverter:
    def __init__(self, db_params: Dict[str, Any]):
        self.db_params = db_params
        
        # Construct the connection string
        user = db_params['user']
        password = db_params['password']
        host = db_params['host']
        dbname = db_params['dbname']
        
        # Handle the port
        port = db_params.get('port')
        port_string = f":{port}" if port and port != 'None' else ""

        connection_string = f"postgresql://{user}:{password}@{host}{port_string}/{dbname}"
        
        self.engine = create_engine(connection_string)
        
        # Initialize metadata
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)

    def create_base_tables(self, base_token: str):
        # Create daily table
        daily_table_name = f"crypto_daily_table_{base_token.lower()}"
        create_daily_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {daily_table_name} (
                datetime TIMESTAMPTZ NOT NULL,
                stock TEXT NOT NULL,
                stock_name TEXT,
                crypto_name TEXT,
                open NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                high NUMERIC,
                low NUMERIC,
                ema NUMERIC,
                ema_metric NUMERIC,
                ema_rank NUMERIC,
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT {daily_table_name}_pkey PRIMARY KEY (datetime, stock)
            )
        """)
        
        # Create weekly table
        weekly_table_name = f"crypto_weekly_table_{base_token.lower()}"
        create_weekly_table_sql = text(f"""
            CREATE TABLE IF NOT EXISTS {weekly_table_name} (
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
                last_modified_date TIMESTAMPTZ NOT NULL,
                CONSTRAINT {weekly_table_name}_pkey PRIMARY KEY (datetime, stock)
            )
        """)
        
        with self.engine.connect() as conn:
            # Create tables
            conn.execute(create_daily_table_sql)
            conn.execute(create_weekly_table_sql)
            
            # Create hypertables
            create_daily_hypertable_sql = text(f"SELECT create_hypertable('{daily_table_name}', 'datetime', if_not_exists => TRUE)")
            create_weekly_hypertable_sql = text(f"SELECT create_hypertable('{weekly_table_name}', 'datetime', if_not_exists => TRUE)")
            
            conn.execute(create_daily_hypertable_sql)
            conn.execute(create_weekly_hypertable_sql)
            
            conn.commit()

    def get_base_token_prices(self, base_token: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        query = text("""
            SELECT datetime, close
            FROM crypto_daily_table
            WHERE stock = :stock AND datetime BETWEEN :start_date AND :end_date
            ORDER BY datetime
        """)
        
        params = {
            'stock': f'X:{base_token}USD',
            'start_date': start_date,
            'end_date': end_date
        }
        
        return pd.read_sql_query(query, self.engine, params=params, index_col='datetime')

    def update_base_table(self, base_token: str, start_date: datetime, end_date: datetime, batch_size=1000):
        table_name = f"crypto_daily_table_{base_token.lower()}"
        try:
            # Get base token prices
            base_prices = self.get_base_token_prices(base_token, start_date, end_date)

            # Get total number of rows
            count_query = text("""
                SELECT COUNT(*) FROM crypto_daily_table
                WHERE datetime BETWEEN :start_date AND :end_date
                  AND stock != :base_stock
            """)
            with self.engine.connect() as conn:
                total_rows = conn.execute(count_query, {'start_date': start_date, 'end_date': end_date, 'base_stock': f'X:{base_token}USD'}).scalar()

            # Process data in batches
            for offset in range(0, total_rows, batch_size):
                # Fetch data from crypto_daily_table
                query = text("""
                    SELECT datetime, stock, stock_name, crypto_name, open, close, volume, high, low
                    FROM crypto_daily_table
                    WHERE datetime BETWEEN :start_date AND :end_date
                      AND stock != :base_stock
                    ORDER BY datetime
                    LIMIT :limit OFFSET :offset
                """)
                params = {
                    'start_date': start_date, 
                    'end_date': end_date,
                    'base_stock': f'X:{base_token}USD',
                    'limit': batch_size,
                    'offset': offset
                }
                df = pd.read_sql_query(query, self.engine, params=params)
                
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)

                # Merge with base prices and perform conversion
                df = df.join(base_prices, how='left', rsuffix='_base')
                for col in ['open', 'close', 'high', 'low']:
                    df[col] = df[col] / df['close_base']
                # df['volume'] = df['volume'] / df['close_base']
                df['last_modified_date'] = datetime.now(pytz.UTC)
                
                # Select only the columns we need
                columns = ['datetime', 'stock', 'stock_name', 'crypto_name', 
                           'open', 'close', 'volume', 'high', 'low',
                           'last_modified_date']
                df = df.reset_index()[columns]
                # Prepare data for upsert
                data = df.to_dict(orient='records')
                # Perform upsert
                table = self.metadata.tables[table_name]
                stmt = insert(table).values(data)
                update_dict = {c.name: c for c in stmt.excluded if c.name not in ['datetime', 'stock']}
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['datetime', 'stock'],
                    set_=update_dict
                )

                with self.engine.begin() as conn:
                    conn.execute(upsert_stmt)

                print(f"Successfully updated {table_name} (rows {offset+1} to {min(offset+batch_size, total_rows)}).")

            print(f"Completed updating {table_name}. Total rows processed: {total_rows}")

        except Exception as e:
            print(f"Error updating {table_name}: {str(e)}")
            raise

    def schedule_updates(self, base_token: str, interval_days: int = 1):
        # This is a placeholder. In a real-world scenario, you'd use a task scheduler like Celery or APScheduler
        print(f"Scheduled update for {base_token} every {interval_days} day(s)")

    def add_new_base_token(self):
            # Ask user for input
            base_token = input("Enter the base token you want to create or update a table for (e.g., ETH, BTC): ").upper()
            
            # Confirm with the user
            confirm = input(f"You are about to create a new or update table for {base_token}. Are you sure? (y/n): ").lower()
            
            if confirm != 'y':
                print("Operation cancelled.")
                return
            
            try:
                # Create the base table
                print(f"Creating /Updating table for {base_token}...")
                self.create_base_table(base_token)
                print(f"Table for {base_token} created / updated successfully.")
                
                # Ask user if they want to populate the table with initial data
                populate = input("Do you want to update the table with initial data? (y/n): ").lower()
                
                if populate == 'y':
                    # Set date range for initial data population
                    end_date = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    start_date = datetime(2018, 1, 1, tzinfo=pytz.UTC)  # Adjust this date as needed
                    
                    print(f"Updating table for {base_token} with data from {start_date.date()} to {end_date.date()}...")
                    self.update_base_table(base_token, start_date, end_date)
                    print(f"Table for {base_token} has been updated with initial data.")
                else:
                    print("Table created but not populated with initial data.")
                
                print(f"Process completed for {base_token}.")
            
            except Exception as e:
                print(f"An error occurred while processing {base_token}: {str(e)}")


# Usage

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

#################### FUNCTION TO CONVERT LAST DAY'S DATA TO OTHER BASES ############################

def run_crypto_conversion_process(end_date: datetime):
    # Initialize CryptoBaseConverter
    converter = CryptoBaseConverter(db_params)

    # Set date range to yesterday and today
    end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date

    # Define base currencies
    bases = ['ETH', 'BTC']

    print("Updating base conversion tables:")
    for base in bases:
        print(f"Updating {base} base table...")
        try:
            # Uncomment the following line if you need to add a new base token
            # converter.add_new_base_token(base)
            
            converter.update_base_table(base, start_date, end_date)
            print(f"Successfully updated {base} base table.")
        except Exception as e:
            print(f"Error updating {base} base table: {str(e)}")

    print("Finished updating all base conversion tables.")


##################### FUNCTION TO ADD NEW BASE TABLE ############################################

def run_add_new_base_token():
    converter = CryptoBaseConverter(db_params)
    converter.add_new_base_token()


if __name__ == "__main__":
        run_add_new_base_token()