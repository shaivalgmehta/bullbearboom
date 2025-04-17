#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv
import json
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

def migrate_crypto_alerts_data(base_currency="all"):
    """
    Migrates data from the old crypto alerts table structure to the new one.
    
    Parameters:
    - base_currency: 'usd', 'eth', 'btc', or 'all' to specify which base currency's alerts to migrate
    """
    bases_to_process = []
    if base_currency == "all" or base_currency == "usd":
        bases_to_process.append(("", "USD"))
    if base_currency == "all" or base_currency == "eth":
        bases_to_process.append(("_eth", "ETH"))
    if base_currency == "all" or base_currency == "btc":
        bases_to_process.append(("_btc", "BTC"))
    
    results = {}
    
    for suffix, base_name in bases_to_process:
        logger.info(f"Starting {base_name} crypto alerts migration")
        results[base_name.lower()] = migrate_base_alerts(suffix, base_name)
    
    return results

def migrate_base_alerts(suffix, base_name):
    """
    Migrate alerts for a specific base currency (USD, ETH, BTC)
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Step 1: Check if the backup table already exists
                backup_table = f"crypto_alerts_table{suffix}_backup"
                alerts_table = f"crypto_alerts_table{suffix}"
                
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{backup_table}'
                    )
                """)
                backup_exists = cur.fetchone()[0]
                
                if not backup_exists:
                    # Create a backup of the existing table
                    logger.info(f"Creating backup of existing {base_name} crypto alerts table")
                    cur.execute(f"""
                        CREATE TABLE {backup_table} AS 
                        SELECT * FROM {alerts_table}
                    """)
                    logger.info("Backup created successfully")
                else:
                    logger.info(f"Backup table for {base_name} already exists, skipping backup creation")
                
                # Step 2: Fetch data from the existing table
                logger.info(f"Fetching data from existing {base_name} crypto alerts table")
                cur.execute(f"""
                    SELECT 
                        datetime, 
                        stock, 
                        crypto_name, 
                        oversold_alert, 
                        anchored_obv_alert_state
                    FROM {backup_table}
                """)
                old_data = cur.fetchall()
                logger.info(f"Fetched {len(old_data)} rows from the old {base_name} table")
                
                # Step 3: Create the new table structure (if it doesn't match already)
                logger.info(f"Checking current {base_name} crypto alerts table structure")
                cur.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{alerts_table}'
                    ORDER BY column_name
                """)
                columns = [col[0] for col in cur.fetchall()]
                
                # Check if the table has been updated already
                if 'alerts' not in columns:
                    logger.info(f"Recreating {base_name} crypto alerts table with new structure")
                    # Drop the existing table and create the new one
                    cur.execute(f"DROP TABLE IF EXISTS {alerts_table}")
                    
                    # Create the new table with the correct primary key constraint name
                    if suffix == "":
                        constraint_name = "crypto_alerts_pkey"
                    else:
                        constraint_name = f"crypto_alerts{suffix}_pkey"
                        
                    cur.execute(f"""
                        CREATE TABLE {alerts_table} (
                            datetime TIMESTAMPTZ NOT NULL,
                            stock TEXT NOT NULL,
                            crypto_name TEXT,
                            alerts JSONB NOT NULL,
                            CONSTRAINT {constraint_name} PRIMARY KEY (datetime, stock)
                        )
                    """)
                    
                    # Create necessary indexes
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{alerts_table}_stock 
                        ON {alerts_table} (stock)
                    """)
                    
                    cur.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{alerts_table}_datetime 
                        ON {alerts_table} (datetime)
                    """)
                    
                    logger.info(f"New {base_name} crypto alerts table structure created successfully")
                else:
                    logger.info(f"{base_name} crypto alerts table structure already updated, will append data")
                
                # Step 4: Transform and migrate the data
                migrated_count = 0
                batch_size = 100
                total_batches = (len(old_data) + batch_size - 1) // batch_size
                
                for batch_num in range(total_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min((batch_num + 1) * batch_size, len(old_data))
                    batch = old_data[start_idx:end_idx]
                    
                    # Transform data to the new structure
                    new_data = []
                    for row in batch:
                        datetime_val, stock, crypto_name, oversold_alert, obv_alert = row
                        
                        alerts_array = []
                        
                        # Add oversold alert if present
                        if oversold_alert == '$$$':
                            alerts_array.append({
                                "type": "oversold",
                                "value": oversold_alert,
                                "description": "Williams %R and Force Index Oversold Alert"
                            })
                        
                        # Add OBV alert if present
                        if obv_alert == '$$$':
                            alerts_array.append({
                                "type": "obv_positive",
                                "value": obv_alert,
                                "description": "OBV Positive Crossover"
                            })
                        elif obv_alert == '-$$$':
                            alerts_array.append({
                                "type": "obv_negative",
                                "value": obv_alert,
                                "description": "OBV Negative Crossover"
                            })
                        
                        # Only include entries that have at least one alert
                        if alerts_array:
                            new_data.append((
                                datetime_val,
                                stock,
                                crypto_name,
                                json.dumps(alerts_array)
                            ))
                    
                    if new_data:
                        # Step 5: Insert data into the new table
                        execute_values(cur, f"""
                            INSERT INTO {alerts_table} (
                                datetime, stock, crypto_name, alerts
                            ) VALUES %s
                            ON CONFLICT (datetime, stock) DO UPDATE SET
                                crypto_name = EXCLUDED.crypto_name,
                                alerts = EXCLUDED.alerts
                        """, new_data)
                        
                        migrated_count += len(new_data)
                        logger.info(f"Migrated {base_name} batch {batch_num+1}/{total_batches} ({len(new_data)} rows)")
                
                conn.commit()
                
                # Step 6: Verify migration
                cur.execute(f"SELECT COUNT(*) FROM {alerts_table}")
                new_count = cur.fetchone()[0]
                
                logger.info(f"{base_name} migration complete: {migrated_count} rows migrated")
                logger.info(f"Total rows in new {base_name} table: {new_count}")
                
                return {
                    "success": True,
                    "base": base_name,
                    "old_row_count": len(old_data),
                    "migrated_row_count": migrated_count,
                    "new_table_row_count": new_count
                }
                
    except Exception as e:
        logger.error(f"Error during {base_name} migration: {str(e)}")
        return {
            "success": False,
            "base": base_name,
            "error": str(e)
        }

def display_alert_samples(base_currency="all"):
    """
    Display sample data from the new table structure for verification
    
    Parameters:
    - base_currency: 'usd', 'eth', 'btc', or 'all' to specify which base currency's alerts to display
    """
    bases_to_process = []
    if base_currency == "all" or base_currency == "usd":
        bases_to_process.append(("", "USD"))
    if base_currency == "all" or base_currency == "eth":
        bases_to_process.append(("_eth", "ETH"))
    if base_currency == "all" or base_currency == "btc":
        bases_to_process.append(("_btc", "BTC"))
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for suffix, base_name in bases_to_process:
                    alerts_table = f"crypto_alerts_table{suffix}"
                    
                    logger.info(f"Fetching sample data from the new {base_name} table structure")
                    
                    cur.execute(f"""
                        SELECT datetime, stock, crypto_name, alerts
                        FROM {alerts_table}
                        LIMIT 5
                    """)
                    
                    sample_data = cur.fetchall()
                    
                    if not sample_data:
                        logger.info(f"No data found in the new {base_name} table")
                        continue
                    
                    logger.info(f"Sample data from the new {base_name} table structure:")
                    for row in sample_data:
                        alerts_formatted = json.dumps(row['alerts'], indent=2)
                        logger.info(f"Date: {row['datetime']}, Symbol: {row['stock']}")
                        logger.info(f"Crypto Name: {row['crypto_name']}")
                        logger.info(f"Alerts: {alerts_formatted}")
                        logger.info("-" * 40)
    
    except Exception as e:
        logger.error(f"Error fetching sample data: {str(e)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate crypto alerts data to new structure')
    parser.add_argument(
        '--base', 
        choices=['usd', 'eth', 'btc', 'all'], 
        default='all',
        help='Base currency to migrate: USD (usd), ETH (eth), BTC (btc), or all base currencies (all)'
    )
    args = parser.parse_args()
    
    logger.info(f"Starting migration for base currency(s): {args.base}")
    results = migrate_crypto_alerts_data(args.base)
    
    all_success = True
    for base, result in results.items():
        if not result["success"]:
            all_success = False
            logger.error(f"Migration failed for {result['base']}: {result.get('error')}")
        else:
            logger.info(f"Migration successful for {result['base']}")
    
    if all_success:
        logger.info("All migrations completed successfully")
        display_alert_samples(args.base)
    else:
        logger.error("One or more migrations failed")