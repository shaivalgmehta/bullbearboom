#!/usr/bin/env python3

import os
import psycopg2
from dotenv import load_dotenv
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
    """Establish database connection"""
    return psycopg2.connect(**db_params)

def setup_user_tables():
    """Create user authentication and watch list tables"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Starting user tables setup")
                
                # Check if tables already exist
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'users'
                    )
                """)
                users_exist = cur.fetchone()[0]
                
                if not users_exist:
                    # Create users table
                    logger.info("Creating users table")
                    cur.execute("""
                        CREATE TABLE users (
                            id SERIAL PRIMARY KEY,
                            email TEXT UNIQUE NOT NULL,
                            password_hash TEXT NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT NOW()
                        )
                    """)
                    
                    # Create index on email for efficient lookups
                    cur.execute("""
                        CREATE INDEX idx_users_email ON users(email)
                    """)
                    
                    logger.info("Users table created successfully")
                else:
                    logger.info("Users table already exists")
                
                # Check if watch lists table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'watch_lists'
                    )
                """)
                watch_lists_exist = cur.fetchone()[0]
                
                if not watch_lists_exist:
                    # Create watch lists table
                    logger.info("Creating watch_lists table")
                    cur.execute("""
                        CREATE TABLE watch_lists (
                            id SERIAL PRIMARY KEY,
                            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                            entity_type TEXT NOT NULL,
                            symbol TEXT NOT NULL,
                            added_at TIMESTAMPTZ DEFAULT NOW(),
                            UNIQUE(user_id, entity_type, symbol)
                        )
                    """)
                    
                    # Create index for efficient filtering
                    cur.execute("""
                        CREATE INDEX idx_watch_lists_user_id ON watch_lists(user_id)
                    """)
                    
                    logger.info("Watch lists table created successfully")
                else:
                    logger.info("Watch lists table already exists")
                
                conn.commit()
                
                return True
                
    except Exception as e:
        logger.error(f"Error creating user tables: {str(e)}")
        return False

if __name__ == "__main__":
    success = setup_user_tables()
    
    if success:
        logger.info("User tables setup completed successfully")
    else:
        logger.error("Failed to set up user tables")