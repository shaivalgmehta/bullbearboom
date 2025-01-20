import os
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

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

def calculate_erp5_rankings(date):
    """Calculate ERP5 rankings for all stocks on a given date"""
    print(f"Calculating ERP5 rankings for date: {date}")
    
    with get_db_connection() as conn:
        # Use RealDictCursor for named columns
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all stocks with their component rankings
            query = """
                WITH latest_quarterly AS (
                    SELECT DISTINCT ON (stock)
                        stock,
                        return_on_equity_rank,
                        ev_ebitda_rank,
                        datetime as quarterly_date
                    FROM in_quarterly_table
                    WHERE datetime <= %s 
                    AND datetime >= %s - INTERVAL '6 months'
                    ORDER BY stock, datetime DESC
                )
                SELECT 
                    d.stock,
                    d.earnings_yield_rank,
                    d.pb_ratio_rank,
                    d.book_to_price_rank,
                    q.return_on_equity_rank,
                    q.ev_ebitda_rank,
                    q.quarterly_date
                FROM in_daily_table d
                LEFT JOIN latest_quarterly q ON d.stock = q.stock
                WHERE DATE(d.datetime) = DATE(%s)
                AND d.earnings_yield_rank IS NOT NULL
                AND d.pb_ratio_rank IS NOT NULL
                AND d.book_to_price_rank IS NOT NULL
                AND q.return_on_equity_rank IS NOT NULL
                AND q.ev_ebitda_rank IS NOT NULL
            """
            
            print("Executing query...")
            cur.execute(query, (date, date, date))
            print("Query executed, fetching results...")
            
            stocks_data = cur.fetchall()
            
            if not stocks_data:
                print(f"No stocks with complete ranking data found for {date}")
                return
                
            print(f"Found {len(stocks_data)} stocks with complete ranking data")

            # Calculate combined ERP5 ranks
            erp5_scores = []
            for row in stocks_data:
                try:
                    stock = row['stock']
                    # Sum all component ranks (lower is better)
                    combined_rank = sum([
                        row['earnings_yield_rank'],
                        row['pb_ratio_rank'],
                        row['book_to_price_rank'],
                        row['return_on_equity_rank'],
                        row['ev_ebitda_rank']  # Added EV/EBITDA rank
                    ])
                    erp5_scores.append((stock, combined_rank))
                except (KeyError, TypeError) as e:
                    print(f"Error processing stock {row.get('stock', 'unknown')}: {e}")
                    print(f"Row data: {row}")
                    continue

            if not erp5_scores:
                print("No valid ERP5 scores calculated")
                return

            # Sort by combined rank (lower is better)
            erp5_scores.sort(key=lambda x: x[1])
            
            print(f"Calculated scores for {len(erp5_scores)} stocks")
            
            # Calculate percentile ranks (1 is best, 100 is worst)
            total_stocks = len(erp5_scores)
            erp5_percentiles = []
            
            for i, (stock, _) in enumerate(erp5_scores):
                percentile = round((i / (total_stocks - 1)) * 99) + 1  # 1 to 100
                erp5_percentiles.append((
                    date,
                    stock,
                    percentile,
                    datetime.now(pytz.UTC)
                ))
            
            print(f"Inserting {len(erp5_percentiles)} ERP5 rankings into database...")
            
            # Store ERP5 rankings
            execute_values(cur, """
                INSERT INTO in_daily_table (
                    datetime,
                    stock,
                    erp5_rank,
                    last_modified_date
                ) VALUES %s
                ON CONFLICT (datetime, stock) 
                DO UPDATE SET 
                    erp5_rank = EXCLUDED.erp5_rank,
                    last_modified_date = EXCLUDED.last_modified_date
            """, erp5_percentiles)
            
            conn.commit()
            print(f"Successfully calculated and stored ERP5 rankings for {len(erp5_percentiles)} stocks")

def main():
    # Calculate ERP5 rankings for yesterday
    yesterday = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    calculate_erp5_rankings(yesterday)

if __name__ == "__main__":
    main()