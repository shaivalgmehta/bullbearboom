#!/usr/bin/env python3

import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
import pytz
from psycopg2.extras import execute_values



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

class MetricRanker:
    def __init__(self, db_params: Dict[str, Any]):
        self.db_params = db_params
        # Define metrics that should exclude negative values
        self.exclude_negatives = ['pe_ratio', 'ev_ebitda', 'pb_ratio', 'peg_ratio', 'earnings_yield', 'book_to_price']
        
        # Define metrics that should exclude near-zero values
        self.exclude_near_zero = ['pe_ratio', 'ev_ebitda', 'pb_ratio', 'peg_ratio', 'earnings_yield', 'book_to_price']
        
        # Define ranking direction for each metric
        # True means higher value is better (rank 1)
        # False means lower value is better (rank 1)
        self.metric_directions = {
            # Daily metrics
            'pe_ratio': False,  # Lower P/E is better (more earnings per price)
            'ev_ebitda': False,  # Lower EV/EBITDA is better (cheaper valuation)
            'pb_ratio': False,  # Lower P/B is better (cheaper relative to book value)
            'peg_ratio': False,  # Lower PEG is better (growth adjusted P/E)
            'earnings_yield': True,  # Higher earnings yield is better
            'book_to_price': True,   # Higher book to price is better            
        }

    def get_previous_date(self, current_date: datetime, frequency: str) -> datetime:
        """Calculate the appropriate previous date based on frequency."""
        if frequency == 'daily':
            return current_date - timedelta(days=1)
        elif frequency == 'weekly':
            # Get previous Monday
            days_since_monday = current_date.weekday()
            return (current_date - timedelta(days=days_since_monday)).replace(
                hour=0, minute=0, second=0, microsecond=0)
        return current_date


    def rank_metric(self, df: pd.DataFrame, metric: str) -> pd.DataFrame:
        """Rank a single metric with appropriate handling of unrealistic values."""
        # Create a copy of the dataframe with only non-null values
        ranking_df = df[['stock', metric]].dropna().copy()
        
        # Define minimum thresholds for each metric
        min_thresholds = {
            'pe_ratio': 0.5,     # Absolute minimum P/E
            'ev_ebitda': 1.0,    # Absolute minimum EV/EBITDA
            'pb_ratio': 0.2,     # Absolute minimum P/B
            'peg_ratio': 0.2,     # Absolute minimum PEG
            'earnings_yield': 0.005,     # Absolute minimum Earnings Yield
            'book_to_price': 0.05     # Absolute minimum B/P
        }
        
        # Handle unrealistic values for specified metrics
        if metric in self.exclude_near_zero:
            min_value = min_thresholds.get(metric, 0.01)  # Default to 0.01 if not specified
            ranking_df = ranking_df[ranking_df[metric] >= min_value]
        
        # Handle metrics that should exclude negative values
        if metric in self.exclude_negatives:
            ranking_df = ranking_df[ranking_df[metric] > 0]
        
        # Determine ranking direction
        ascending = not self.metric_directions[metric]
        
        # Add rank
        if not ranking_df.empty:
            ranking_df['rank'] = ranking_df[metric].rank(
                method='min',
                ascending=ascending,
                na_option='keep'
            )
        
        return ranking_df[['stock', 'rank']]

    def rank_metrics(self, conn, date: datetime) -> Dict[str, pd.DataFrame]:
        """Rank all metrics for the given date."""
        daily_metrics = ['pe_ratio', 'ev_ebitda', 'pb_ratio', 'peg_ratio']
        
        # Get daily metrics
        daily_date = self.get_previous_date(date, 'daily')
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT stock, {', '.join(daily_metrics)}
                FROM us_daily_table
                WHERE datetime = %s
            """, (daily_date,))
            daily_df = pd.DataFrame(cur.fetchall(), 
                                  columns=['stock'] + daily_metrics)

        # Rank each metric
        rankings = {}
        
        # Rank daily metrics
        for metric in daily_metrics:
            if metric in self.metric_directions:
                rankings[f"{metric}_rank"] = self.rank_metric(daily_df, metric)

        return rankings

    def update_rankings(self, conn, rankings: Dict[str, pd.DataFrame], date: datetime):
        """Update all rankings in a single bulk operation."""
        daily_date = self.get_previous_date(date, 'daily')
        daily_metrics = ['pe_ratio', 'ev_ebitda', 'pb_ratio', 'peg_ratio']
        
        with conn.cursor() as cur:
            # First, set all ranks to NULL for this date
            cur.execute(f"""
                UPDATE us_daily_table
                SET 
                    pe_ratio_rank = NULL,
                    ev_ebitda_rank = NULL,
                    pb_ratio_rank = NULL,
                    peg_ratio_rank = NULL,
                    earnings_yield_rank = NULL,
                    book_to_price_rank = NULL
                WHERE datetime = %s
            """, (daily_date,))
            
            # Prepare combined data for all metrics
            all_updates = []
            for metric in daily_metrics:
                rank_df = rankings.get(f"{metric}_rank")
                if rank_df is not None and not rank_df.empty:
                    for _, row in rank_df.iterrows():
                        all_updates.append((
                            row['stock'],
                            daily_date,
                            metric,
                            int(row['rank'])
                        ))
            
            if all_updates:
                # Single bulk update for all metrics
                execute_values(
                    cur,
                    """
                    UPDATE us_daily_table AS t
                    SET 
                        pe_ratio_rank = CASE WHEN c.metric = 'pe_ratio' THEN c.rank ELSE t.pe_ratio_rank END,
                        ev_ebitda_rank = CASE WHEN c.metric = 'ev_ebitda' THEN c.rank ELSE t.ev_ebitda_rank END,
                        pb_ratio_rank = CASE WHEN c.metric = 'pb_ratio' THEN c.rank ELSE t.pb_ratio_rank END,
                        peg_ratio_rank = CASE WHEN c.metric = 'peg_ratio' THEN c.rank ELSE t.peg_ratio_rank END
                        earnings_yield_rank = CASE WHEN c.metric = 'earnings_yield' THEN c.rank ELSE t.earnings_yield_rank END,
                        book_to_price_rank = CASE WHEN c.metric = 'book_to_price' THEN c.rank ELSE t.book_to_price_rank END
                    FROM (VALUES %s) AS c(stock, datetime, metric, rank)
                    WHERE t.stock = c.stock AND t.datetime = c.datetime
                    """,
                    all_updates,
                    template="(%s, %s, %s, %s)"
                )
        
        conn.commit()

def main():

    
    ranker = MetricRanker(db_params)
    current_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        with psycopg2.connect(**db_params) as conn:
            rankings = ranker.rank_metrics(conn, current_date)
            ranker.update_rankings(conn, rankings, current_date)
            print("Rankings updated successfully")
    except Exception as e:
        print(f"Error updating rankings: {str(e)}")

if __name__ == "__main__":
    main()