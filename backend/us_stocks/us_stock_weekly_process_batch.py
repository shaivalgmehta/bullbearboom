#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import multiprocessing as mp
from functools import partial
import pandas as pd
import numpy as np
import pytz
from dotenv import load_dotenv
import logging
import requests
from itertools import groupby
from operator import itemgetter
from abc import ABC, abstractmethod

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

@dataclass
class TransformerResult:
    datetime: datetime
    calculations: Dict[str, Any]
    valid: bool = True
    error: Optional[str] = None

@dataclass
class ProcessingResult:
    stock: str
    date: datetime
    williams_r: Optional[TransformerResult] = None
    force_index: Optional[TransformerResult] = None
    error: Optional[str] = None

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data: Dict[str, Any], symbol: str) -> TransformerResult:
        pass

    def _calculate_pine_script_ema(self, data: np.ndarray, length: int) -> float:
        """Calculate EMA using Pine Script's method with numpy for performance"""
        alpha = 2 / (length + 1)
        ema = np.empty_like(data)
        ema[0] = data[0]
        for i in range(1, len(data)):
            ema[i] = data[i] * alpha + ema[i-1] * (1 - alpha)
        return ema[-1]

class WilliamsRTransformer(BaseTransformer):
    def __init__(self, ema_length: int = 21):
        self.ema_length = ema_length
        self.min_periods = max(52, ema_length)

    def transform(self, data: Dict[str, Any], symbol: str) -> TransformerResult:
        try:
            weekly_data = data['price_data']
            previous_alert = data['previous_alerts']['williams_r_momentum_alert_state']
            
            dates = np.array([d['datetime'] for d in weekly_data])
            highs = np.array([float(d['high']) for d in weekly_data])
            lows = np.array([float(d['low']) for d in weekly_data])
            closes = np.array([float(d['close']) for d in weekly_data])

            if len(dates) < self.min_periods:
                return TransformerResult(
                    datetime=dates[0],
                    calculations={},
                    valid=False,
                    error=f"Insufficient data: {len(dates)} periods, need {self.min_periods}"
                )

            highest_high = np.maximum.accumulate(highs[::-1])[::-1]
            lowest_low = np.minimum.accumulate(lows[::-1])[::-1]
            
            denominator = highest_high - lowest_low
            denominator = np.where(denominator == 0, np.nan, denominator)
            
            williams_r = np.where(
                np.isnan(denominator),
                0,
                ((highest_high - closes) / denominator) * -100
            )

            valid_williams_r = williams_r[~np.isnan(williams_r)]
            if len(valid_williams_r) >= self.ema_length:
                williams_r_ema = self._calculate_pine_script_ema(valid_williams_r, self.ema_length)
            else:
                return TransformerResult(
                    datetime=dates[0],
                    calculations={},
                    valid=False,
                    error="Insufficient valid data for EMA calculation"
                )

            latest_williams_r = williams_r[0]
            latest_ema = williams_r_ema

            # Original alert state logic
            meets_criteria = latest_williams_r > latest_ema and latest_williams_r < -50
            
            if previous_alert is None:
                alert_state = '$$$' if meets_criteria else '-'
            elif previous_alert == '$$$':
                alert_state = '$' if meets_criteria else '-'
            elif previous_alert == '$':
                alert_state = '$' if meets_criteria else '-'
            else:  # previous_alert == '-'
                alert_state = '$$$' if meets_criteria else '-'

            return TransformerResult(
                datetime=dates[0],
                calculations={
                    'williams_r': float(latest_williams_r),
                    'williams_r_ema': float(latest_ema),
                    'williams_r_momentum_alert_state': alert_state
                }
            )

        except Exception as e:
            return TransformerResult(
                datetime=weekly_data[0]['datetime'] if weekly_data else None,
                calculations={},
                valid=False,
                error=str(e)
            )

class ForceIndexTransformer(BaseTransformer):
    def __init__(self, fast_period: int = 7, slow_period: int = 52):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.min_periods = slow_period + 1

    def transform(self, data: Dict[str, Any], symbol: str) -> TransformerResult:
        try:
            weekly_data = data['price_data']
            previous_alert = data['previous_alerts']['force_index_alert_state']
            
            dates = np.array([d['datetime'] for d in weekly_data])
            closes = np.array([float(d['close']) for d in weekly_data])
            volumes = np.array([float(d['volume']) for d in weekly_data])

            if len(dates) < self.min_periods:
                return TransformerResult(
                    datetime=dates[0],
                    calculations={},
                    valid=False,
                    error=f"Insufficient data: {len(dates)} periods, need {self.min_periods}"
                )

            price_changes = np.diff(closes[::-1])[::-1]
            price_changes = np.pad(price_changes, (1, 0), 'constant')
            force_index = price_changes * volumes

            force_index_7_week = self._calculate_pine_script_ema(force_index, self.fast_period)
            force_index_52_week = self._calculate_pine_script_ema(force_index, self.slow_period)
            last_week_force_index_7_week = self._calculate_pine_script_ema(force_index[1:], self.fast_period)
            last_week_force_index_52_week = self._calculate_pine_script_ema(force_index[1:], self.slow_period)

            # Original alert state logic
            upward_crossover = (last_week_force_index_7_week <= last_week_force_index_52_week) and \
                             (force_index_7_week > force_index_52_week)
            downward_crossover = (last_week_force_index_7_week >= last_week_force_index_52_week) and \
                               (force_index_7_week < force_index_52_week)

            if upward_crossover or downward_crossover:
                if previous_alert in ['$$$', '$']:
                    alert_state = '$'
                else:
                    alert_state = '$$$'
            else:
                alert_state = '-'

            return TransformerResult(
                datetime=dates[0],
                calculations={
                    'force_index_7_week': float(force_index_7_week),
                    'force_index_52_week': float(force_index_52_week),
                    'last_week_force_index_7_week': float(last_week_force_index_7_week),
                    'last_week_force_index_52_week': float(last_week_force_index_52_week),
                    'force_index_alert_state': alert_state
                }
            )

        except Exception as e:
            return TransformerResult(
                datetime=weekly_data[0]['datetime'] if weekly_data else None,
                calculations={},
                valid=False,
                error=str(e)
            )

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_batch_data(symbols: List[str], end_date: datetime) -> Dict[str, Dict[str, Any]]:
    """Fetch both weekly price data and previous alert states for a batch of symbols"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First, fetch the weekly price data
            cur.execute("""
                WITH RECURSIVE weeks AS (
                    SELECT %s::timestamp as week_end
                    UNION ALL
                    SELECT (week_end - interval '7 days')::timestamp
                    FROM weeks
                    WHERE week_end - interval '7 days' >= %s::timestamp - interval '80 weeks'
                ),
                weekly_data AS (
                    SELECT 
                        t.stock,
                        w.week_end as datetime,
                        MAX(t.high) as high,
                        MIN(t.low) as low,
                        (array_agg(t.close ORDER BY t.datetime DESC))[1] as close,
                        SUM(t.volume) as volume
                    FROM us_daily_table t
                    CROSS JOIN weeks w
                    WHERE t.stock = ANY(%s)
                    AND t.datetime <= w.week_end
                    AND t.datetime > w.week_end - interval '7 days'
                    GROUP BY t.stock, w.week_end
                    ORDER BY t.stock, w.week_end DESC
                )
                SELECT * FROM weekly_data
            """, (end_date, end_date, symbols))
            
            price_rows = cur.fetchall()
            
            # Then, fetch the previous alert states for each symbol
            cur.execute("""
                SELECT 
                    t.stock,
                    t.williams_r_momentum_alert_state,
                    t.force_index_alert_state,
                    t.datetime
                FROM us_weekly_table t
                INNER JOIN (
                    SELECT stock, MAX(datetime) as last_date
                    FROM us_weekly_table
                    WHERE stock = ANY(%s)
                    AND datetime < %s
                    GROUP BY stock
                ) last_dates
                ON t.stock = last_dates.stock
                AND t.datetime = last_dates.last_date
            """, (symbols, end_date))
            
            alert_rows = cur.fetchall()
            
            # Organize results
            result = {}
            
            # Group price data by symbol
            for stock, group in groupby(price_rows, key=itemgetter(0)):
                weekly_prices = [
                    {
                        'datetime': row[1],
                        'high': row[2],
                        'low': row[3],
                        'close': row[4],
                        'volume': row[5]
                    }
                    for row in group
                ]
                
                result[stock] = {
                    'price_data': weekly_prices,
                    'previous_alerts': {
                        'williams_r_momentum_alert_state': None,
                        'force_index_alert_state': None,
                        'datetime': None
                    }
                }
            
            # Add alert states
            for row in alert_rows:
                symbol = row[0]
                if symbol in result:
                    result[symbol]['previous_alerts'] = {
                        'williams_r_momentum_alert_state': row[1],
                        'force_index_alert_state': row[2],
                        'datetime': row[3]
                    }
            
            return result

def store_batch_results(results: List[ProcessingResult]):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            williams_r_values = []
            force_index_values = []
            current_time = datetime.now(pytz.UTC)

            for result in results:
                if result.error:
                    logger.warning(f"Skipping {result.stock} due to error: {result.error}")
                    continue

                if result.williams_r and result.williams_r.valid:
                    williams_r_values.append((
                        result.date,
                        result.stock,
                        result.williams_r.calculations['williams_r'],
                        result.williams_r.calculations['williams_r_ema'],
                        result.williams_r.calculations['williams_r_momentum_alert_state'],
                        current_time
                    ))

                if result.force_index and result.force_index.valid:
                    force_index_values.append((
                        result.date,
                        result.stock,
                        result.force_index.calculations['force_index_7_week'],
                        result.force_index.calculations['force_index_52_week'],
                        result.force_index.calculations['last_week_force_index_7_week'],
                        result.force_index.calculations['last_week_force_index_52_week'],
                        result.force_index.calculations['force_index_alert_state'],
                        current_time
                    ))

            if williams_r_values:
                execute_values(cur, """
                    INSERT INTO us_weekly_table (
                        datetime, stock, williams_r, williams_r_ema,
                        williams_r_momentum_alert_state, last_modified_date
                    ) VALUES %s
                    ON CONFLICT (datetime, stock) 
                    DO UPDATE SET
                        williams_r = EXCLUDED.williams_r,
                        williams_r_ema = EXCLUDED.williams_r_ema,
                        williams_r_momentum_alert_state = EXCLUDED.williams_r_momentum_alert_state,
                        last_modified_date = EXCLUDED.last_modified_date
                """, williams_r_values)

            if force_index_values:
                execute_values(cur, """
                    INSERT INTO us_weekly_table (
                        datetime, stock, force_index_7_week, force_index_52_week,
                        last_week_force_index_7_week, last_week_force_index_52_week,
                        force_index_alert_state, last_modified_date
                    ) VALUES %s
                    ON CONFLICT (datetime, stock) 
                    DO UPDATE SET
                        force_index_7_week = EXCLUDED.force_index_7_week,
                        force_index_52_week = EXCLUDED.force_index_52_week,
                        last_week_force_index_7_week = EXCLUDED.last_week_force_index_7_week,
                        last_week_force_index_52_week = EXCLUDED.last_week_force_index_52_week,
                        force_index_alert_state = EXCLUDED.force_index_alert_state,
                        last_modified_date = EXCLUDED.last_modified_date
                """, force_index_values)

            conn.commit()

def process_batch(stocks: List[Dict[str, Any]], end_date: datetime) -> List[ProcessingResult]:
    try:
        symbols = [stock['symbol'] for stock in stocks]
        batch_data = fetch_batch_data(symbols, end_date)
        
        williams_r_transformer = WilliamsRTransformer()
        force_index_transformer = ForceIndexTransformer()
        
        results = []
        for stock in stocks:
            symbol = stock['symbol']
            stock_data = batch_data.get(symbol)
            
            if not stock_data or not stock_data['price_data']:
                results.append(ProcessingResult(
                    stock=symbol,
                    date=end_date,
                    error="No data available"
                ))
                continue
            
            try:
                williams_r_result = williams_r_transformer.transform(stock_data, symbol)
                force_index_result = force_index_transformer.transform(stock_data, symbol)
                
                results.append(ProcessingResult(
                    stock=symbol,
                    date=end_date,
                    williams_r=williams_r_result,
                    force_index=force_index_result
                ))
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                results.append(ProcessingResult(
                    stock=symbol,
                    date=end_date,
                    error=f"Processing error: {str(e)}"
                ))
        
        return results
        
    except Exception as e:
        logger.error(f"Batch processing error: {e}")
        return [ProcessingResult(
            stock=stock['symbol'],
            date=end_date,
            error=f"Batch error: {str(e)}"
        ) for stock in stocks]

def fetch_stock_list_twelve_data():
    TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY')
    url = f"https://api.twelvedata.com/stocks?country=United States&type=Common Stock&exchange=NASDAQ&apikey={TWELVE_DATA_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error fetching stock list: {response.status_code}")

def main():
    try:
        logger.info("Fetching stock list...")
        stocks = fetch_stock_list_twelve_data()
        
        current = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        days_since_sunday = (current.weekday() + 1) % 7
        end_date = current - timedelta(days=days_since_sunday)
        dates_to_process = [end_date - timedelta(weeks=i) for i in range(10)]
        dates_to_process.reverse()

        # Configure parallel processing
        num_processes = min(mp.cpu_count(), 8)  # Limit to 8 processes
        batch_size = max(100, len(stocks) // (num_processes * 2))
        logger.info(f"Using {num_processes} processes with batch size {batch_size}")

        for process_date in dates_to_process:
            logger.info(f"Processing data for week ending {process_date.date()}")
            
            # Split stocks into batches
            stock_batches = [stocks[i:i + batch_size] for i in range(0, len(stocks), batch_size)]
            
            total_processed = 0
            successful = 0
            failed = 0
            
            # Process batches in parallel
            with ProcessPoolExecutor(max_workers=num_processes) as executor:
                future_to_batch = {
                    executor.submit(process_batch, batch, process_date): batch 
                    for batch in stock_batches
                }
                
                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    try:
                        results = future.result()
                        total_processed += len(results)
                        
                        # Count successes and failures
                        successful += sum(1 for r in results if not r.error)
                        failed += sum(1 for r in results if r.error)
                        
                        # Store results
                        store_batch_results(results)
                        
                        logger.info(
                            f"Batch completed - "
                            f"Processed: {len(results)}, "
                            f"Success: {successful}, "
                            f"Failed: {failed}"
                        )
                        
                    except Exception as e:
                        logger.error(f"Batch processing failed: {e}")
                        failed += len(batch)
            
            logger.info(
                f"Completed processing for {process_date.date()} - "
                f"Total: {total_processed}, "
                f"Successful: {successful}, "
                f"Failed: {failed}"
            )

    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise

if __name__ == "__main__":
    main()