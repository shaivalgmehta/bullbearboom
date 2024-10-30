import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from crypto_data_transformer_new import get_transformer
import json
import time
import pandas as pd
from typing import Dict, Any, List
import pytz


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

# Polygon.io API key
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

######################### DEFINE FUNCTIONS TO FETCH DATA FROM POLYGON.IO #############################
def get_table_name(base):
    return f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"


def fetch_stock_list_polygon():
    url = f"https://api.polygon.io/v3/reference/tickers?market=crypto&active=true&apiKey={POLYGON_API_KEY}"
    all_tickers = []
    
    while url:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            all_tickers.extend(data['results'])
            url = data.get('next_url')
            if url:
                url += f"&apiKey={POLYGON_API_KEY}"
        else:
            raise Exception(f"Error fetching stock list: {response.status_code}")
    
    return [{'symbol': ticker['ticker'], 'name': ticker['name'], 'crypto_name': ticker['base_currency_name']} for ticker in all_tickers]


def fetch_daily_data_polygon(symbol, start_date, end_date):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
    
    all_results = []
    while url:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            all_results.extend(data.get('results', []))
            url = data.get('next_url')
            if url:
                url += f"&apiKey={POLYGON_API_KEY}"
        else:
            print(f"Error fetching data for {symbol}: {response.status_code}")
            return None
    
    if all_results:
        df = pd.DataFrame(all_results)
        df['t'] = pd.to_datetime(df['t'], unit='ms')
        df = df.sort_values('t')
        return df[['t', 'o', 'c', 'v', 'h','l']].to_dict('records')
    return None


def fetch_technical_indicators_polygon(symbol: str, db_params: Dict[str, Any], POLYGON_API_KEY: str, store_stock_daily_data, end_date: datetime, base: str = 'USD') -> Dict[str, Any]:
    end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=230)  # We need 201 days of data to calculate 200-day EMA

    table_name = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"

    # Fetch data from TimescaleDB
    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT datetime, stock_name, crypto_name, open, close, volume, high, low
                FROM {table_name}
                WHERE stock = %s AND datetime BETWEEN %s AND %s
                ORDER BY datetime
            """, (symbol, start_date, end_date))
            db_data = cur.fetchall()
    finally:
        conn.close()
    df = pd.DataFrame(db_data, columns=['datetime', 'stock_name', 'crypto_name', 'open', 'close', 'volume', 'high', 'low'])
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    df.set_index('datetime', inplace=True)

    # Convert numeric columns to float, replacing non-numeric values with NaN
    numeric_columns = ['open', 'close', 'volume', 'high', 'low']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # For non-USD bases, skip missing data check
    if base.lower() != 'usd':
        df = df.dropna()
    else:
        # Identify missing dates
        date_range = pd.date_range(start=start_date, end=end_date, freq='D', tz=pytz.UTC)
        missing_dates = date_range.difference(df.index)

        # Fetch missing data from Polygon if needed
        if len(missing_dates) > 0:
            polygon_data = fetch_missing_data_from_polygon(symbol, missing_dates, POLYGON_API_KEY)

            if polygon_data is not None:
                # Store the newly fetched data
                stock_name = df['stock_name'].iloc[0] if not df.empty else None
                crypto_name = df['crypto_name'].iloc[0] if not df.empty else None
                store_missing_data(polygon_data, symbol, stock_name, crypto_name, store_stock_daily_data)

                # Add the new data to our DataFrame
                df = pd.concat([df, polygon_data])
                df.sort_index(inplace=True)

        # Remove any rows with NaN values
        df = df.dropna()

    # If we don't have enough data, log an error
    if len(df) < 200:
        print(f"Error: Not enough valid data available for {symbol} to calculate EMA. Only {len(df)} days available.")
        return None

    # Get the latest date in the data
    latest_date = df.index[-1]
    
    # Check if the latest date matches the end_date
    calculate_ema_flag = latest_date.date() == end_date.date()
    
    latest_data = df.loc[latest_date]
    try:
        result = {
            'datetime': latest_date.strftime('%Y-%m-%d'),
            'open': float(latest_data['open']),
            'close': float(latest_data['close']),
            'volume': float(latest_data['volume']),
            'high': float(latest_data['high']),
            'low': float(latest_data['low'])
        }
        
        # Only calculate EMA if the latest date matches the end_date
        if calculate_ema_flag:
            try:
                ema = calculate_ema(df['close'], 200)
                result['ema'] = float(ema)
            except Exception as e:
                print(f"Error calculating EMA for {symbol}: {str(e)}")
                result['ema'] = None
        else:
            result['ema'] = None
            print(f"Warning: Latest data for {symbol} is not up to date. EMA not calculated.")
        
        return result
    except Exception as e:
        print(f"Error converting data to float for {symbol}: {str(e)}")
        return None


def fetch_missing_data_from_polygon(symbol: str, dates: pd.DatetimeIndex, POLYGON_API_KEY: str) -> pd.DataFrame:
    start_date = dates.min().strftime('%Y-%m-%d')
    end_date = dates.max().strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        
        if 'results' in data and data['results']:
            df = pd.DataFrame(data['results'])
            df['datetime'] = pd.to_datetime(df['t'], unit='ms', utc=True)
            df.set_index('datetime', inplace=True)
            df = df.rename(columns={'o': 'open', 'c': 'close', 'v': 'volume', 'h': 'high', 'l': 'low'})
            # Convert to numeric, replacing any non-numeric values with NaN
            for col in ['open', 'close', 'volume', 'high', 'low']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Adjust volume by multiplying with close price
            df['volume'] = df['volume'] * df['close']
            return df[['open', 'close', 'volume', 'high', 'low']]
        else:
            print(f"No results found for {symbol} between {start_date} and {end_date}")
            return None
    else:
        print(f"Error fetching data: HTTP {response.status_code}")
        return None

def store_missing_data(df: pd.DataFrame, symbol: str, stock_name: str, crypto_name: str, store_stock_daily_data):
    data_to_store = []
    for date, row in df.iterrows():
        data_point = {
            'datetime': date.strftime('%Y-%m-%d'),
            'stock': symbol,
            'stock_name': stock_name,
            'crypto_name': crypto_name,
            'open': float(row['open']) if pd.notnull(row['open']) else None,
            'close': float(row['close']) if pd.notnull(row['close']) else None,
            'volume': float(row['volume']) if pd.notnull(row['volume']) else None,
            'high': float(row['high']) if pd.notnull(row['high']) else None,
            'low': float(row['low']) if pd.notnull(row['low']) else None
        }
        data_to_store.append(data_point)
    
    store_stock_daily_data(data_to_store)

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean().iloc[-1]


def fetch_williams_r_polygon(symbol: str, db_params: Dict[str, Any], end_date: datetime, base: str = 'USD') -> List[Dict[str, Any]]:
    end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(weeks=80)

    daily_table_name = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
    weekly_table_name = f"crypto_weekly_table{'_' + base.lower() if base.lower() != 'usd' else ''}"

    # Fetch data from TimescaleDB
    conn = psycopg2.connect(**db_params)
    # try:
    #     with conn.cursor() as cur:
    #         cur.execute(f"""
    #             WITH weekly_data AS (
    #                 SELECT 
    #                     date_trunc('week', datetime) as week,
    #                     stock,
    #                     high,
    #                     low,
    #                     close,
    #                     ROW_NUMBER() OVER (PARTITION BY date_trunc('week', datetime), stock ORDER BY datetime DESC) as rn
    #                 FROM 
    #                     {daily_table_name}
    #                 WHERE 
    #                     stock = %s 
    #                     AND datetime BETWEEN %s AND %s
    #             )
    #             SELECT 
    #                 week,
    #                 MAX(high) as week_high,
    #                 MIN(low) as week_low,
    #                 MAX(CASE WHEN rn = 1 THEN close END) as week_close
    #             FROM 
    #                 weekly_data
    #             GROUP BY 
    #                 week, stock
    #             ORDER BY 
    #                 week
    #         """, (symbol, start_date, end_date))
    #         db_data = cur.fetchall()
    # finally:
    #     conn.close()

    try:
        with conn.cursor() as cur:
            # Check if we have data for this day
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM {daily_table_name} 
                WHERE DATE(datetime) = DATE(%s) AND stock = %s
            """, (end_date, symbol))
            
            has_trading = cur.fetchone()[0] > 0
            
            if not has_trading:
                print(f"No trading data for {symbol} on {end_date.date()}. Skipping process.")
                return None

            # If it is a trading day, proceed with weekly calculations
            cur.execute(f"""
                WITH RECURSIVE weeks AS (
                    -- Base case: start with the end_date
                    SELECT %s::timestamp as week_end
                    UNION ALL
                    -- Recursive case: subtract 7 days
                    SELECT (week_end - interval '7 days')::timestamp
                    FROM weeks
                    WHERE week_end - interval '7 days' >= %s::timestamp - interval '7 days'
                ),
                date_periods AS (
                    SELECT 
                        t.datetime,
                        t.high,
                        t.low,
                        t.close,
                        (SELECT min(w.week_end)
                         FROM weeks w
                         WHERE w.week_end >= t.datetime) as week_end
                    FROM {daily_table_name} t
                    WHERE stock = %s 
                    AND datetime BETWEEN %s AND %s
                ),
                weekly_data AS (
                    SELECT 
                        week_end as week,
                        MAX(high) as week_high,
                        MIN(low) as week_low,
                        (array_agg(close ORDER BY datetime DESC))[1] as week_close
                    FROM date_periods
                    GROUP BY week_end
                )
                SELECT 
                    week,
                    week_high,
                    week_low,
                    week_close
                FROM weekly_data
                ORDER BY week DESC
            """, (end_date, start_date, symbol, start_date, end_date))
            
            db_data = cur.fetchall()
    finally:
        conn.close()

    if not db_data:
        return None

    df = pd.DataFrame(db_data, columns=['week', 'week_high', 'week_low', 'week_close'])
    df['week'] = pd.to_datetime(df['week'])
    df.set_index('week', inplace=True)
    df.sort_index(inplace=True)

    # Convert Decimal to float
    for col in ['week_high', 'week_low', 'week_close']:
        df[col] = df[col].astype(float)

    # Calculate Highest High and Lowest Low over the last 52 weeks
    df['highest_high_52'] = df['week_high'].rolling(window=52).max()
    df['lowest_low_52'] = df['week_low'].rolling(window=52).min()

    # Calculate Williams %R
    df['willr'] = ((df['highest_high_52'] - df['week_close']) / (df['highest_high_52'] - df['lowest_low_52'])) * -100

    # Ensure we have at least 21 weeks of Williams %R data
    if len(df.dropna()) < 21:
        print(f"William R Warning: Not enough data for {symbol}. Only {len(df.dropna())} weeks available.")
        return None

    return df[['willr']].reset_index().rename(columns={'week': 't'}).dropna().to_dict('records')

def fetch_force_index_data(symbol: str, db_params: Dict[str, Any], end_date: datetime, base: str = 'USD') -> List[Dict[str, Any]]:
    # end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    # target_weekday = (end_date.weekday() - 1) % 7  # Get the weekday of the day before
    # start_date = end_date - timedelta(weeks=70)

    # # Adjust start_date to the target weekday
    # start_date -= timedelta(days=(start_date.weekday() - target_weekday) % 7)

    daily_table_name = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
    weekly_table_name = f"crypto_weekly_table{'_' + base.lower() if base.lower() != 'usd' else ''}"

    # Fetch data from TimescaleDB
    # conn = psycopg2.connect(**db_params)
    # try:
    #     with conn.cursor() as cur:
    #         cur.execute(f"""
    #             WITH weekly_data AS (
    #                 SELECT 
    #                     date_trunc('week', datetime) as week,
    #                     stock,
    #                     close,
    #                     volume,
    #                     ROW_NUMBER() OVER (PARTITION BY date_trunc('week', datetime), stock ORDER BY datetime DESC) as rn
    #                 FROM 
    #                     {daily_table_name}
    #                 WHERE 
    #                     stock = %s 
    #                     AND datetime BETWEEN %s AND %s
    #             )
    #             SELECT 
    #                 week,
    #                 MAX(CASE WHEN rn = 1 THEN close END) as week_close,
    #                 SUM(volume) as week_volume
    #             FROM 
    #                 weekly_data
    #             GROUP BY 
    #                 week, stock
    #             ORDER BY 
    #                 week
    #         """, (symbol, start_date, end_date))
    #         db_data = cur.fetchall()
    # finally:
    #     conn.close()

    end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(weeks=70)

    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            # Check if we have data for this day
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM {daily_table_name} 
                WHERE DATE(datetime) = DATE(%s) AND stock = %s
            """, (end_date, symbol))
            
            has_trading = cur.fetchone()[0] > 0
      
            if not has_trading:
                print(f"No trading data for {symbol} on {end_date.date()}. Skipping process.")
                return None

            # If it is a trading day, proceed with weekly calculations
            cur.execute(f"""
                WITH RECURSIVE weeks AS (
                    -- Base case: start with the end_date
                    SELECT %s::timestamp as week_end
                    UNION ALL
                    -- Recursive case: subtract 7 days
                    SELECT (week_end - interval '7 days')::timestamp
                    FROM weeks
                    WHERE week_end - interval '7 days' >= %s::timestamp - interval '7 days'
                ),
                date_periods AS (
                    SELECT 
                        t.datetime,
                        t.close,
                        t.volume,
                        (SELECT min(w.week_end)
                         FROM weeks w
                         WHERE w.week_end >= t.datetime) as week_end
                    FROM {daily_table_name} t
                    WHERE stock = %s 
                    AND datetime BETWEEN %s AND %s
                ),
                weekly_data AS (
                    SELECT 
                        week_end as week,
                        (array_agg(close ORDER BY datetime DESC))[1] as week_close,
                        SUM(volume) as week_volume
                    FROM date_periods
                    GROUP BY week_end
                )
                SELECT 
                    week,
                    week_close,
                    week_volume
                FROM weekly_data
                ORDER BY week DESC
            """, (end_date, start_date, symbol, start_date, end_date))
            
            db_data = cur.fetchall()
    finally:
        conn.close()
    if not db_data:
        return None

    df = pd.DataFrame(db_data, columns=['datetime', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    df.sort_index(inplace=True)

    # Calculate Force Index
    df['force_index'] = (df['close'] - df['close'].shift(1)) * df['volume']

    # Ensure we have at least 55 weeks of Force Index data
    if len(df) < 55:
        print(f"Force Index Warning: Not enough data for {symbol}. Only {len(df)} weeks available.")

    return df[['force_index', 'close', 'volume']].reset_index().rename(columns={'datetime': 't'}).dropna().to_dict('records')


######################### DEFINE FUNCTIONS TO STORE DATA IN TIMESCALE DB #############################

def store_force_index_data(data, symbol, base: str = 'USD'):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    table_name = f"crypto_weekly_table{'_' + base.lower() if base.lower() != 'usd' else ''}"
    
    values = [(
        data['datetime'],
        symbol,
        data['force_index_7_week'],
        data['force_index_52_week'],
        data['last_week_force_index_7_week'],
        data['last_week_force_index_52_week'],
        data['force_index_alert_state'],
        datetime.now(timezone.utc)
    )]
    execute_values(cur, f"""
        INSERT INTO {table_name} (
            datetime, stock, force_index_7_week, force_index_52_week,
            last_week_force_index_7_week, last_week_force_index_52_week, force_index_alert_state, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            force_index_7_week = EXCLUDED.force_index_7_week,
            force_index_52_week = EXCLUDED.force_index_52_week,
            last_week_force_index_7_week = EXCLUDED.last_week_force_index_7_week,
            last_week_force_index_52_week = EXCLUDED.last_week_force_index_52_week,
            force_index_alert_state = EXCLUDED.force_index_alert_state,
            last_modified_date = EXCLUDED.last_modified_date;
    """, values)
    conn.commit()
    cur.close()
    conn.close()

def store_williams_r_data(data, symbol, base: str = 'USD'):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    table_name = f"crypto_weekly_table{'_' + base.lower() if base.lower() != 'usd' else ''}"

    values = [(
        data['datetime'],
        symbol,
        data['williams_r'],
        data['williams_r_ema'],
        data['williams_r_momentum_alert_state'],
        datetime.now(timezone.utc)
    )]

    execute_values(cur, f"""
        INSERT INTO {table_name} (
            datetime, stock, williams_r, williams_r_ema, williams_r_momentum_alert_state, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            williams_r = EXCLUDED.williams_r,
            williams_r_ema = EXCLUDED.williams_r_ema,
            williams_r_momentum_alert_state = EXCLUDED.williams_r_momentum_alert_state,
            last_modified_date = EXCLUDED.last_modified_date
    """, values)

    conn.commit()
    cur.close()
    conn.close()

def store_stock_data(data, base: str = 'USD'):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    table_name = f"crypto_daily_table{'_' + base.lower() if base.lower() != 'usd' else ''}"

    values = [(
        data['datetime'],
        data['stock'],
        data['stock_name'],
        data['crypto_name'],
        data['ema'],
        data['open'],
        data['close'],
        data['volume'],
        data['high'],
        data['low'],
        datetime.now(timezone.utc)
    )]

    execute_values(cur, f"""
        INSERT INTO {table_name} (
            datetime, stock, stock_name, crypto_name, ema, open, close, volume, high, low, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            stock_name = EXCLUDED.stock_name,
            crypto_name = EXCLUDED.crypto_name,
            ema = EXCLUDED.ema,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            last_modified_date = EXCLUDED.last_modified_date
    """, values)

    conn.commit()
    cur.close()
    conn.close()


def store_stock_daily_data(data_list):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Prepare the values list for bulk insert
    values = [
        (
            data['datetime'],
            data['stock'],
            data['stock_name'],
            data['crypto_name'],
            data['open'],
            data['close'],
            data['volume'],
            data['high'],
            data['low'],
            datetime.now(timezone.utc)
        )
        for data in data_list
    ]

    # Perform bulk upsert
    execute_values(cur, """
        INSERT INTO crypto_daily_table (
            datetime, stock, stock_name, crypto_name, open, close, volume, high, low, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            stock_name = EXCLUDED.stock_name,
            crypto_name = EXCLUDED.crypto_name,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            last_modified_date = EXCLUDED.last_modified_date
    """, values)

    conn.commit()
    cur.close()
    conn.close()