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


# def fetch_technical_indicators_polygon(symbol):
#     end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
#     start_date = (datetime.now() - timedelta(days=201)).strftime('%Y-%m-%d')
#     url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
  
#     response = requests.get(url)
#     if response.status_code == 200:
#         data = response.json()
#         if data['results']:
#             df = pd.DataFrame(data['results'])
#             df['t'] = pd.to_datetime(df['t'], unit='ms')
#             df = df.sort_values('t')
#             df['ema'] = df['c'].ewm(span=200, adjust=False).mean()
#             latest_data = df.iloc[-1]
#             return {
#                 'datetime': latest_data['t'].strftime('%Y-%m-%d'),
#                 'open': latest_data['o'],
#                 'close': latest_data['c'],
#                 'ema': latest_data['ema'],
#                 'volume': latest_data['v'],
#                 'high': latest_data['h'],
#                 'low': latest_data['l']
#             }
#     return None


def fetch_technical_indicators_polygon(symbol: str, db_params: Dict[str, Any], POLYGON_API_KEY: str, store_stock_daily_data) -> Dict[str, Any]:
    end_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    start_date = end_date - timedelta(days=200)  # We need 201 days of data to calculate 200-day EMA

    # Fetch data from TimescaleDB
    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datetime, stock_name, crypto_name, open, close, volume, high, low
                FROM crypto_daily_table
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

    # If we still don't have enough data, log an error
    if len(df) < 200:
        print(f"Error: Not enough valid data available for {symbol} to calculate EMA. Only {len(df)} days available.")
        return None

    # Calculate EMA for the latest date
    latest_date = df.index[-1]
    try:
        ema = calculate_ema(df['close'], 200)
    except Exception as e:
        print(f"Error calculating EMA for {symbol}: {str(e)}")
        return None
    
    latest_data = df.loc[latest_date]
    try:
        return {
            'datetime': latest_date.strftime('%Y-%m-%d'),
            'open': float(latest_data['open']),
            'close': float(latest_data['close']),
            'ema': float(ema),
            'volume': float(latest_data['volume']),
            'high': float(latest_data['high']),
            'low': float(latest_data['low'])
        }
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


def fetch_williams_r_polygon(symbol: str, db_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    end_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    start_date = end_date - timedelta(weeks=73)

    # Fetch data from TimescaleDB
    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH weekly_data AS (
                    SELECT 
                        date_trunc('week', datetime) as week,
                        stock,
                        high,
                        low,
                        close,
                        ROW_NUMBER() OVER (PARTITION BY date_trunc('week', datetime), stock ORDER BY datetime DESC) as rn
                    FROM 
                        crypto_daily_table
                    WHERE 
                        stock = %s 
                        AND datetime BETWEEN %s AND %s
                )
                SELECT 
                    week,
                    MAX(high) as week_high,
                    MIN(low) as week_low,
                    MAX(CASE WHEN rn = 1 THEN close END) as week_close
                FROM 
                    weekly_data
                GROUP BY 
                    week, stock
                ORDER BY 
                    week
            """, (symbol, start_date, end_date))
            db_data = cur.fetchall()
    finally:
        conn.close()

    df = pd.DataFrame(db_data, columns=['week', 'week_high', 'week_low', 'week_close'])
    df['week'] = pd.to_datetime(df['week'])
    df.set_index('week', inplace=True)
    df.sort_index(inplace=True)
    print(f"{df}")

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

def fetch_force_index_data(symbol: str, db_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    end_date = datetime.now(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    target_weekday = (end_date.weekday() - 1) % 7  # Get the weekday of the day before
    start_date = end_date - timedelta(weeks=55)

    # Adjust start_date to the target weekday
    start_date -= timedelta(days=(start_date.weekday() - target_weekday) % 7)

    # Convert Python weekday to PostgreSQL weekday
    postgres_weekday = (target_weekday + 1) % 7
     # Fetch data from TimescaleDB
    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH weekly_data AS (
                    SELECT 
                        date_trunc('week', datetime) as week,
                        stock,
                        close,
                        volume,
                        ROW_NUMBER() OVER (PARTITION BY date_trunc('week', datetime), stock ORDER BY datetime DESC) as rn
                    FROM 
                        crypto_daily_table
                    WHERE 
                        stock = %s 
                        AND datetime BETWEEN %s AND %s
                )
                SELECT 
                    week,
                    MAX(CASE WHEN rn = 1 THEN close END) as week_close,
                    SUM(volume) as week_volume
                FROM 
                    weekly_data
                GROUP BY 
                    week, stock
                ORDER BY 
                    week
            """, (symbol, start_date, end_date))
            db_data = cur.fetchall()
    finally:
        conn.close()

    df = pd.DataFrame(db_data, columns=['datetime', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    df.sort_index(inplace=True)

    # Calculate Force Index
    df['force_index'] = (df['close'] - df['close'].shift(1)) * df['volume']

    # Ensure we have at least 55 weeks of Force Index data
    if len(df) < 55:
        print(f"Foroce Index Warning: Not enough data for {symbol}. Only {len(df)} weeks available.")

    return df[['force_index', 'close', 'volume']].reset_index().rename(columns={'datetime': 't'}).dropna().to_dict('records')


######################### DEFINE FUNCTIONS TO STORE DATA IN TIMESCALE DB #############################

def store_force_index_data(data, symbol):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
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
    execute_values(cur, """
        INSERT INTO crypto_weekly_table (
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

def store_williams_r_data(data, symbol):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    values = [(
        data['datetime'],
        symbol,
        data['williams_r'],
        data['williams_r_ema'],
        data['williams_r_momentum_alert_state'],
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO crypto_weekly_table (
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

def store_stock_data(data):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

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

    execute_values(cur, """
        INSERT INTO crypto_daily_table (
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

################################################# CONVERT TO ETH BASE ################################################################################################################################################################
    
def fetch_stock_list_polygon_eth():
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

# Global cache dictionary
eth_price_cache: Dict[str, float] = {}

def fetch_eth_price(date: datetime) -> float:
    date_str = date.strftime('%Y-%m-%d')
    
    # Always check the cache first
    if date_str in eth_price_cache:
        return eth_price_cache[date_str]
    
    # If not in cache, return None (we shouldn't be making individual API calls here)
    return None

def bulk_fetch_eth_prices(start_date: datetime, end_date: datetime) -> None:
    # Ensure we're not fetching more than 2 years of data at once (Polygon.io limit)
    if (end_date - start_date).days > 730:
        raise ValueError("Date range exceeds 2 years. Please use a smaller range.")

    url = f"https://api.polygon.io/v2/aggs/ticker/X:ETHUSD/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            for result in data['results']:
                date = datetime.fromtimestamp(result['t'] / 1000).strftime('%Y-%m-%d')
                eth_price_cache[date] = result['c']
    else:
        print(f"Error fetching ETH prices: {response.status_code}")

# Function to ensure we have all required ETH prices
def ensure_eth_prices(start_date: datetime, end_date: datetime) -> None:
    missing_dates = []
    current_date = start_date
    while current_date <= end_date:
        if current_date.strftime('%Y-%m-%d') not in eth_price_cache:
            missing_dates.append(current_date)
        current_date += timedelta(days=1)
    
    if missing_dates:
        bulk_fetch_eth_prices(min(missing_dates), max(missing_dates))

# Use this at the start of your script
def initialize_eth_price_cache(start_date: datetime, end_date: datetime) -> None:
    bulk_fetch_eth_prices(start_date, end_date)


def fetch_technical_indicators_polygon_eth(symbol):
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=201)
    initialize_eth_price_cache(start_date, end_date)
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?apiKey={POLYGON_API_KEY}"
  
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            df = df.sort_values('t')
            
            # Ensure we have all required ETH prices
            ensure_eth_prices(start_date, end_date)

            # Fetch ETH price for each date
            df['eth_price'] = df['t'].apply(fetch_eth_price)
            
            # Convert prices to ETH base
            df['o_eth'] = df['o'] / df['eth_price']
            df['c_eth'] = df['c'] / df['eth_price']
            df['h_eth'] = df['h'] / df['eth_price']
            df['l_eth'] = df['l'] / df['eth_price']
            
            # Convert volume to ETH
            df['v_usd'] = df['v'] * df['c']  # Assuming 'v' is in crypto units
            df['v_eth'] = df['v_usd'] / df['eth_price']
            
            df['ema_eth'] = df['c_eth'].ewm(span=200, adjust=False).mean()
            
            latest_data = df.iloc[-1]
            
            return {
                'datetime': latest_data['t'].strftime('%Y-%m-%d'),
                'open': latest_data['o_eth'],
                'close': latest_data['c_eth'],
                'ema': latest_data['ema_eth'],
                'volume': latest_data['v_eth'],
                'high': latest_data['h_eth'],
                'low': latest_data['l_eth'],
                'eth_price': latest_data['eth_price']
            }
    return None

def fetch_williams_r_polygon_eth(symbol):
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=73)
    initialize_eth_price_cache(start_date, end_date)
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/week/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            
            # Ensure we have all required ETH prices
            ensure_eth_prices(start_date, end_date)

            # Fetch ETH prices for each date
            df['eth_price'] = df['t'].apply(fetch_eth_price)
            
            # Convert prices to ETH base
            df['h_eth'] = df['h'] / df['eth_price']
            df['l_eth'] = df['l'] / df['eth_price']
            df['c_eth'] = df['c'] / df['eth_price']
            
            # Calculate Highest High and Lowest Low over the last 52 weeks in ETH terms
            df['highest_high_52'] = df['h_eth'].rolling(window=52).max()
            df['lowest_low_52'] = df['l_eth'].rolling(window=52).min()
            
            # Calculate Williams %R
            df['willr'] = ((df['highest_high_52'] - df['c_eth']) / (df['highest_high_52'] - df['lowest_low_52'])) * -100
            
            return df[['t', 'willr', 'eth_price']].dropna().to_dict('records')
    return None

def fetch_force_index_data_eth(symbol):
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=55)
    initialize_eth_price_cache(start_date, end_date)
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/week/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            
            # Ensure we have all required ETH prices
            ensure_eth_prices(start_date, end_date)
            # Fetch ETH prices for each date
            df['eth_price'] = df['t'].apply(fetch_eth_price)
            
            # Convert closing prices to ETH base
            df['c_eth'] = df['c'] / df['eth_price']
            
            # Convert volume to ETH
            df['v_usd'] = df['v'] * df['c']  # Assuming 'v' is in crypto units
            df['v_eth'] = df['v_usd'] / df['eth_price']
            
            # Calculate Force Index using ETH-based prices and volumes
            df['force_index'] = (df['c_eth'] - df['c_eth'].shift(1)) * df['v_eth']
            
            return df[['t', 'force_index', 'c_eth', 'v_eth', 'eth_price']].dropna().to_dict('records')
    return None

######################### DEFINE FUNCTIONS TO STORE DATA IN TIMESCALE DB #############################

def store_force_index_data_eth(data, symbol):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
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
    execute_values(cur, """
        INSERT INTO crypto_weekly_table_eth (
            datetime, stock, force_index_7_week, force_index_52_week,
            last_week_force_index_7_week, last_week_force_index_52_week, 
            force_index_alert_state, last_modified_date
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

def store_williams_r_data_eth(data, symbol):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    values = [(
        data['datetime'],
        symbol,
        data['williams_r'],
        data['williams_r_ema'],
        data['williams_r_momentum_alert_state'],
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO crypto_weekly_table_eth (
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

def store_stock_data_eth(data):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

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

    execute_values(cur, """
        INSERT INTO crypto_daily_table_eth (
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

    ################################################# CONVERT TO BTC BASE ################################################################################################################################################################
    
def fetch_stock_list_polygon_btc():
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

# Global cache dictionary
btc_price_cache: Dict[str, float] = {}

def fetch_btc_price(date: datetime) -> float:
    date_str = date.strftime('%Y-%m-%d')
    
    # Always check the cache first
    if date_str in btc_price_cache:
        return btc_price_cache[date_str]
    
    # If not in cache, return None (we shouldn't be making individual API calls here)
    return None

def bulk_fetch_btc_prices(start_date: datetime, end_date: datetime) -> None:
    # Ensure we're not fetching more than 2 years of data at once (Polygon.io limit)
    if (end_date - start_date).days > 730:
        raise ValueError("Date range exceeds 2 years. Please use a smaller range.")

    url = f"https://api.polygon.io/v2/aggs/ticker/X:BTCUSD/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            for result in data['results']:
                date = datetime.fromtimestamp(result['t'] / 1000).strftime('%Y-%m-%d')
                btc_price_cache[date] = result['c']
    else:
        print(f"Error fetching BTC prices: {response.status_code}")

# Function to ensure we have all required BTC prices
def ensure_btc_prices(start_date: datetime, end_date: datetime) -> None:
    missing_dates = []
    current_date = start_date
    while current_date <= end_date:
        if current_date.strftime('%Y-%m-%d') not in btc_price_cache:
            missing_dates.append(current_date)
        current_date += timedelta(days=1)
    
    if missing_dates:
        bulk_fetch_btc_prices(min(missing_dates), max(missing_dates))

# Use this at the start of your script
def initialize_btc_price_cache(start_date: datetime, end_date: datetime) -> None:
    bulk_fetch_btc_prices(start_date, end_date)


def fetch_technical_indicators_polygon_btc(symbol):
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=201)
    initialize_btc_price_cache(start_date, end_date)
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?apiKey={POLYGON_API_KEY}"
  
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            df = df.sort_values('t')
            
            # Ensure we have all required BTC prices
            ensure_btc_prices(start_date, end_date)

            # Fetch BTC price for each date
            df['btc_price'] = df['t'].apply(fetch_btc_price)
            
            # Convert prices to BTC base
            df['o_btc'] = df['o'] / df['btc_price']
            df['c_btc'] = df['c'] / df['btc_price']
            df['h_btc'] = df['h'] / df['btc_price']
            df['l_btc'] = df['l'] / df['btc_price']
            
            # Convert volume to BTC
            df['v_usd'] = df['v'] * df['c']  # Assuming 'v' is in crypto units
            df['v_btc'] = df['v_usd'] / df['btc_price']
            
            df['ema_btc'] = df['c_btc'].ewm(span=200, adjust=False).mean()
            
            latest_data = df.iloc[-1]
            
            return {
                'datetime': latest_data['t'].strftime('%Y-%m-%d'),
                'open': latest_data['o_btc'],
                'close': latest_data['c_btc'],
                'ema': latest_data['ema_btc'],
                'volume': latest_data['v_btc'],
                'high': latest_data['h_btc'],
                'low': latest_data['l_btc'],
                'btc_price': latest_data['btc_price']
            }
    return None

def fetch_williams_r_polygon_btc(symbol):
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=73)
    initialize_btc_price_cache(start_date, end_date)
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/week/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            
            # Ensure we have all required BTC prices
            ensure_btc_prices(start_date, end_date)

            # Fetch BTC prices for each date
            df['btc_price'] = df['t'].apply(fetch_btc_price)
            
            # Convert prices to BTC base
            df['h_btc'] = df['h'] / df['btc_price']
            df['l_btc'] = df['l'] / df['btc_price']
            df['c_btc'] = df['c'] / df['btc_price']
            
            # Calculate Highest High and Lowest Low over the last 52 weeks in BTC terms
            df['highest_high_52'] = df['h_btc'].rolling(window=52).max()
            df['lowest_low_52'] = df['l_btc'].rolling(window=52).min()
            
            # Calculate Williams %R
            df['willr'] = ((df['highest_high_52'] - df['c_btc']) / (df['highest_high_52'] - df['lowest_low_52'])) * -100
            
            return df[['t', 'willr', 'btc_price']].dropna().to_dict('records')
    return None

def fetch_force_index_data_btc(symbol):
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=55)
    initialize_btc_price_cache(start_date, end_date)
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/week/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            
            # Ensure we have all required BTC prices
            ensure_btc_prices(start_date, end_date)
            # Fetch BTC prices for each date
            df['btc_price'] = df['t'].apply(fetch_btc_price)
            
            # Convert closing prices to BTC base
            df['c_btc'] = df['c'] / df['btc_price']
            
            # Convert volume to BTC
            df['v_usd'] = df['v'] * df['c']  # Assuming 'v' is in crypto units
            df['v_btc'] = df['v_usd'] / df['btc_price']
            
            # Calculate Force Index using BTC-based prices and volumes
            df['force_index'] = (df['c_btc'] - df['c_btc'].shift(1)) * df['v_btc']
            
            return df[['t', 'force_index', 'c_btc', 'v_btc', 'btc_price']].dropna().to_dict('records')
    return None

######################### DEFINE FUNCTIONS TO STORE DATA IN TIMESCALE DB #############################

def store_force_index_data_btc(data, symbol):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
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
    execute_values(cur, """
        INSERT INTO crypto_weekly_table_btc (
            datetime, stock, force_index_7_week, force_index_52_week,
            last_week_force_index_7_week, last_week_force_index_52_week, 
            force_index_alert_state, last_modified_date
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

def store_williams_r_data_btc(data, symbol):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    values = [(
        data['datetime'],
        symbol,
        data['williams_r'],
        data['williams_r_ema'],
        data['williams_r_momentum_alert_state'],
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO crypto_weekly_table_btc (
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

def store_stock_data_btc(data):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

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

    execute_values(cur, """
        INSERT INTO crypto_daily_table_btc (
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