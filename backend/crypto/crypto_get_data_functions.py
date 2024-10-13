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
from typing import Dict, Any


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

def fetch_technical_indicators_polygon(symbol):
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=201)).strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
  
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            df = df.sort_values('t')
            df['ema'] = df['c'].ewm(span=200, adjust=False).mean()
            latest_data = df.iloc[-1]
            return {
                'datetime': latest_data['t'].strftime('%Y-%m-%d'),
                'open': latest_data['o'],
                'close': latest_data['c'],
                'ema': latest_data['ema'],
                'volume': latest_data['v']
            }
    return None

def fetch_williams_r_polygon(symbol):
    end_date = datetime.now().strftime('%Y-%m-%d')
    # Fetch 73 weeks of data: 52 weeks for initial Williams %R + 21 weeks for EMA
    start_date = (datetime.now() - timedelta(weeks=73)).strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/week/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            
            # Calculate Highest High and Lowest Low over the last 52 weeks
            df['highest_high_52'] = df['h'].rolling(window=52).max()
            df['lowest_low_52'] = df['l'].rolling(window=52).min()
            
            # Calculate Williams %R
            df['willr'] = ((df['highest_high_52'] - df['c']) / (df['highest_high_52'] - df['lowest_low_52'])) * -100
            
            # Ensure we have at least 21 weeks of Williams %R data
            if len(df.dropna()) < 21:
                print(f"Warning: Not enough data for {symbol}. Only {len(df.dropna())} weeks available.")
            
            return df[['t', 'willr']].dropna().to_dict('records')
    return None

def fetch_force_index_data(symbol):
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(weeks=55)).strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/week/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            df['force_index'] = (df['c'] - df['c'].shift(1)) * df['v']
            return df[['t', 'force_index', 'c', 'v']].dropna().to_dict('records')
    return None

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
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO crypto_daily_table (
            datetime, stock, stock_name, crypto_name, ema, open, close, volume, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            stock_name = EXCLUDED.stock_name,
            crypto_name = EXCLUDED.crypto_name,
            ema = EXCLUDED.ema,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
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
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO crypto_daily_table_eth (
            datetime, stock, stock_name, crypto_name, ema, open, close, volume, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            stock_name = EXCLUDED.stock_name,
            crypto_name = EXCLUDED.crypto_name,
            ema = EXCLUDED.ema,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
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
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO crypto_daily_table_btc (
            datetime, stock, stock_name, crypto_name, ema, open, close, volume, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            stock_name = EXCLUDED.stock_name,
            crypto_name = EXCLUDED.crypto_name,
            ema = EXCLUDED.ema,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            last_modified_date = EXCLUDED.last_modified_date
    """, values)

    conn.commit()
    cur.close()
    conn.close()