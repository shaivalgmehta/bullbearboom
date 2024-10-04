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
    url = f"https://api.polygon.io/v3/reference/tickers?market=crypto&active=true&currency=USD&apiKey={POLYGON_API_KEY}"
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
    
    return [{'symbol': ticker['ticker'], 'name': ticker['name']} for ticker in all_tickers]

def fetch_technical_indicators_polygon(symbol):
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=200)).strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            df = pd.DataFrame(data['results'])
            df['t'] = pd.to_datetime(df['t'], unit='ms')
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
        data['ema'],
        data['open'],
        data['close'],
        data['volume'],
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO crypto_daily_table (
            datetime, stock, stock_name, ema, open, close, volume, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            stock_name = EXCLUDED.stock_name,
            ema = EXCLUDED.ema,
            open = EXCLUDED.open,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            last_modified_date = EXCLUDED.last_modified_date
    """, values)

    conn.commit()
    cur.close()
    conn.close()