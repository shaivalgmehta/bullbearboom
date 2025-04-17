import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from twelvedata import TDClient
from us_stock_data_transformer_new import get_transformer
import json  # Import json for pretty printing
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

# Twelve Data API key
TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY')

# TimescaleDB connection details
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Initialize TwelveData client
td = TDClient(apikey=TWELVE_DATA_API_KEY)

######################### DEFINE FUNCTIONS TO FETCH DATA FROM TWELVE DATA #############################
def fetch_stock_list_twelve_data():
    """
    Fetch stock list from both NYSE and NASDAQ exchanges using separate API calls.
    Combines the results into a single list.
    """
    all_stocks = []
    
    # List of exchanges to query
    exchanges = ['NYSE', 'NASDAQ']
    
    for exchange in exchanges:
        try:
            url = f"https://api.twelvedata.com/stocks?country=United States&type=Common Stock&exchange={exchange}&apikey={TWELVE_DATA_API_KEY}"
            response = requests.get(url)
            
            if response.status_code == 200:
                stocks = response.json().get('data', [])
                all_stocks.extend(stocks)
                print(f"Successfully fetched {len(stocks)} stocks from {exchange}")
            else:
                print(f"Error fetching {exchange} stocks: {response.status_code}")
                
            # Add a small delay between requests to avoid rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error fetching data from {exchange}: {str(e)}")
    
    if not all_stocks:
        raise Exception("Failed to fetch stocks from both exchanges")
    
    # Remove any potential duplicates (in case a stock is listed on both exchanges)
    unique_stocks = {stock['symbol']: stock for stock in all_stocks}.values()
    
    print(f"Total unique stocks fetched: {len(unique_stocks)}")
    return list(unique_stocks)

def fetch_stock_statistics_twelve_data(symbol):
    statistics = td.get_statistics(symbol=symbol, country="United States").as_json()
    return statistics

def fetch_stock_cashflow_twelve_data(symbol):
    td = TDClient(apikey=TWELVE_DATA_API_KEY)
    cashflow = td.get_cash_flow(symbol=symbol, country="United States", period="quarterly").as_json()
    return cashflow

def fetch_missing_data_from_twelve_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    td = TDClient(apikey=TWELVE_DATA_API_KEY)
    time_series = td.time_series(
        symbol=symbol,
        interval="1day",
        start_date=start_date,
        end_date=end_date,
        country="United States",
        type="Common Stock",
        timezone="UTC",
        outputsize =500
    ).as_json()
    if time_series:
        df = pd.DataFrame(time_series)
        if df.empty:
            return None
            
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        df.set_index('datetime', inplace=True)
        df = df.rename(columns={
            'open': 'open',
            'close': 'close',
            'volume': 'volume',
            'high': 'high',
            'low': 'low'
        })
        for col in ['open', 'close', 'volume', 'high', 'low']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df[['open', 'close', 'volume', 'high', 'low']]
    return None

def fetch_technical_indicators_twelve_data(symbol: str, db_params: Dict[str, Any], TWELVE_DATA_API_KEY: str, store_stock_daily_data, end_date: datetime) -> Dict[str, Any]:
    end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=300)
    
    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT datetime, stock_name, open, close, volume, high, low
                FROM us_daily_table
                WHERE stock = %s AND datetime BETWEEN %s AND %s
                ORDER BY datetime
            """, (symbol, start_date, end_date))
            db_data = cur.fetchall()
    finally:
        conn.close()
        
    df = pd.DataFrame(db_data, columns=['datetime', 'stock_name', 'open', 'close', 'volume', 'high', 'low'])
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    df.set_index('datetime', inplace=True)
    
    for col in ['open', 'close', 'volume', 'high', 'low']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Check if we have enough data AND it's up to date
    has_sufficient_data = len(df) >= 200
    is_data_current = False if df.empty else df.index[-1].date() == end_date.date()
    
    if has_sufficient_data and is_data_current:
        print(f"Have {len(df)} data points and current data for {symbol}, skipping missing dates check")
    else:
        # Generate date range excluding weekends
        date_range = pd.date_range(start=start_date, end=end_date, freq='B', tz=pytz.UTC)
        missing_dates = date_range.difference(df.index)
        
        # Additional filter to remove any remaining weekend dates
        missing_dates = missing_dates[missing_dates.dayofweek < 5]
        
        if len(missing_dates) > 0:
            print(f"Fetching missing data for {symbol} - Need more data points or current data")
            new_data = fetch_missing_data_from_twelve_data(
                symbol,
                missing_dates.min().strftime('%Y-%m-%d'),
                missing_dates.max().strftime('%Y-%m-%d')
            )
            if new_data is not None:
                store_missing_data(new_data, symbol, df['stock_name'].iloc[0] if not df.empty else None, store_stock_daily_data)
                df = pd.concat([df, new_data])
                df.sort_index(inplace=True)
    
    if len(df) < 200:
        print(f"Error: Not enough valid data available for {symbol}. Only {len(df)} days available.")
        return None
    
    latest_date = df.index[-1]
    calculate_ema_flag = latest_date.date() == end_date.date()
    
    latest_data = df.iloc[-1]
    result = {
        'datetime': latest_date.strftime('%Y-%m-%d'),
        'open': float(latest_data['open']),
        'close': float(latest_data['close']),
        'volume': float(latest_data['volume']),
        'high': float(latest_data['high']),
        'low': float(latest_data['low'])
    }
    
    if calculate_ema_flag:
        result['ema'] = float(df['close'].ewm(span=200, adjust=False).mean().iloc[-1])
    else:
        result['ema'] = None
        print(f"Warning: Latest data for {symbol} is not up to date. EMA not calculated.")
    
    return result

def store_missing_data(df: pd.DataFrame, symbol: str, stock_name: str, store_stock_daily_data):
    data_to_store = []
    for date, row in df.iterrows():
        data_point = {
            'datetime': date.strftime('%Y-%m-%d'),
            'stock': symbol,
            'stock_name': stock_name,
            'open': float(row['open']) if pd.notnull(row['open']) else None,
            'close': float(row['close']) if pd.notnull(row['close']) else None,
            'volume': float(row['volume']) if pd.notnull(row['volume']) else None,
            'high': float(row['high']) if pd.notnull(row['high']) else None,
            'low': float(row['low']) if pd.notnull(row['low']) else None
        }
        data_to_store.append(data_point)

    store_stock_daily_data(data_to_store)


def fetch_williams_r_twelve_data(symbol: str, db_params: Dict[str, Any], end_date: datetime) -> List[Dict[str, Any]]:
    end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(weeks=80)

    # Fetch data from TimescaleDB
    conn = psycopg2.connect(**db_params)

    try:
        with conn.cursor() as cur:
            # Check if we have data for this day
            cur.execute("""
                SELECT COUNT(*) 
                FROM us_daily_table 
                WHERE DATE(datetime) = DATE(%s) AND stock = %s
            """, (end_date, symbol))
            
            # has_trading = cur.fetchone()[0] > 0
            
            # if not has_trading:
            #     print(f"No trading data for {symbol} on {end_date.date()}. Skipping process.")
            #     return None

            # If it is a trading day, proceed with weekly calculations
            cur.execute("""
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
                    FROM us_daily_table t
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
    # print(f'{df}')
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

def fetch_force_index_data(symbol: str, db_params: Dict[str, Any], end_date: datetime) -> List[Dict[str, Any]]:

    end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(weeks=80)

    # Fetch data from TimescaleDB
    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            # Check if we have data for this day
            cur.execute("""
                SELECT COUNT(*) 
                FROM us_daily_table 
                WHERE DATE(datetime) = DATE(%s) AND stock = %s
            """, (end_date, symbol))
            
            # has_trading = cur.fetchone()[0] > 0
            
            # if not has_trading:
            #     print(f"No trading data for {symbol} on {end_date.date()}. Skipping process.")
            #     return None

            # If it is a trading day, proceed with weekly calculations
            cur.execute("""
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
                    FROM us_daily_table t
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
    # print(f'{df}')
    # Calculate Force Index
    df['force_index'] = (df['close'] - df['close'].shift(1)) * df['volume']

    # Ensure we have at least 55 weeks of Force Index data
    if len(df) < 55:
        print(f"Force Index Warning: Not enough data for {symbol}. Only {len(df)} weeks available.")

    return df[['force_index', 'close', 'volume']].reset_index().rename(columns={'datetime': 't'}).dropna().to_dict('records')


######################### DEFINE FUNCTIONS TO STORE DATA IN TIMESCALE DB #############################

def store_stock_daily_data(data_list):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    values = [
        (
            data['datetime'],
            data['stock'],
            data['stock_name'],
            data['open'],
            data['close'],
            data['volume'],
            data['high'],
            data['low'],
            datetime.now(timezone.utc)
        )
        for data in data_list
    ]

    execute_values(cur, """
        INSERT INTO us_daily_table (
            datetime, stock, stock_name, open, close, volume, high, low, last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            stock_name = EXCLUDED.stock_name,
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
        INSERT INTO us_weekly_table (
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
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
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
        INSERT INTO us_weekly_table (
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
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    try:
        values = [(
            data['datetime'],
            data['stock'],
            data['stock_name'],
            data['ema'],
            data['open'],
            data['close'],
            data['volume'],
            data['high'],
            data['low'],
            data['pe_ratio'],
            data['pb_ratio'],
            data['peg_ratio'],
            data['price_change_3m'],
            data['price_change_6m'],
            data['price_change_12m'],
            data['earnings_yield'],
            data['book_to_price'],                 
            datetime.now(timezone.utc)
        )]
        
        execute_values(cur, """
            INSERT INTO us_daily_table (
                datetime, stock, stock_name, ema, open, close, volume, high, low, 
                pe_ratio, pb_ratio, peg_ratio, 
                price_change_3m, price_change_6m, price_change_12m, 
                earnings_yield, book_to_price, last_modified_date
            ) VALUES %s
            ON CONFLICT (datetime, stock) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                ema = EXCLUDED.ema,
                open = EXCLUDED.open,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                pe_ratio = EXCLUDED.pe_ratio,
                pb_ratio = EXCLUDED.pb_ratio,
                peg_ratio = EXCLUDED.peg_ratio,
                price_change_3m = EXCLUDED.price_change_3m,
                price_change_6m = EXCLUDED.price_change_6m,
                price_change_12m = EXCLUDED.price_change_12m,
                earnings_yield = EXCLUDED.earnings_yield,
                book_to_price = EXCLUDED.book_to_price,
                last_modified_date = EXCLUDED.last_modified_date
        """, values)
        
        conn.commit()  # Make sure to commit the transaction
        
    except Exception as e:
        print(f"Error storing data for {data['stock']}: {str(e)}")
        conn.rollback()  # Rollback on error
        raise
    finally:
        cur.close()
        conn.close()


def store_statistics_data(data):

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    values = [(
        data['datetime'],
        data['stock'],
        data['sales'],
        data['ebitda'],
        data['free_cash_flow'],
        data['market_cap'],
        data['return_on_equity'],
        data['return_on_assets'],
        data['price_to_sales'],
        data['free_cash_flow_yield'],
        data['dividend_payments'],
        data['share_repurchases'],
        data['shareholder_yield'],
        None,  # return_on_equity_rank
        None,  # return_on_assets_rank
        None,  # price_to_sales_rank
        None,  # free_cash_flow_yield_rank
        None,  # shareholder_yield_rank
        data['ev_ebitda'],
        data['diluted_eps'],
        data['book_value_per_share'],
        data['quarterly_earnings_growth_yoy'],
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO us_quarterly_table (
            datetime, 
            stock, 
            sales, 
            ebitda, 
            free_cash_flow,
            market_cap,
            return_on_equity,
            return_on_assets,
            price_to_sales,
            free_cash_flow_yield,
            dividend_payments,
            share_repurchases,
            shareholder_yield,
            return_on_equity_rank,
            return_on_assets_rank,
            price_to_sales_rank,
            free_cash_flow_yield_rank,
            shareholder_yield_rank,
            ev_ebitda,
            diluted_eps,
            book_value_per_share,
            quarterly_earnings_growth_yoy,
            last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            sales = EXCLUDED.sales,
            ebitda = EXCLUDED.ebitda,
            free_cash_flow = EXCLUDED.free_cash_flow,
            market_cap = EXCLUDED.market_cap,
            return_on_equity = EXCLUDED.return_on_equity,
            return_on_assets = EXCLUDED.return_on_assets,
            price_to_sales = EXCLUDED.price_to_sales,
            free_cash_flow_yield = EXCLUDED.free_cash_flow_yield,
            dividend_payments = EXCLUDED.dividend_payments,
            share_repurchases = EXCLUDED.share_repurchases,
            shareholder_yield = EXCLUDED.shareholder_yield,
            return_on_equity_rank = EXCLUDED.return_on_equity_rank,
            return_on_assets_rank = EXCLUDED.return_on_assets_rank,
            price_to_sales_rank = EXCLUDED.price_to_sales_rank,
            free_cash_flow_yield_rank = EXCLUDED.free_cash_flow_yield_rank,
            shareholder_yield_rank = EXCLUDED.shareholder_yield_rank,
            ev_ebitda = EXCLUDED.ev_ebitda,
            diluted_eps = EXCLUDED.diluted_eps,
            book_value_per_share = EXCLUDED.book_value_per_share,
            quarterly_earnings_growth_yoy = EXCLUDED.quarterly_earnings_growth_yoy,
            last_modified_date = EXCLUDED.last_modified_date
    """, values)

    conn.commit()
    cur.close()
    conn.close()

####################### ANCHOR OBV DATABASE FUNCTIONS ################################################################

def fetch_obv_data(symbol: str, db_params: Dict[str, Any], end_date: datetime) -> List[Dict[str, Any]]:
    """
    Fetch weekly price and volume data for OBV calculation
    Makes sure to fetch from before the quarter start date to ensure we have enough data
    """
    end_date = end_date.astimezone(pytz.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Get start of previous quarter to ensure we have enough data
    month = end_date.month
    quarter_start_month = ((month - 1) // 3) * 3 + 1
    start_date = datetime(
        end_date.year if month >= 4 else end_date.year - 1,
        quarter_start_month if month >= 4 else 10,
        1,
        tzinfo=end_date.tzinfo
    )

    conn = psycopg2.connect(**db_params)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH RECURSIVE weeks AS (
                    SELECT %s::timestamp as week_end
                    UNION ALL
                    SELECT (week_end - interval '7 days')::timestamp
                    FROM weeks
                    WHERE week_end - interval '7 days' >= %s
                ),
                weekly_data AS (
                    SELECT 
                        (SELECT min(w.week_end)
                         FROM weeks w
                         WHERE w.week_end >= t.datetime) as datetime,
                        (array_agg(close ORDER BY datetime DESC))[1] as close,
                        SUM(volume) as volume
                    FROM us_daily_table t
                    WHERE stock = %s 
                      AND datetime BETWEEN %s AND %s
                    GROUP BY (SELECT min(w.week_end)
                             FROM weeks w
                             WHERE w.week_end >= t.datetime)
                )
                SELECT 
                    datetime as t,
                    close as c,
                    volume as v
                FROM weekly_data
                WHERE datetime IS NOT NULL
                ORDER BY datetime DESC
            """, (end_date, start_date, symbol, start_date, end_date))
            
            weekly_data = cur.fetchall()
            if not weekly_data:
                return None

            return [
                {
                    't': row[0],
                    'c': float(row[1]) if row[1] is not None else None,
                    'v': float(row[2]) if row[2] is not None else None
                }
                for row in weekly_data
            ]
    finally:
        conn.close()

def store_obv_data(data: Dict[str, Any], symbol: str):
    """
    Store OBV calculation results
    """
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    values = [(
        data['datetime'],
        symbol,
        data['anchored_obv'],
        data['anchor_date'],
        data['obv_confidence'],
        data['anchored_obv_alert_state'],
        datetime.now(timezone.utc)
    )]

    execute_values(cur, """
        INSERT INTO us_weekly_table (
            datetime, 
            stock, 
            anchored_obv,
            anchor_date,
            obv_confidence,
            anchored_obv_alert_state,
            last_modified_date
        ) VALUES %s
        ON CONFLICT (datetime, stock) DO UPDATE SET
            anchored_obv = EXCLUDED.anchored_obv,
            anchor_date = EXCLUDED.anchor_date,
            obv_confidence = EXCLUDED.obv_confidence,
            anchored_obv_alert_state = EXCLUDED.anchored_obv_alert_state,
            last_modified_date = EXCLUDED.last_modified_date
    """, values)

    conn.commit()
    cur.close()
    conn.close()