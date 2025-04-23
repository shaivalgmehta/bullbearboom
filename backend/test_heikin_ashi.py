import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Import the HeikinAshiTransformer
from heikin_ashi_transformer import HeikinAshiTransformer

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

def fetch_stock_data(symbol, days=180, market='us'):
    """
    Fetch stock data from the database for testing
    
    Args:
        symbol: Stock symbol
        days: Number of days of data to fetch
        market: Market ('us', 'in', or 'crypto')
    
    Returns:
        DataFrame with OHLC data
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Select the appropriate table based on the market
    if market == 'us':
        table_name = 'us_daily_table'
    elif market == 'in':
        table_name = 'in_daily_table'
    elif market == 'crypto':
        table_name = 'crypto_daily_table'
    else:
        raise ValueError(f"Unsupported market: {market}")
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = f"""
                SELECT 
                    datetime,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM {table_name}
                WHERE stock = %s
                AND datetime BETWEEN %s AND %s
                ORDER BY datetime ASC
            """
            
            cur.execute(query, (symbol, start_date, end_date))
            data = cur.fetchall()
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df

def test_with_real_data(symbol='AAPL', market='us'):
    """
    Test the Heikin-Ashi calculations with real market data
    
    Args:
        symbol: Stock symbol to test with
        market: Market ('us', 'in', or 'crypto')
    
    Returns:
        DataFrame with original and Heikin-Ashi data
    """
    print(f"Testing with {market.upper()} stock: {symbol}")
    
    # Fetch the data
    df = fetch_stock_data(symbol, days=180, market=market)
    
    if df.empty:
        print(f"No data found for {symbol} in {market} market")
        return None
    
    print(f"Fetched {len(df)} days of data")
    
    # Calculate Heikin-Ashi values
    ha_df = HeikinAshiTransformer.transform_dataframe(df)
    
    # Print some sample data
    print("\nSample data (last 5 rows):")
    print(ha_df[['datetime', 'open', 'high', 'low', 'close', 
                 'ha_open', 'ha_high', 'ha_low', 'ha_close', 'ha_color']].tail())
    
    # Test custom aggregation and Heikin-Ashi calculations
    
    # 3-day aggregation
    print("\n3-day aggregation:")
    agg_3d = HeikinAshiTransformer.aggregate_to_custom_periods(df, 3)
    ha_3d = HeikinAshiTransformer.transform_dataframe(agg_3d)
    print(ha_3d[['open', 'high', 'low', 'close', 
                 'ha_open', 'ha_high', 'ha_low', 'ha_close', 'ha_color']].tail())
    
    # Check for color changes in 3-day data
    print("\nColor changes in 3-day Heikin-Ashi (last 10 periods):")
    for i in range(1, min(10, len(ha_3d))):
        current = ha_3d.iloc[-i].to_dict()
        previous = ha_3d.iloc[-(i+1)].to_dict()
        change = HeikinAshiTransformer.detect_color_change(current, previous)
        if change:
            date = current['datetime'] if 'datetime' in current else ha_3d.index[-i]
            print(f"Period ending {date}: {change}")
    
    # Test weekly aggregation (assuming 5 trading days per week)
    print("\nWeekly aggregation:")
    agg_weekly = HeikinAshiTransformer.aggregate_to_custom_periods(df, 5)
    ha_weekly = HeikinAshiTransformer.transform_dataframe(agg_weekly)
    print(ha_weekly[['open', 'high', 'low', 'close', 
                    'ha_open', 'ha_high', 'ha_low', 'ha_close', 'ha_color']].tail())
    
    # Test 2-week aggregation
    print("\n2-week aggregation:")
    agg_2w = HeikinAshiTransformer.aggregate_to_custom_periods(agg_weekly, 2)
    ha_2w = HeikinAshiTransformer.transform_dataframe(agg_2w)
    print(ha_2w[['open', 'high', 'low', 'close', 
                 'ha_open', 'ha_high', 'ha_low', 'ha_close', 'ha_color']].tail())
    
    # Check for color changes in 2-week data
    print("\nColor changes in 2-week Heikin-Ashi (last 10 periods):")
    for i in range(1, min(10, len(ha_2w))):
        current = ha_2w.iloc[-i].to_dict()
        previous = ha_2w.iloc[-(i+1)].to_dict()
        change = HeikinAshiTransformer.detect_color_change(current, previous)
        if change:
            date = current['datetime'] if 'datetime' in current else ha_2w.index[-i]
            print(f"Period ending {date}: {change}")
    
    return {
        'daily': ha_df,
        '3d': ha_3d,
        'weekly': ha_weekly,
        '2w': ha_2w
    }

def plot_comparison(ha_df, title="OHLC vs Heikin-Ashi Comparison", days=30):
    """
    Create a plot to compare regular OHLC and Heikin-Ashi candles
    
    Args:
        ha_df: DataFrame with both OHLC and Heikin-Ashi data
        title: Plot title
        days: Number of days to plot
    """
    if ha_df is None or ha_df.empty:
        print("No data to plot")
        return
    
    # Get the last N days of data
    plot_data = ha_df.tail(days).copy()
    
    # Create a figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    
    # Format the date column if it exists
    if 'datetime' in plot_data.columns:
        plot_data['date'] = pd.to_datetime(plot_data['datetime'])
        plot_data = plot_data.set_index('date')
    
    # Plot regular OHLC candles
    for i in range(len(plot_data)):
        date = plot_data.index[i]
        op, hi, lo, cl = plot_data.iloc[i][['open', 'high', 'low', 'close']]
        
        # Determine color (green for bullish, red for bearish)
        color = 'green' if cl >= op else 'red'
        
        # Plot candle body
        ax1.plot([date, date], [op, cl], color=color, linewidth=6)
        
        # Plot wicks
        ax1.plot([date, date], [lo, hi], color='black', linewidth=1)
    
    # Plot Heikin-Ashi candles
    for i in range(len(plot_data)):
        date = plot_data.index[i]
        op, hi, lo, cl = plot_data.iloc[i][['ha_open', 'ha_high', 'ha_low', 'ha_close']]
        
        # Determine color (green for bullish, red for bearish)
        color = 'green' if cl >= op else 'red'
        
        # Plot candle body
        ax2.plot([date, date], [op, cl], color=color, linewidth=6)
        
        # Plot wicks
        ax2.plot([date, date], [lo, hi], color='black', linewidth=1)
    
    # Set titles and labels
    ax1.set_title('Regular OHLC')
    ax2.set_title('Heikin-Ashi')
    ax2.set_xlabel('Date')
    ax1.set_ylabel('Price')
    ax2.set_ylabel('Price')
    
    plt.suptitle(title, fontsize=16)
    plt.tight_layout()
    safe_title = title.replace(' ', '_').replace(':', '_')
    plt.savefig(f"{safe_title}.png")
    plt.show()

def main():
    """
    Main function to test the Heikin-Ashi implementation
    """
    # Test with different markets and symbols
    markets = {
        'us': ['AAPL', 'MSFT', 'AMZN'],
        'in': ['KBSINDIA', 'KMSUGAR', 'INTELLECT'],
        'crypto': ['X:BTCUSD', 'X:ETHUSD', 'X:SOLUSD']
    }
    
    # Choose one symbol from each market for testing
    results = {}
    for market, symbols in markets.items():
        symbol = symbols[0]  # Use the first symbol from each market
        print(f"\n\n{'=' * 50}")
        print(f"Testing with {market.upper()} market: {symbol}")
        print(f"{'=' * 50}")
        results[market] = test_with_real_data(symbol, market)
        
        # Plot comparison if data is available
        if results[market] and 'daily' in results[market]:
            plot_comparison(results[market]['daily'], f"{symbol} - {market.upper()}")
    
    print("\nAll tests completed!")
    return results

if __name__ == "__main__":
    main()