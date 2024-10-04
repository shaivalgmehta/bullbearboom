import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode

API_KEY = 'Hz8AtnGUqVbCf16GgULrCDJHtVBsre5_'  # Replace with your actual Polygon.io API key

def get_all_crypto_tickers():
    base_url = 'https://api.polygon.io/v3/reference/tickers'
    params = {
        'market': 'crypto',
        'active': 'true',
        'limit': 1000,  # Maximum allowed per page
        'apiKey': API_KEY
    }
    
    all_tickers = []
    next_url = f"{base_url}?{urlencode(params)}"

    while next_url:
        try:
            response = requests.get(next_url)
            response.raise_for_status()
            data = response.json()

            # Filter tickers with quote currency USD
            tickers = [ticker['ticker'] for ticker in data['results'] if ticker.get('currency_symbol') == 'USD']
            all_tickers.extend(tickers)

            # Check if there is a next_url
            next_url = data.get('next_url')
            if next_url:
                # Append API key to the next_url
                next_url = f"{next_url}&apiKey={API_KEY}"
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching crypto tickers: {e}")
            break  # Exit the loop if an error occurs

    return all_tickers

# Function to get 52-week time series data for each crypto ticker
def get_time_series_data(ticker):
    end_date = datetime.now().strftime('%Y-%m-%d')  # Current date
    start_date = (datetime.now() - timedelta(weeks=52)).strftime('%Y-%m-%d')  # 52 weeks ago

    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/week/{start_date}/{end_date}'
    params = {
        'apiKey': API_KEY,
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data['results'] if 'results' in data else []

    except requests.exceptions.RequestException as e:
        print(f"Error fetching time series data for {ticker}: {e}")
        return []

# Fetch 52-week time series data for all crypto tickers
def fetch_crypto_data_for_last_52_weeks():
    # Step 1: Get all tickers
    tickers = get_all_crypto_tickers()

    # Step 2: Print all the tickers fetched
    print(f"\nTotal number of crypto tickers with USD: {len(tickers)}")
    print("Tickers fetched:")
    for ticker in tickers:
        print(ticker)

    # Step 3: Fetch time series data for each ticker
    for ticker in tickers:
        print(f"\nFetching time series data for {ticker}...")
        time_series_data = get_time_series_data(ticker)

        if time_series_data:
            print(f"Time series data for {ticker}:")
            for data_point in time_series_data:
                print(f"Date: {datetime.utcfromtimestamp(data_point['t'] / 1000).strftime('%Y-%m-%d')}, "
                      f"Open: {data_point['o']}, High: {data_point['h']}, Low: {data_point['l']}, "
                      f"Close: {data_point['c']}, Volume: {data_point['v']}")
        else:
            print(f"No data available for {ticker}.")

# Main function to run the script
if __name__ == '__main__':
    print("Fetching all crypto tickers trading with USD...")
    fetch_crypto_data_for_last_52_weeks()