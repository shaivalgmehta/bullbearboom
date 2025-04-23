import pandas as pd
import numpy as np
import json
from typing import List, Dict, Any, Tuple, Union, Optional
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from heikin_ashi_transformer import HeikinAshiTransformer

class HeikinAshiAlertDetector:
    """
    Detects Heikin-Ashi color change alerts for different timeframes.
    This class works with your existing database structure to fetch data,
    calculate Heikin-Ashi values, detect color changes, and format alerts.
    """
    
    def __init__(self, db_params: Dict[str, Any]):
        """
        Initialize the detector with database connection parameters.
        
        Args:
            db_params: Database connection parameters
        """
        self.db_params = db_params
        self.transformer = HeikinAshiTransformer()
    
    def get_db_connection(self):
        """Get a database connection"""
        return psycopg2.connect(**self.db_params)
    
    def fetch_ohlc_data(self, symbol: str, 
                        market: str = 'us', 
                        days: int = 180, 
                        end_date: Optional[datetime] = None,
                        base: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch OHLC data for a symbol from the appropriate table.
        
        Args:
            symbol: Stock/crypto symbol
            market: Market type ('us', 'in', or 'crypto')
            days: Number of days of data to fetch
            end_date: End date for the data (defaults to current date)
            base: Base currency for crypto (usd, eth, btc) - overrides symbol parsing
            
        Returns:
            DataFrame with OHLC data
        """
        if end_date is None:
            end_date = datetime.now()
            
        start_date = end_date - timedelta(days=days)
        
        # Select the appropriate table based on the market
        if market == 'us':
            table_name = 'us_daily_table'
        elif market == 'in':
            table_name = 'in_daily_table'
        elif market == 'crypto':
            if base:
                # Use the explicitly provided base
                crypto_base = base
            else:
                # Extract from symbol if not provided explicitly
                crypto_base = 'usd'  # Default base for crypto
                if '/' in symbol:
                    # Extract base if it's in the symbol (e.g., BTC/USD)
                    _, crypto_base = symbol.split('/')
            table_name = f"crypto_daily_table{'_' + crypto_base.lower() if crypto_base.lower() != 'usd' else ''}"
        else:
            raise ValueError(f"Unsupported market: {market}")
        
        with self.get_db_connection() as conn:
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
    
    def detect_3d_color_changes(self, symbol: str, market: str = 'us', 
                               end_date: Optional[datetime] = None,
                               base: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Detect color changes in 3-day Heikin-Ashi data.
        
        Args:
            symbol: Stock/crypto symbol
            market: Market type ('us', 'in', or 'crypto')
            end_date: End date for the data (defaults to current date)
            base: Base currency for crypto (usd, eth, btc)
            
        Returns:
            List of alert dictionaries
        """
        # Need at least 180 days to calculate 3-day periods and detect changes
        df = self.fetch_ohlc_data(symbol, market, days=180, end_date=end_date, base=base)
        
        if df.empty or len(df) < 6:  # Need at least two 3-day periods
            return []
        
        # Aggregate to 3-day periods
        agg_3d = HeikinAshiTransformer.aggregate_to_custom_periods(df, 3)
        
        # Calculate Heikin-Ashi on aggregated data
        ha_3d = HeikinAshiTransformer.transform_dataframe(agg_3d)
        
        # Check for color changes in the most recent period
        alerts = []
        if len(ha_3d) >= 2:
            current = ha_3d.iloc[-1].to_dict()
            previous = ha_3d.iloc[-2].to_dict()
            
            change = HeikinAshiTransformer.detect_color_change(current, previous)
            if change:
                alert_type = "heikin_ashi_3d_bullish" if change == "red_to_green" else "heikin_ashi_3d_bearish"
                alerts.append({
                    "type": alert_type,
                    "value": change,
                    "description": f"3-day Heikin-Ashi color change: {change}"
                })
        
        return alerts
    
    def detect_2w_color_changes(self, symbol: str, market: str = 'us', 
                               end_date: Optional[datetime] = None,
                               base: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Detect color changes in 2-week Heikin-Ashi data.
        
        Args:
            symbol: Stock/crypto symbol
            market: Market type ('us', 'in', or 'crypto')
            end_date: End date for the data (defaults to current date)
            base: Base currency for crypto (usd, eth, btc)
            
        Returns:
            List of alert dictionaries
        """
        # Need at least 200 days to calculate weekly and then 2-week periods
        df = self.fetch_ohlc_data(symbol, market, days=200, end_date=end_date, base=base)
        
        if df.empty or len(df) < 10:  # Need at least two 2-week periods (assuming 5 days per week)
            return []
            
        # First, aggregate to weekly periods (assuming 5 trading days per week)
        weekly_df = HeikinAshiTransformer.aggregate_to_custom_periods(df, 5)
        
        # Then, aggregate to 2-week periods
        agg_2w = HeikinAshiTransformer.aggregate_to_custom_periods(weekly_df, 2)
        
        # Calculate Heikin-Ashi on aggregated data
        ha_2w = HeikinAshiTransformer.transform_dataframe(agg_2w)
        
        # Check for color changes in the most recent period
        alerts = []
        if len(ha_2w) >= 2:
            current = ha_2w.iloc[-1].to_dict()
            previous = ha_2w.iloc[-2].to_dict()
            
            change = HeikinAshiTransformer.detect_color_change(current, previous)
            if change:
                alert_type = "heikin_ashi_2w_bullish" if change == "red_to_green" else "heikin_ashi_2w_bearish"
                alerts.append({
                    "type": alert_type,
                    "value": change,
                    "description": f"2-week Heikin-Ashi color change: {change}"
                })
        
        return alerts
    
    def detect_all_alerts(self, symbol: str, market: str = 'us', 
                         end_date: Optional[datetime] = None,
                         base: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect all Heikin-Ashi color change alerts for a symbol.
        
        Args:
            symbol: Stock/crypto symbol
            market: Market type ('us', 'in', or 'crypto')
            end_date: End date for the data (defaults to current date)
            base: Base currency for crypto (usd, eth, btc)
            
        Returns:
            Dictionary with alerts for each timeframe
        """
        alerts = {
            '3d': self.detect_3d_color_changes(symbol, market, end_date, base),
            '2w': self.detect_2w_color_changes(symbol, market, end_date, base)
        }
        
        return alerts
    
    def store_alerts_in_db(self, symbol: str, market: str, 
                          alerts: Dict[str, List[Dict[str, Any]]], 
                          end_date: Optional[datetime] = None,
                          base: Optional[str] = None):
        """
        Store detected Heikin-Ashi alerts in the database.
        
        Args:
            symbol: Stock/crypto symbol
            market: Market type ('us', 'in', or 'crypto')
            alerts: Dictionary with alerts for each timeframe
            end_date: Date for the alerts (defaults to current date)
            base: Base currency for crypto (usd, eth, btc)
        """
        if end_date is None:
            end_date = datetime.now()
            
        # Flatten all alerts into a single list
        all_alerts = []
        for timeframe, timeframe_alerts in alerts.items():
            all_alerts.extend(timeframe_alerts)
            
        if not all_alerts:
            return  # No alerts to store
            
        # Determine the appropriate alerts table
        if market == 'us':
            alerts_table = 'us_alerts_table'
        elif market == 'in':
            alerts_table = 'in_alerts_table'
        elif market == 'crypto':
            if base:
                # Use the explicitly provided base
                crypto_base = base
            else:
                # Extract from symbol if not provided explicitly
                crypto_base = 'usd'  # Default base for crypto
                if '/' in symbol:
                    # Extract base if it's in the symbol (e.g., BTC/USD)
                    _, crypto_base = symbol.split('/')
            alerts_table = f"crypto_alerts_table{'_' + crypto_base.lower() if crypto_base.lower() != 'usd' else ''}"
        else:
            raise ValueError(f"Unsupported market: {market}")
            
        # Get the stock_name/crypto_name
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                # First, check if an alert for this stock on this date already exists
                if market == 'crypto':
                    name_field = 'crypto_name'
                else:
                    name_field = 'stock_name'
                    
                # Determine the appropriate table for getting the name
                if market == 'us':
                    table_name = 'us_daily_table'
                elif market == 'in':
                    table_name = 'in_daily_table'
                else:  # crypto
                    if base:
                        crypto_base = base
                    else:
                        crypto_base = 'usd'
                        if '/' in symbol:
                            _, crypto_base = symbol.split('/')
                    table_name = f"crypto_daily_table{'_' + crypto_base.lower() if crypto_base.lower() != 'usd' else ''}"
                
                # Get the name
                cur.execute(f"""
                    SELECT {name_field}
                    FROM {table_name}
                    WHERE stock = %s
                    ORDER BY datetime DESC
                    LIMIT 1
                """, (symbol,))
                
                name_result = cur.fetchone()
                entity_name = name_result[0] if name_result else symbol
                
                # Check if an alert exists for this date
                cur.execute(f"""
                    SELECT alerts
                    FROM {alerts_table}
                    WHERE stock = %s AND DATE(datetime) = DATE(%s)
                """, (symbol, end_date))
                
                existing_result = cur.fetchone()
                
                if existing_result:
                    # Update existing alert
                    existing_alerts = json.loads(existing_result[0]) if existing_result[0] else []
                    
                    # Add new alerts, avoiding duplicates
                    existing_alert_types = {a['type'] for a in existing_alerts}
                    for alert in all_alerts:
                        if alert['type'] not in existing_alert_types:
                            existing_alerts.append(alert)
                            existing_alert_types.add(alert['type'])
                    
                    cur.execute(f"""
                        UPDATE {alerts_table}
                        SET alerts = %s
                        WHERE stock = %s AND DATE(datetime) = DATE(%s)
                    """, (json.dumps(existing_alerts), symbol, end_date))
                else:
                    # Insert new alert
                    cur.execute(f"""
                        INSERT INTO {alerts_table} (datetime, stock, {name_field}, alerts)
                        VALUES (%s, %s, %s, %s)
                    """, (end_date, symbol, entity_name, json.dumps(all_alerts)))
                
                conn.commit()
    
    def process_symbols(self, symbols: List[Dict[str, Any]], market: str, 
                       end_date: Optional[datetime] = None,
                       base: Optional[str] = None):
        """
        Process a list of symbols to detect and store Heikin-Ashi alerts.
        
        Args:
            symbols: List of symbol dictionaries with 'symbol' key
            market: Market type ('us', 'in', or 'crypto')
            end_date: Date for the alerts (defaults to current date)
            base: Base currency for crypto (usd, eth, btc)
        """
        for symbol_dict in symbols:
            symbol = symbol_dict['symbol']
            try:
                print(f"Processing {symbol} for {market} market...")
                alerts = self.detect_all_alerts(symbol, market, end_date, base)
                
                # Count alerts
                alert_count = sum(len(timeframe_alerts) for timeframe_alerts in alerts.values())
                
                if alert_count > 0:
                    print(f"Found {alert_count} alerts for {symbol}")
                    self.store_alerts_in_db(symbol, market, alerts, end_date, base)
                else:
                    print(f"No alerts for {symbol}")
                    
            except Exception as e:
                print(f"Error processing {symbol}: {str(e)}")
                
    @staticmethod
    def get_sample_data() -> List[Dict[str, Any]]:
        """
        Get sample OHLC data for testing.
        
        Returns:
            List of dictionaries with OHLC data
        """
        return [
            {'datetime': '2023-01-01', 'open': 100, 'high': 110, 'low': 95, 'close': 105},
            {'datetime': '2023-01-02', 'open': 105, 'high': 115, 'low': 100, 'close': 110},
            {'datetime': '2023-01-03', 'open': 110, 'high': 120, 'low': 105, 'close': 115},
            {'datetime': '2023-01-04', 'open': 115, 'high': 125, 'low': 110, 'close': 120},
            {'datetime': '2023-01-05', 'open': 120, 'high': 130, 'low': 115, 'close': 125},
            {'datetime': '2023-01-06', 'open': 125, 'high': 135, 'low': 120, 'close': 130},
            {'datetime': '2023-01-07', 'open': 130, 'high': 140, 'low': 125, 'close': 135},
            {'datetime': '2023-01-08', 'open': 135, 'high': 145, 'low': 130, 'close': 140},
            {'datetime': '2023-01-09', 'open': 140, 'high': 150, 'low': 135, 'close': 145},
            {'datetime': '2023-01-10', 'open': 145, 'high': 155, 'low': 140, 'close': 150},
            # Add downtrend
            {'datetime': '2023-01-11', 'open': 150, 'high': 155, 'low': 135, 'close': 140},
            {'datetime': '2023-01-12', 'open': 140, 'high': 145, 'low': 130, 'close': 135},
            {'datetime': '2023-01-13', 'open': 135, 'high': 140, 'low': 125, 'close': 130},
            {'datetime': '2023-01-14', 'open': 130, 'high': 135, 'low': 120, 'close': 125},
            {'datetime': '2023-01-15', 'open': 125, 'high': 130, 'low': 115, 'close': 120},
            # Trend reversal
            {'datetime': '2023-01-16', 'open': 120, 'high': 130, 'low': 115, 'close': 125},
            {'datetime': '2023-01-17', 'open': 125, 'high': 135, 'low': 120, 'close': 130},
            {'datetime': '2023-01-18', 'open': 130, 'high': 140, 'low': 125, 'close': 135},
        ]


# Simple test function
def test_alert_detector():
    """Test the HeikinAshiAlertDetector with sample data"""
    from datetime import datetime
    
    # Sample database connection parameters (would be replaced with actual values)
    db_params = {
        'host': 'localhost',
        'port': '5432',
        'dbname': 'testdb',
        'user': 'testuser',
        'password': 'testpass'
    }
    
    # Create the detector
    detector = HeikinAshiAlertDetector(db_params)
    
    # Get sample data
    sample_data = detector.get_sample_data()
    
    # Convert to DataFrame
    df = pd.DataFrame(sample_data)
    
    # Calculate Heikin-Ashi values
    ha_df = HeikinAshiTransformer.transform_dataframe(df)
    
    # Print color changes
    print("Color changes in sample data:")
    for i in range(1, len(ha_df)):
        current = ha_df.iloc[i].to_dict()
        previous = ha_df.iloc[i-1].to_dict()
        
        change = HeikinAshiTransformer.detect_color_change(current, previous)
        if change:
            print(f"Date: {current['datetime']} - {change}")
    
    # Test 3-day aggregation
    agg_3d = HeikinAshiTransformer.aggregate_to_custom_periods(df, 3)
    ha_3d = HeikinAshiTransformer.transform_dataframe(agg_3d)
    
    print("\nColor changes in 3-day aggregated data:")
    for i in range(1, len(ha_3d)):
        current = ha_3d.iloc[i].to_dict()
        previous = ha_3d.iloc[i-1].to_dict()
        
        change = HeikinAshiTransformer.detect_color_change(current, previous)
        if change:
            print(f"Period ending {current['datetime']} - {change}")
    
    return ha_df, ha_3d

if __name__ == "__main__":
    test_alert_detector()