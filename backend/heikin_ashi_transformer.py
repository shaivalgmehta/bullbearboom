import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple, Union, Optional

class HeikinAshiTransformer:
    """
    Transformer to convert OHLC data to Heikin-Ashi data.
    Heikin-Ashi is a modified candlestick technique that helps identify trends more easily.
    """
    
    @staticmethod
    def transform_dataframe(df: pd.DataFrame, 
                           open_col: str = 'open', 
                           high_col: str = 'high', 
                           low_col: str = 'low', 
                           close_col: str = 'close') -> pd.DataFrame:
        """
        Transform a DataFrame containing OHLC data to Heikin-Ashi data.
        
        Args:
            df: DataFrame with OHLC data
            open_col: Column name for open prices
            high_col: Column name for high prices
            low_col: Column name for low prices
            close_col: Column name for close prices
            
        Returns:
            DataFrame with additional Heikin-Ashi columns: ha_open, ha_high, ha_low, ha_close, ha_color
        """
        if df.empty:
            return df.copy()
            
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # Ensure all price columns are numeric
        for col in [open_col, high_col, low_col, close_col]:
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
        
        # Calculate HA Close: (Open + High + Low + Close) / 4
        result_df['ha_close'] = (result_df[open_col] + result_df[high_col] + 
                               result_df[low_col] + result_df[close_col]) / 4
        
        # Calculate HA Open: (Previous HA Open + Previous HA Close) / 2
        # For first row, use actual open price as HA Open
        result_df['ha_open'] = 0.0
        
        # Reset index to make sure we can use integer-based indexing
        result_df = result_df.reset_index(drop=False)
        
        # Store the original index to restore it later if needed
        if 'index' in result_df.columns:
            original_index = 'index'
        elif 'level_0' in result_df.columns:
            original_index = 'level_0'
        else:
            original_index = None
            
        # Set the first value
        result_df.loc[0, 'ha_open'] = result_df.loc[0, open_col]
        
        # Calculate the rest
        for i in range(1, len(result_df)):
            result_df.loc[i, 'ha_open'] = (result_df.loc[i-1, 'ha_open'] + 
                                         result_df.loc[i-1, 'ha_close']) / 2
        
        # Restore the original index if needed
        if original_index and original_index in result_df.columns:
            result_df = result_df.set_index(original_index)
        
        # Calculate HA High: Max(High, HA Open, HA Close)
        result_df['ha_high'] = result_df[[high_col, 'ha_open', 'ha_close']].max(axis=1)
        
        # Calculate HA Low: Min(Low, HA Open, HA Close)
        result_df['ha_low'] = result_df[[low_col, 'ha_open', 'ha_close']].min(axis=1)
        
        # Determine candle color (green for bullish, red for bearish)
        result_df['ha_color'] = np.where(result_df['ha_close'] >= result_df['ha_open'], 
                                        'green', 'red')
        
        return result_df
    
    @staticmethod
    def transform_dict_list(data_list: List[Dict[str, Any]], 
                          open_key: str = 'open', 
                          high_key: str = 'high', 
                          low_key: str = 'low', 
                          close_key: str = 'close',
                          datetime_key: str = 'datetime') -> List[Dict[str, Any]]:
        """
        Transform a list of dictionaries containing OHLC data to Heikin-Ashi data.
        
        Args:
            data_list: List of dictionaries with OHLC data
            open_key: Dictionary key for open prices
            high_key: Dictionary key for high prices
            low_key: Dictionary key for low prices
            close_key: Dictionary key for close prices
            datetime_key: Dictionary key for datetime values
            
        Returns:
            List of dictionaries with added Heikin-Ashi values
        """
        if not data_list:
            return []
            
        # Convert to DataFrame for processing
        df = pd.DataFrame(data_list)
        
        if df.empty or any(key not in df.columns for key in [open_key, high_key, low_key, close_key]):
            return data_list
        
        # Apply the DataFrame transformation
        ha_df = HeikinAshiTransformer.transform_dataframe(
            df, 
            open_col=open_key, 
            high_col=high_key, 
            low_col=low_key, 
            close_col=close_key
        )
        
        # Convert back to list of dictionaries
        result = ha_df.to_dict('records')
        
        return result
    
    @staticmethod
    def detect_color_change(current_data: Dict[str, Any], previous_data: Dict[str, Any]) -> Optional[str]:
        """
        Detect if there's been a color change between two Heikin-Ashi candles.
        
        Args:
            current_data: Dictionary with current period's Heikin-Ashi data
            previous_data: Dictionary with previous period's Heikin-Ashi data
            
        Returns:
            String indicating the type of color change, or None if no change
            Possible values: "green_to_red", "red_to_green", None
        """
        if not current_data or not previous_data:
            return None
            
        if 'ha_color' not in current_data or 'ha_color' not in previous_data:
            return None
            
        prev_color = previous_data['ha_color']
        current_color = current_data['ha_color']
        
        if prev_color == 'green' and current_color == 'red':
            return "green_to_red"
        elif prev_color == 'red' and current_color == 'green':
            return "red_to_green"
        else:
            return None
    
    @staticmethod
    def aggregate_to_custom_periods(data: pd.DataFrame, 
                                   period: int, 
                                   datetime_col: str = 'datetime',
                                   open_col: str = 'open',
                                   high_col: str = 'high',
                                   low_col: str = 'low',
                                   close_col: str = 'close',
                                   volume_col: Optional[str] = 'volume') -> pd.DataFrame:
        """
        Aggregate OHLC data to a custom period (e.g., 3 days, 2 weeks).
        
        Args:
            data: DataFrame with OHLC data
            period: Number of rows to aggregate into one period
            datetime_col: Column name for datetime
            open_col: Column name for open prices
            high_col: Column name for high prices
            low_col: Column name for low prices
            close_col: Column name for close prices
            volume_col: Column name for volume (if available)
            
        Returns:
            DataFrame with aggregated data
        """
        if data.empty or len(data) < period:
            return data.copy()
            
        # Make a copy to avoid modifying the original
        data = data.copy()
        
        # Save the original index
        original_has_datetime_index = False
        if datetime_col in data.columns:
            # Datetime is a column
            pass
        elif datetime_col in data.index.names:
            # Datetime is already the index
            original_has_datetime_index = True
            # Convert index to column for easier handling
            data = data.reset_index()
        
        # Create result DataFrame to hold aggregated data
        result = []
        
        # Process in chunks of the specified period
        for i in range(0, len(data), period):
            chunk = data.iloc[i:i+period]
            
            if len(chunk) < period:
                # Skip incomplete periods if desired
                # Alternatively, could process them anyway
                continue
                
            aggregated = {
                datetime_col: chunk[datetime_col].iloc[-1],  # Use the last date in the period
                open_col: chunk[open_col].iloc[0],  # First open
                high_col: chunk[high_col].max(),    # Highest high
                low_col: chunk[low_col].min(),      # Lowest low
                close_col: chunk[close_col].iloc[-1]  # Last close
            }
            
            # Add volume if available
            if volume_col and volume_col in chunk.columns:
                aggregated[volume_col] = chunk[volume_col].sum()
                
            result.append(aggregated)
        
        # Convert to DataFrame
        result_df = pd.DataFrame(result)
        
        # Set datetime as index if it was originally the index
        if original_has_datetime_index:
            result_df = result_df.set_index(datetime_col)
            
        return result_df

# Testing function
def test_heikin_ashi_calculation():
    """
    Test the Heikin-Ashi calculation with sample data.
    """
    # Sample OHLC data
    sample_data = [
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
    ]
    
    # Transform the data
    ha_data = HeikinAshiTransformer.transform_dict_list(sample_data)
    
    # Print the results
    print("Original vs Heikin-Ashi Data:")
    for i, data in enumerate(ha_data):
        original = sample_data[i]
        print(f"Date: {original['datetime']}")
        print(f"  Original: Open={original['open']}, High={original['high']}, Low={original['low']}, Close={original['close']}")
        print(f"  Heikin-Ashi: Open={data['ha_open']:.2f}, High={data['ha_high']:.2f}, Low={data['ha_low']:.2f}, Close={data['ha_close']:.2f}, Color={data['ha_color']}")
        
    # Check for color changes
    print("\nColor Changes:")
    for i in range(1, len(ha_data)):
        change = HeikinAshiTransformer.detect_color_change(ha_data[i], ha_data[i-1])
        if change:
            print(f"Date: {ha_data[i]['datetime']} - {change}")
    
    # Test custom aggregation (3-day periods)
    print("\nCustom Aggregation (3-day periods):")
    df = pd.DataFrame(sample_data)
    agg_df = HeikinAshiTransformer.aggregate_to_custom_periods(df, 3)
    print(agg_df)
    
    # Calculate Heikin-Ashi on the aggregated data
    agg_ha_df = HeikinAshiTransformer.transform_dataframe(agg_df)
    print("\nHeikin-Ashi on Aggregated Data:")
    print(agg_ha_df[['ha_open', 'ha_high', 'ha_low', 'ha_close', 'ha_color']])
    
    return ha_data

if __name__ == "__main__":
    test_heikin_ashi_calculation()