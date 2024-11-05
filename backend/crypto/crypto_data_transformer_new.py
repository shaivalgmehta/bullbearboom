from abc import ABC, abstractmethod
from typing import Dict, Any, List
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta


class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

class DailyDataTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            daily_data = data['daily_data']
            
            transformed_data = []
            for day in daily_data:
                # close_price = day['c']
                # volume = day['v']
                # adjusted_volume = volume * close_price

                transformed_day = {
                    'datetime': day['t'].strftime('%Y-%m-%d'),
                    'stock': stock_data['symbol'],
                    'stock_name': stock_data['name'],
                    'crypto_name': stock_data['crypto_name'],
                    'ema': None,  # EMA is not calculated for this data
                    'open': day['o'],
                    'close': day['c'],
                    'volume': day['v'],  # Volume adjusted by close price
                    'high': day['h'],
                    'low': day['l']
                }
                transformed_data.append(transformed_day)
            
            return transformed_data
        except KeyError as e:
            print(f"Error in DailyDataTransformer: Missing key {str(e)}")
            return []
        except Exception as e:
            print(f"Error in DailyDataTransformer: {str(e)}")
            return []

class CoreDataTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            technical_indicator = data['technical_indicator']
            price_changes = data.get('price_changes', {})

            # close_price = technical_indicator['close']
            # volume = technical_indicator['volume']
            # adjusted_volume = volume * close_price
   
            transformed_data = {
                'datetime': technical_indicator['datetime'],
                'stock': stock_data['symbol'],
                'stock_name': stock_data['name'],
                'crypto_name': stock_data['crypto_name'],
                'ema': self._parse_numeric(technical_indicator['ema']),
                'open': self._parse_numeric(technical_indicator['open']),
                'close': self._parse_numeric(technical_indicator['close']),
                'volume': self._parse_numeric(technical_indicator['volume']),
                'high': self._parse_numeric(technical_indicator['high']),
                'low': self._parse_numeric(technical_indicator['low']),
                'price_change_3m': self._parse_numeric(price_changes.get('price_change_3m')),
                'price_change_6m': self._parse_numeric(price_changes.get('price_change_6m')),
                'price_change_12m': self._parse_numeric(price_changes.get('price_change_12m'))
            }
            return [transformed_data]
        except KeyError as e:
            print(f"Error in CoreDataTransformer: Missing key {str(e)}")
            return []
        except Exception as e:
            print(f"Error in CoreDataTransformer: {str(e)}")
            return []

    def _parse_numeric(self, value: Any) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            try:
                return float(value.replace(',', ''))
            except ValueError:
                return None
        else:
            return None


class WilliamsRTransformer(BaseTransformer):
    def __init__(self, db_connection_params, ema_length=21):
        """
        Initialize transformer with Pine Script matching EMA calculation
        ema_length: matches input(defval=21) for the EMA
        """
        self.db_connection_params = db_connection_params
        self.base = 'usd'
        self.ema_length = ema_length

    def calculate_pine_script_ema(self, data: pd.Series, length: int) -> pd.Series:
        """Calculate EMA using Pine Script's method"""
        alpha = 2 / (length + 1)
        return data.ewm(alpha=alpha, adjust=False).mean()

    def transform(self, data: List[Dict[str, Any]], symbol: str, base: str = 'usd') -> Dict[str, Any]:
        self.base = base.lower()
        
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')
            
            # Ensure we have enough data
            if len(df) < self.ema_length:
                raise ValueError(f"Insufficient data for {symbol}. Need at least {self.ema_length} periods, got {len(df)}.")
            
            # Calculate EMA of Williams %R using Pine Script's method
            df['willr_ema'] = self.calculate_pine_script_ema(df['willr'], self.ema_length)
            
            # Get the latest values
            latest_williams_r = float(df['willr'].iloc[-1])
            latest_datetime = df['datetime'].iloc[-1]
            latest_ema = float(df['willr_ema'].iloc[-1])
            
            prev_alert_state = self._get_previous_alert_state(symbol, latest_datetime)
            williams_r_momentum_alert_state = self._determine_alert_state(
                latest_williams_r, 
                latest_ema, 
                prev_alert_state
            )
            
            return {
                'datetime': latest_datetime,
                'williams_r': latest_williams_r,
                'williams_r_ema': latest_ema,
                'williams_r_momentum_alert_state': williams_r_momentum_alert_state
            }
            
        except Exception as e:
            print(f"Error in WilliamsRTransformer for {symbol}: {str(e)}")
            return {
                'datetime': None,
                'williams_r': None,
                'williams_r_ema': None,
                'williams_r_momentum_alert_state': None
            }
            
    def _get_previous_alert_state(self, symbol, latest_datetime):
        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        
        table_suffix = f"_{self.base}" if self.base != 'usd' else ""
        table_name = f"crypto_weekly_table{table_suffix}"
        
        cur.execute(f"""
            SELECT williams_r_momentum_alert_state 
            FROM {table_name}
            WHERE datetime < %s AND stock = %s 
            ORDER BY datetime DESC 
            LIMIT 1
        """, (latest_datetime, symbol))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return result[0] if result else None

    def _determine_alert_state(self, williams_r, ema, prev_state):
        if williams_r is None or ema is None:
            return '-'
        
        meets_criteria = williams_r > ema and williams_r < -50
        
        if prev_state is None:
            return '$$$' if meets_criteria else '-'
        elif prev_state == '$$$':
            return '$' if meets_criteria else '-'
        elif prev_state == '$':
            return '$' if meets_criteria else '-'
        else:  # prev_state == '-'
            return '$$$' if meets_criteria else '-'


class ForceIndexTransformer(BaseTransformer):
    def __init__(self, db_connection_params, fast_period=4, slow_period=14):
        """
        Initialize ForceIndexTransformer with configurable periods but maintain original field names
        """
        self.db_connection_params = db_connection_params
        self.base = 'usd'  # Default base
        self.fast_period = fast_period  # internally used for calculation
        self.slow_period = slow_period  # internally used for calculation
        
    def calculate_force_index_ema(self, df: pd.DataFrame, period: int) -> float:
        """
        Calculate EMA of the provided force_index using Pine Script's method,
        initializing with SMA for the first value
        """
        # Convert force_index to float type
        force_index = df['force_index'].astype(float)
        
        # Calculate initial SMA
        sma = force_index.iloc[:period].mean()
        
        # Initialize EMA array with the SMA as first value
        ema = [float(sma)]  # Ensure float type
        alpha = 2 / (period + 1)
        
        # Calculate EMA values after the initial SMA
        for force_value in force_index.iloc[period:]:
            ema_value = (float(force_value) - ema[-1]) * alpha + ema[-1]
            ema.append(ema_value)
            
        return float(ema[-1])

    def _get_previous_values(self, symbol, latest_datetime):
        table_suffix = f"_{self.base}" if self.base != 'usd' else ""
        table_name = f"crypto_weekly_table{table_suffix}"

        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        try:  # Only real change: Added try-finally block for connection handling
            cur.execute(f"""
                SELECT 
                    force_index_7_week,
                    force_index_52_week,
                    force_index_alert_state
                FROM {table_name} 
                WHERE datetime < %s AND stock = %s 
                ORDER BY datetime DESC 
                LIMIT 1
            """, (latest_datetime, symbol,))
            
            result = cur.fetchone()
            
            if result:
                return {
                    'force_index_7_week': result[0],
                    'force_index_52_week': result[1],
                    'alert_state': result[2]
                }
            return None
            
        finally:  # Only real change: Added finally block
            cur.close()
            conn.close()

    def transform(self, data: List[Dict[str, Any]], symbol: str, base: str = 'usd') -> Dict[str, Any]:
        self.base = base.lower()
        
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')
            latest_datetime = df['datetime'].iloc[-1]

            # Calculate current week's Force Index values
            force_index_7_week = self.calculate_force_index_ema(df, self.fast_period)
            force_index_52_week = self.calculate_force_index_ema(df, self.slow_period)

            # Get previous week's values from database
            prev_values = self._get_previous_values(symbol, latest_datetime)
            
            if prev_values is None:
                # If no previous values exist, use current values (no crossover will be detected)
                last_week_force_index_7_week = force_index_7_week
                last_week_force_index_52_week = force_index_52_week
                prev_alert_state = None
            else:
                last_week_force_index_7_week = prev_values['force_index_7_week']
                last_week_force_index_52_week = prev_values['force_index_52_week']
                prev_alert_state = prev_values['alert_state']

            force_index_alert_state = self._determine_alert_state(
                force_index_7_week, 
                force_index_52_week,
                last_week_force_index_7_week, 
                last_week_force_index_52_week,
                prev_alert_state
            )

            return {
                'datetime': latest_datetime,
                'force_index_7_week': force_index_7_week,
                'force_index_52_week': force_index_52_week,
                'last_week_force_index_7_week': last_week_force_index_7_week,
                'last_week_force_index_52_week': last_week_force_index_52_week,
                'force_index_alert_state': force_index_alert_state
            }
            
        except Exception as e:
            print(f"Error in ForceIndexTransformer for {symbol}: {str(e)}")
            return {
                'datetime': None,
                'force_index_7_week': None,
                'force_index_52_week': None,
                'last_week_force_index_7_week': None,
                'last_week_force_index_52_week': None,
                'force_index_alert_state': None
            }


    def _determine_alert_state(self, fi_7_week, fi_52_week, last_fi_7_week, last_fi_52_week, prev_state):
        upward_crossover = (last_fi_7_week <= last_fi_52_week) and (fi_7_week > fi_52_week)
        downward_crossover = (last_fi_7_week >= last_fi_52_week) and (fi_7_week < fi_52_week)
        
        if upward_crossover:
            return '$$$'
        elif downward_crossover:
            return '-$$$'
        else:
            return '-'


############################## ANCHOR OBV CALCULATIONS #############################################################

class AnchoredOBVTransformer(BaseTransformer):
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params
        self.base = 'usd'  # Default base

    def transform(self, data: List[Dict[str, Any]], symbol: str, base: str = 'usd') -> Dict[str, Any]:
        self.base = base.lower()
        
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')
            
            if df.empty:
                raise ValueError(f"No data available for {symbol}")
                
            latest_datetime = df['datetime'].iloc[-1]
            anchor_date = self._get_anchor_date(latest_datetime)
            
            # Calculate OBV
            current_obv = self._calculate_obv(df, anchor_date)   

            # Calculate confidence
            confidence = self._calculate_confidence(df, anchor_date)

            # Get previous values
            prev_values = self._get_previous_values(symbol, latest_datetime)
            prev_obv = prev_values.get('anchored_obv') if prev_values else None

            # Determine alert state
            alert_state = self._determine_alert_state(current_obv, prev_obv)
            
            return {
                'datetime': latest_datetime,
                'anchored_obv': current_obv,
                'anchor_date': anchor_date,
                'obv_confidence': confidence,
                'anchored_obv_alert_state': alert_state
            }
            
        except Exception as e:
            print(f"Error in AnchoredOBVTransformer for {symbol}/{self.base}: {str(e)}")
            return {
                'datetime': None,
                'anchored_obv': None,
                'anchor_date': None,
                'obv_confidence': None,
                'anchored_obv_alert_state': None
            }

    def _get_anchor_date(self, current_date: datetime) -> datetime:
        """Get the start of the current quarter"""
        month = current_date.month
        quarter_start_month = ((month - 1) // 3) * 3 + 1
        
        return datetime(
            current_date.year, 
            quarter_start_month, 
            1, 
            tzinfo=current_date.tzinfo
        )

    def _calculate_obv(self, df: pd.DataFrame, anchor_date: datetime) -> float:
        """Calculate OBV from anchor date"""
        # Filter data from anchor date
        df = df[df['datetime'] >= anchor_date].copy()
        
        if len(df) < 2:
            return 0
        
        # Initialize OBV with explicit dtype
        df['obv'] = pd.Series(0, index=df.index, dtype='float64')
        
        # Use appropriate close price column based on base
        close_col = 'c' if self.base == 'usd' else f'c_{self.base}'
        volume_col = 'v' if self.base == 'usd' else f'v_{self.base}'
        
        # Calculate daily price changes
        df['price_change'] = df[close_col].astype('float64').diff()
        
        # Calculate OBV
        for i in range(1, len(df)):
            prev_obv = float(df['obv'].iloc[i-1])
            curr_volume = float(df[volume_col].iloc[i])
            
            if df['price_change'].iloc[i] > 0:
                df.loc[df.index[i], 'obv'] = prev_obv + curr_volume
            elif df['price_change'].iloc[i] < 0:
                df.loc[df.index[i], 'obv'] = prev_obv - curr_volume
            else:
                df.loc[df.index[i], 'obv'] = prev_obv
        return float(df['obv'].iloc[-1])

    def _calculate_confidence(self, df: pd.DataFrame, anchor_date: datetime) -> float:
        """Calculate confidence based on data completeness"""
        # Get all dates from anchor to latest
        date_range = pd.date_range(
            start=anchor_date,
            end=df['datetime'].max(),
            freq='W'
        )
        
        # Calculate the percentage of weeks we have data for
        actual_weeks = df[df['datetime'] >= anchor_date]['datetime'].nunique()
        expected_weeks = len(date_range)
        
        if expected_weeks == 0:
            return 0
            
        return (actual_weeks / expected_weeks) * 100

    def _get_previous_values(self, symbol: str, latest_datetime: datetime) -> Dict[str, Any]:
        """Get previous OBV value for comparison"""
        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        
        table_name = f"crypto_weekly_table{'_' + self.base if self.base != 'usd' else ''}"
        
        try:
            cur.execute(f"""
                SELECT datetime, anchored_obv, anchor_date
                FROM {table_name}
                WHERE stock = %s 
                  AND datetime < %s
                  AND anchored_obv IS NOT NULL
                ORDER BY datetime DESC
                LIMIT 1
            """, (symbol, latest_datetime))
            
            result = cur.fetchone()
            
            if result:
                return {
                    'datetime': result[0],
                    'anchored_obv': result[1],
                    'anchor_date': result[2]
                }
            return None
        finally:
            cur.close()
            conn.close()

    def _determine_alert_state(self, current_obv: float, prev_obv: float) -> str:
        """Generate alert state based on OBV crossover
        
        Returns:
            str: Alert state
                '$$$' - When OBV crosses from negative to positive
                '-$$$' - When OBV crosses from positive to negative
                '-' - No significant change
        """
        if current_obv is None or prev_obv is None:
            return '-'
            
        # Generate $$$ when crossing from negative to positive
        if current_obv > 0 and prev_obv <= 0:
            return '$$$'
        
        # Generate -$$$ when crossing from positive to negative
        if current_obv < 0 and prev_obv >= 0:
            return '-$$$'
        
        return '-'

######################################################################################

def get_transformer(source: str, db_params=None) -> BaseTransformer:
    if source.lower() == 'core_data':
        return CoreDataTransformer()
    elif source.lower() == 'daily_data':
        return DailyDataTransformer()
    elif source.lower() == 'williams_r':
        return WilliamsRTransformer(db_params)
    elif source.lower() == 'force_index':
        return ForceIndexTransformer(db_params)
    elif source.lower() == 'anchored_obv':
        return AnchoredOBVTransformer(db_params)
    else:
        raise ValueError(f"Unsupported data source: {source}")