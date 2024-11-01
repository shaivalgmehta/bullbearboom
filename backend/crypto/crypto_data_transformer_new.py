from abc import ABC, abstractmethod
from typing import Dict, Any, List
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

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
                'low': self._parse_numeric(technical_indicator['low'])
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


#################################### BASE ETH TRANSFORMERS ##################################################

class CoreDataTransformerETH(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            technical_indicator = data['technical_indicator']
   
            transformed_data = {
                'datetime': technical_indicator['datetime'],
                'stock': stock_data['symbol'],
                'stock_name': stock_data['name'],
                'crypto_name': stock_data['crypto_name'],
                'ema': self._parse_numeric(technical_indicator['ema']),
                'open': self._parse_numeric(technical_indicator['open']),
                'close': self._parse_numeric(technical_indicator['close']),
                'high': self._parse_numeric(technical_indicator['high']),
                'low': self._parse_numeric(technical_indicator['low']),
                'volume': self._parse_numeric(technical_indicator['volume'])
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

class WilliamsRTransformerETH(BaseTransformer):
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')
            
            # Ensure we have at least 21 weeks of data
            if len(df) < 21:
                raise ValueError(f"Insufficient data for {symbol}. Need at least 21 weeks, got {len(df)}.")
            
            # Use only the last 21 weeks of data and create a copy
            df_last_21 = df.tail(21).copy()
            
            # Calculate the 21-week EMA of Williams %R
            df_last_21['willr_ema'] = df_last_21['willr'].ewm(span=21, adjust=False).mean()
            
            # Get the latest values
            latest_williams_r = float(df_last_21['willr'].iloc[-1])
            latest_datetime = df_last_21['datetime'].iloc[-1]
            latest_ema = float(df_last_21['willr_ema'].iloc[-1])
            latest_eth_price = float(df_last_21['eth_price'].iloc[-1])

            prev_alert_state = self._get_previous_alert_state(symbol)
            
            williams_r_momentum_alert_state = self._determine_alert_state(latest_williams_r, latest_ema, prev_alert_state)
            
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
            
    def _get_previous_alert_state(self, symbol):
        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT williams_r_momentum_alert_state 
            FROM crypto_weekly_table_eth
            WHERE stock = %s 
            ORDER BY datetime DESC 
            LIMIT 1
        """, (symbol,))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return result[0] if result else None

    def _determine_alert_state(self, williams_r, ema, prev_state):
        if williams_r is None or ema is None:
            return '-'
        
        meets_criteria = williams_r > ema and williams_r > -80
        
        if prev_state is None:
            return '$$$' if meets_criteria else '-'
        elif prev_state == '$$$':
            return '$' if meets_criteria else '-'
        elif prev_state == '$':
            return '$' if meets_criteria else '-'
        else:  # prev_state == '-'
            return '$$$' if meets_criteria else '-'

class ForceIndexTransformerETH(BaseTransformer):
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')

            latest_datetime = df['datetime'].iloc[-1]

            force_index_7_week = float(df['force_index'].ewm(span=7, adjust=False).mean().iloc[-1])
            force_index_52_week = float(df['force_index'].ewm(span=52, adjust=False).mean().iloc[-1])
            
            last_week_force_index_7_week = float(df['force_index'].ewm(span=7, adjust=False).mean().iloc[-2])
            last_week_force_index_52_week = float(df['force_index'].ewm(span=52, adjust=False).mean().iloc[-2])
            
            latest_eth_price = float(df['eth_price'].iloc[-1])
            latest_close_eth = float(df['c_eth'].iloc[-1])
            latest_volume_eth = float(df['v_eth'].iloc[-1])
            
            prev_alert_state = self._get_previous_alert_state(symbol)
            force_index_alert_state = self._determine_alert_state(force_index_7_week, force_index_52_week, 
                                                      last_week_force_index_7_week, last_week_force_index_52_week, 
                                                      prev_alert_state)
            
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

    def _get_previous_alert_state(self, symbol):
        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT force_index_alert_state 
            FROM crypto_weekly_table_eth 
            WHERE stock = %s 
            ORDER BY datetime DESC 
            LIMIT 1
        """, (symbol,))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return result[0] if result else None

    def _determine_alert_state(self, fi_7_week, fi_52_week, last_fi_7_week, last_fi_52_week, prev_state):
        upward_crossover = (last_fi_7_week <= last_fi_52_week) and (fi_7_week > fi_52_week)
        downward_crossover = (last_fi_7_week >= last_fi_52_week) and (fi_7_week < fi_52_week)
        
        if upward_crossover or downward_crossover:
            if prev_state in ['$$$', '$']:
                return '$'
            else:
                return '$$$'
        else:
            return '-'

#################################### BASE BTC TRANSFORMERS ##################################################

class CoreDataTransformerBTC(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            technical_indicator = data['technical_indicator']
   
            transformed_data = {
                'datetime': technical_indicator['datetime'],
                'stock': stock_data['symbol'],
                'stock_name': stock_data['name'],
                'crypto_name': stock_data['crypto_name'],
                'ema': self._parse_numeric(technical_indicator['ema']),
                'open': self._parse_numeric(technical_indicator['open']),
                'close': self._parse_numeric(technical_indicator['close']),
                'high': self._parse_numeric(technical_indicator['high']),                
                'low': self._parse_numeric(technical_indicator['low']),
                'volume': self._parse_numeric(technical_indicator['volume'])
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

class WilliamsRTransformerBTC(BaseTransformer):
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')
            
            # Ensure we have at least 21 weeks of data
            if len(df) < 21:
                raise ValueError(f"Insufficient data for {symbol}. Need at least 21 weeks, got {len(df)}.")
            
            # Use only the last 21 weeks of data and create a copy
            df_last_21 = df.tail(21).copy()
            
            # Calculate the 21-week EMA of Williams %R
            df_last_21['willr_ema'] = df_last_21['willr'].ewm(span=21, adjust=False).mean()
            
            # Get the latest values
            latest_williams_r = float(df_last_21['willr'].iloc[-1])
            latest_datetime = df_last_21['datetime'].iloc[-1]
            latest_ema = float(df_last_21['willr_ema'].iloc[-1])
            latest_btc_price = float(df_last_21['btc_price'].iloc[-1])

            prev_alert_state = self._get_previous_alert_state(symbol)
            
            williams_r_momentum_alert_state = self._determine_alert_state(latest_williams_r, latest_ema, prev_alert_state)
            
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
            
    def _get_previous_alert_state(self, symbol):
        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT williams_r_momentum_alert_state 
            FROM crypto_weekly_table_btc
            WHERE stock = %s 
            ORDER BY datetime DESC 
            LIMIT 1
        """, (symbol,))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return result[0] if result else None

    def _determine_alert_state(self, williams_r, ema, prev_state):
        if williams_r is None or ema is None:
            return '-'
        
        meets_criteria = williams_r > ema and williams_r > -80
        
        if prev_state is None:
            return '$$$' if meets_criteria else '-'
        elif prev_state == '$$$':
            return '$' if meets_criteria else '-'
        elif prev_state == '$':
            return '$' if meets_criteria else '-'
        else:  # prev_state == '-'
            return '$$$' if meets_criteria else '-'

class ForceIndexTransformerBTC(BaseTransformer):
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')

            latest_datetime = df['datetime'].iloc[-1]

            force_index_7_week = float(df['force_index'].ewm(span=7, adjust=False).mean().iloc[-1])
            force_index_52_week = float(df['force_index'].ewm(span=52, adjust=False).mean().iloc[-1])
            
            last_week_force_index_7_week = float(df['force_index'].ewm(span=7, adjust=False).mean().iloc[-2])
            last_week_force_index_52_week = float(df['force_index'].ewm(span=52, adjust=False).mean().iloc[-2])
            
            latest_btc_price = float(df['btc_price'].iloc[-1])
            latest_close_btc = float(df['c_btc'].iloc[-1])
            latest_volume_btc = float(df['v_btc'].iloc[-1])
            
            prev_alert_state = self._get_previous_alert_state(symbol)
            force_index_alert_state = self._determine_alert_state(force_index_7_week, force_index_52_week, 
                                                      last_week_force_index_7_week, last_week_force_index_52_week, 
                                                      prev_alert_state)
            
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

    def _get_previous_alert_state(self, symbol):
        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT force_index_alert_state 
            FROM crypto_weekly_table_btc 
            WHERE stock = %s 
            ORDER BY datetime DESC 
            LIMIT 1
        """, (symbol,))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return result[0] if result else None

    def _determine_alert_state(self, fi_7_week, fi_52_week, last_fi_7_week, last_fi_52_week, prev_state):
        upward_crossover = (last_fi_7_week <= last_fi_52_week) and (fi_7_week > fi_52_week)
        downward_crossover = (last_fi_7_week >= last_fi_52_week) and (fi_7_week < fi_52_week)
        
        if upward_crossover or downward_crossover:
            if prev_state in ['$$$', '$']:
                return '$'
            else:
                return '$$$'
        else:
            return '-'

##################################################################################

def get_transformer(source: str, db_params=None) -> BaseTransformer:
    if source.lower() == 'core_data':
        return CoreDataTransformer()
    elif source.lower() == 'daily_data':
        return DailyDataTransformer()
    elif source.lower() == 'williams_r':
        return WilliamsRTransformer(db_params)
    elif source.lower() == 'force_index':
        return ForceIndexTransformer(db_params)
    elif source.lower() == 'core_data_eth':
        return CoreDataTransformerETH()
    elif source.lower() == 'williams_r_eth':
        return WilliamsRTransformerETH(db_params)
    elif source.lower() == 'force_index_eth':
        return ForceIndexTransformerETH(db_params)
    elif source.lower() == 'core_data_btc':
        return CoreDataTransformerBTC()
    elif source.lower() == 'williams_r_btc':
        return WilliamsRTransformerBTC(db_params)
    elif source.lower() == 'force_index_btc':
        return ForceIndexTransformerBTC(db_params)
    else:
        raise ValueError(f"Unsupported data source: {source}")