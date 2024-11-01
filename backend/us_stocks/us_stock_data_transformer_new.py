from abc import ABC, abstractmethod
from typing import Dict, Any, List
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

################# STOCK DATA TRANSFORMER ###########################################################################
class DailyDataTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            daily_data = data.get('daily_data', [])
            
            transformed_data = []
            for day in daily_data:
                daily_point = {
                    'datetime': day['datetime'] if isinstance(day['datetime'], str) else day['datetime'].strftime('%Y-%m-%d'),
                    'stock': stock_data['symbol'],
                    'stock_name': stock_data['name'],
                    'open': self._parse_numeric(day['open']),
                    'close': self._parse_numeric(day['close']),
                    'volume': self._parse_numeric(day['volume']),
                    'high': self._parse_numeric(day['high']),
                    'low': self._parse_numeric(day['low'])
                }
                transformed_data.append(daily_point)
            
            return transformed_data
        except KeyError as e:
            print(f"Error in DailyDataTransformer: Missing key {str(e)}")
            return []
        except Exception as e:
            print(f"Error in DailyDataTransformer: {str(e)}")
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

class CoreDataTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            technical_indicator = data['technical_indicator']
            statistics = data['statistics']
            
            # Handle case where technical_indicator is empty list
            if not technical_indicator:
                return []
            
            # Handle both list and dictionary cases for technical_indicator
            tech_data = technical_indicator[0] if isinstance(technical_indicator, list) else technical_indicator
   
            transformed_data = {
                'datetime': tech_data['datetime'],
                'stock': stock_data['symbol'],
                'stock_name': stock_data['name'],
                'ema': self._parse_numeric(tech_data['ema']),
                'open': self._parse_numeric(tech_data['open']),
                'close': self._parse_numeric(tech_data['close']),
                'volume': self._parse_numeric(tech_data['volume']),
                'high': self._parse_numeric(tech_data['high']),
                'low': self._parse_numeric(tech_data['low']),
                'market_cap': self._parse_numeric(statistics['statistics']['valuations_metrics']['market_capitalization']),
                'pe_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['trailing_pe']),
                'ev_ebitda': self._parse_numeric(statistics['statistics']['valuations_metrics']['enterprise_to_ebitda']),
                'pb_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['price_to_book_mrq']),
                'peg_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['peg_ratio'])
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

################# STATISTICS DATA TRANSFORMER ###########################################################################

class StatisticsDataTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            statistics = data['statistics']
            
            transformed_data = {
                'datetime': statistics['statistics']['financials']['most_recent_quarter'],
                'stock': stock_data['symbol'],
                'sales': self._parse_numeric(statistics['statistics']['financials']['income_statement']['revenue_ttm']),
                'ebitda': self._parse_numeric(statistics['statistics']['financials']['income_statement']['ebitda']),
                'free_cash_flow': self._parse_numeric(statistics['statistics']['financials']['cash_flow']['operating_cash_flow_ttm'])
            }
            return [transformed_data]
        except KeyError as e:
            print(f"Error in StatisticsDataTransformer: Missing key {str(e)}")
            return []
        except Exception as e:
            print(f"Error in StatisticsDataTransformer: {str(e)}")
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

################# WILLIAMS R MOMENTUM ALERT TRANSFORMER & CALCULATIONS ###########################################################################

class WilliamsRTransformer(BaseTransformer):
    def __init__(self, db_connection_params, ema_length=21):
        """
        Initialize transformer with Pine Script matching EMA calculation
        ema_length: matches input(defval=21) for the EMA
        """
        self.db_connection_params = db_connection_params
        self.ema_length = ema_length

    def calculate_pine_script_ema(self, data: pd.Series, length: int) -> pd.Series:
        """Calculate EMA using Pine Script's method"""
        alpha = 2 / (length + 1)
        return data.ewm(alpha=alpha, adjust=False).mean()

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        
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
        
        cur.execute("""
            SELECT williams_r_momentum_alert_state 
            FROM us_weekly_table
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
        
        meets_criteria = williams_r > ema and williams_r > -80
        
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
        self.db_connection_params = db_connection_params
        self.base = 'usd'
        self.fast_period = fast_period
        self.slow_period = slow_period
        
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
        """
        Get previous Force Index values and alert state from database
        """
        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT 
                    force_index_7_week,
                    force_index_52_week,
                    force_index_alert_state
                FROM us_weekly_table 
                WHERE datetime < %s AND stock = %s 
                ORDER BY datetime DESC 
                LIMIT 1
            """, (latest_datetime, symbol,))
            
            result = cur.fetchone()
            
            if result:
                return {
                    'force_index_7_week': float(result[0]) if result[0] is not None else None,
                    'force_index_52_week': float(result[1]) if result[1] is not None else None,
                    'alert_state': result[2]
                }
            return None
            
        finally:
            cur.close()
            conn.close()

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')
            
            if df.empty:
                raise ValueError(f"No data available for {symbol}")
                
            latest_datetime = df['datetime'].iloc[-1]
            
            # Convert force_index to float
            df['force_index'] = df['force_index'].astype(float)
            # print(f'{df}')
            # Calculate current week's Force Index values
            force_index_7_week = self.calculate_force_index_ema(df, self.fast_period)
            force_index_52_week = self.calculate_force_index_ema(df, self.slow_period)

            # Get previous week's values from database
            prev_values = self._get_previous_values(symbol, latest_datetime)
            
            if prev_values is None:
                last_week_force_index_7_week = force_index_7_week
                last_week_force_index_52_week = force_index_52_week
                prev_alert_state = None
            else:
                last_week_force_index_7_week = prev_values['force_index_7_week']
                last_week_force_index_52_week = prev_values['force_index_52_week']
                prev_alert_state = prev_values['alert_state']

            # Ensure we have valid values before determining alert state
            if any(x is None for x in [force_index_7_week, force_index_52_week, 
                                     last_week_force_index_7_week, last_week_force_index_52_week]):
                return {
                    'datetime': None,
                    'force_index_7_week': None,
                    'force_index_52_week': None,
                    'last_week_force_index_7_week': None,
                    'last_week_force_index_52_week': None,
                    'force_index_alert_state': None
                }

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
            return None  # Return None instead of dict with NULL values

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
####################################################################################################################

def get_transformer(source: str, db_params=None) -> BaseTransformer:
    if source.lower() == 'core_data':
        return CoreDataTransformer()
    elif source.lower() == 'daily_data':
        return DailyDataTransformer()
    elif source.lower() == 'williams_r':
        return WilliamsRTransformer(db_params)
    elif source.lower() == 'force_index':
        return ForceIndexTransformer(db_params)
    elif source.lower() == 'statistics':
        return StatisticsDataTransformer()
    else:
        raise ValueError(f"Unsupported data source: {source}")