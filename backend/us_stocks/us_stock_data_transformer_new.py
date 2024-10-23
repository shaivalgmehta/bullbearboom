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
            FROM crypto_weekly_table
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

class ForceIndexTransformer(BaseTransformer):
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        try:
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['t'])
            df = df.sort_values('datetime')
            
            latest_datetime = df['datetime'].iloc[-1]
            
            # Current week calculations
            force_index_7_week = float(df['force_index'].ewm(span=7, adjust=False).mean().iloc[-1])
            force_index_52_week = float(df['force_index'].ewm(span=52, adjust=False).mean().iloc[-1])
            
            # Last week calculations
            last_week_data = df[df['datetime'] < df['datetime'].iloc[-1]]  # Exclude the latest week
            last_week_force_index_7_week = float(last_week_data['force_index'].ewm(span=7, adjust=False).mean().iloc[-1])
            last_week_force_index_52_week = float(last_week_data['force_index'].ewm(span=52, adjust=False).mean().iloc[-1])
            
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
            FROM crypto_weekly_table 
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