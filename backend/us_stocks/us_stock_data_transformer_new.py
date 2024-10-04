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

class CoreDataTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            technical_indicator = data['technical_indicator']
            statistics = data['statistics']
   
            transformed_data = {
                'datetime': technical_indicator[0]['datetime'],
                'stock': stock_data['symbol'],
                'stock_name': stock_data['name'],
                'ema': self._parse_numeric(technical_indicator[0]['ema']),
                'open': self._parse_numeric(technical_indicator[0]['open']),
                'close': self._parse_numeric(technical_indicator[0]['close']),
                'volume': self._parse_numeric(technical_indicator[0]['volume']),
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
                'ebitda': self._parse_numeric(statistics['statistics']['financials']['income_statement']['ebitda'])
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

    def transform(self, data: List[Dict[str, Any]],symbol: str) -> Dict[str, Any]:
        try:
            # Sort data by datetime to ensure it's in the correct order
            sorted_data = sorted(data, key=lambda x: x['datetime'], reverse=True)
            
            # Take the last 21 values (or all if less than 21)
            recent_data = sorted_data[:21]
            williams_r_values = [float(value['willr']) for value in recent_data]

            # Calculate EMA only if we have enough data points
            if len(williams_r_values) == 21:
                ema = float(pd.Series(williams_r_values).ewm(span=21, adjust=False).mean().iloc[-1])
            else:
                ema = None  # or you could use a simple average if preferred
            
            # Get the most recent (today's) Williams %R value
            latest_williams_r = williams_r_values[0] if williams_r_values else None
            latest_datetime = recent_data[0]['datetime'] if recent_data else None

             # Fetch the previous alert state
            prev_alert_state = self._get_previous_alert_state(symbol)
            
            # Determine the alert state
            williams_r_momentum_alert_state = self._determine_alert_state(latest_williams_r, ema, prev_alert_state)
            # print(f"Previous Alert State: {prev_alert_state}")
            # print(f"New Alert State: {williams_r_momentum_alert_state}")
            return {
                'datetime': latest_datetime,
                'williams_r': latest_williams_r,
                'williams_r_ema': ema,
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
            FROM us_weekly_table
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


################# FORCE INDEX ALERT TRANSFORMER & CALCULATIONS ###########################################################################

class ForceIndexTransformer(BaseTransformer):
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
        try:
             # Convert to DataFrame and sort by date
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime')  # Sort in ascending order

            # Get the most recent datetime
            latest_datetime = df['datetime'].iloc[-1]

            # Convert relevant columns to float
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)

            # Calculate Force Index
            df['force_index'] = (df['close'] - df['close'].shift(1)) * df['volume']
            
             # Calculate 7-week and 52-week EWMs
            force_index_7_week = df['force_index'].ewm(span=7, adjust=False).mean().iloc[-1]
            force_index_52_week = df['force_index'].ewm(span=52, adjust=False).mean().iloc[-1]
            
            # Calculate last week's EWMs
            last_week_force_index_7_week = df['force_index'].ewm(span=7, adjust=False).mean().iloc[-2]
            last_week_force_index_52_week = df['force_index'].ewm(span=52, adjust=False).mean().iloc[-2]
            
            # Determine alert state
            prev_alert_state = self._get_previous_alert_state(symbol)
            force_index_alert_state = self._determine_alert_state(force_index_7_week, force_index_52_week, 
                                                      last_week_force_index_7_week, last_week_force_index_52_week, 
                                                      prev_alert_state)
            
            return {
                'datetime': latest_datetime,
                'force_index_7_week': float(force_index_7_week),
                'force_index_52_week': float(force_index_52_week),
                'last_week_force_index_7_week': float(last_week_force_index_7_week),
                'last_week_force_index_52_week': float(last_week_force_index_52_week),
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
            FROM us_weekly_table 
            WHERE stock = %s 
            ORDER BY datetime DESC 
            LIMIT 1
        """, (symbol,))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return result[0] if result else None

    def _determine_alert_state(self, fi_7_week, fi_52_week, last_fi_7_week, last_fi_52_week, prev_state):
        # Check for upward crossover
        upward_crossover = (last_fi_7_week <= last_fi_52_week) and (fi_7_week > fi_52_week)
        
        # Check for downward crossover
        downward_crossover = (last_fi_7_week >= last_fi_52_week) and (fi_7_week < fi_52_week)
        
        if upward_crossover or downward_crossover:
            if prev_state in ['$$$', '$']:
                return '$'  # Continue the alert state if it was already $$$ or $
            else:
                return '$$$'  # New crossover (either direction)
        else:
            return '-'  # No crossover

####################################################################################################################

def get_transformer(source: str, db_params=None) -> BaseTransformer:
    if source.lower() == 'core_data':
        return CoreDataTransformer()
    elif source.lower() == 'williams_r':
        return WilliamsRTransformer(db_params)
    elif source.lower() == 'force_index':
        return ForceIndexTransformer(db_params)
    elif source.lower() == 'statistics':
        return StatisticsDataTransformer()
    else:
        raise ValueError(f"Unsupported data source: {source}")