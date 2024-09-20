from abc import ABC, abstractmethod
from typing import Dict, Any, List
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

class TwelveDataScreenerTransformer(BaseTransformer):
    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        stock_data = data['stock_data']
        statistics = data['statistics']
        technical_indicator = data['technical_indicator']
        williams_r_transformed_data = data['williams_r_transformed_data']
        
        transformed_data = {
            'stock': stock_data['symbol'],
            'market_cap': self._parse_numeric(statistics['statistics']['valuations_metrics']['market_capitalization']),
            'pe_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['trailing_pe']),
            'ev_ebitda': self._parse_numeric(statistics['statistics']['valuations_metrics']['enterprise_to_ebitda']),
            'pb_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['price_to_book_mrq']),
            'peg_ratio': self._parse_numeric(statistics['statistics']['valuations_metrics']['peg_ratio']),
            'current_year_sales': self._parse_numeric(statistics['statistics']['financials']['income_statement']['revenue_ttm']),
            'current_year_ebitda': self._parse_numeric(statistics['statistics']['financials']['income_statement']['ebitda']),
            'ema': self._parse_numeric(technical_indicator[0]['ema']),
            'williams_r': self._parse_numeric(williams_r_transformed_data['williams_r']),
            'williams_r_ema': self._parse_numeric(williams_r_transformed_data['williams_r_ema']),
            'alert_state': williams_r_transformed_data['alert_state']
        }
        return [transformed_data]

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
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params

    def transform(self, data: List[Dict[str, Any]],symbol: str) -> Dict[str, Any]:
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
        alert_state = self._determine_alert_state(latest_williams_r, ema, prev_alert_state)
        print(f"Previous Alert State: {prev_alert_state}")
        print(f"New Alert State: {alert_state}")
        return {
            'datetime': latest_datetime,
            'williams_r': latest_williams_r,
            'williams_r_ema': ema,
            'alert_state': alert_state
        }

    def _get_previous_alert_state(self, symbol):
        conn = psycopg2.connect(**self.db_connection_params)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT alert_state 
            FROM williams_r_table 
            WHERE stock = %s 
            ORDER BY time DESC 
            LIMIT 1
        """, (symbol,))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return result[0] if result else None

    def _determine_alert_state(self, williams_r, ema, prev_state):
        if williams_r is None or ema is None:
            return 'INACTIVE'
        
        meets_criteria = williams_r > ema and williams_r > -80
        
        if prev_state is None:
            return 'TRIGGERED' if meets_criteria else 'INACTIVE'
        elif prev_state == 'TRIGGERED':
            return 'ACTIVE' if meets_criteria else 'INACTIVE'
        elif prev_state == 'ACTIVE':
            return 'ACTIVE' if meets_criteria else 'INACTIVE'
        else:  # prev_state == 'INACTIVE'
            return 'TRIGGERED' if meets_criteria else 'INACTIVE'

def get_transformer(source: str, db_params=None) -> BaseTransformer:
    if source.lower() == 'twelvedata_screener':
        return TwelveDataScreenerTransformer()
    elif source.lower() == 'williams_r':
        return WilliamsRTransformer(db_params)
    else:
        raise ValueError(f"Unsupported data source: {source}")