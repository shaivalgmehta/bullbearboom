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
    def __init__(self, db_params=None):
        self.db_params = db_params

    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            technical_indicator = data['technical_indicator']
            price_changes = data.get('price_changes', {})
            
            if not technical_indicator:
                return []
            
            tech_data = technical_indicator[0] if isinstance(technical_indicator, list) else technical_indicator
            current_price = self._parse_numeric(tech_data['close'])
            
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT diluted_eps, book_value_per_share, quarterly_earnings_growth_yoy
                        FROM in_quarterly_table
                        WHERE stock = %s
                        ORDER BY datetime DESC
                        LIMIT 1
                    """, (stock_data['symbol'],))
                    quarterly_data = cur.fetchone()

            if quarterly_data and current_price:
                diluted_eps = float(quarterly_data[0]) if quarterly_data[0] else None
                book_value_per_share = float(quarterly_data[1]) if quarterly_data[1] else None
                quarterly_earnings_growth_yoy = float(quarterly_data[2]) if quarterly_data[2] else None
                
                pe_ratio = current_price / diluted_eps if diluted_eps and diluted_eps != 0 else None
                pb_ratio = current_price / book_value_per_share if book_value_per_share and book_value_per_share != 0 else None
                peg_ratio = pe_ratio / quarterly_earnings_growth_yoy if pe_ratio and quarterly_earnings_growth_yoy and quarterly_earnings_growth_yoy != 0 else None
            else:
                pe_ratio = pb_ratio = peg_ratio = None

            transformed_data = {
                'datetime': tech_data['datetime'],
                'stock': stock_data['symbol'],
                'stock_name': stock_data['name'],
                'ema': self._parse_numeric(tech_data['ema']),
                'open': self._parse_numeric(tech_data['open']),
                'close': current_price,
                'volume': self._parse_numeric(tech_data['volume']),
                'high': self._parse_numeric(tech_data['high']),
                'low': self._parse_numeric(tech_data['low']),
                'pe_ratio': pe_ratio,
                'pb_ratio': pb_ratio,
                'peg_ratio': peg_ratio,
                'price_change_3m': self._parse_numeric(price_changes.get('price_change_3m')),
                'price_change_6m': self._parse_numeric(price_changes.get('price_change_6m')),
                'price_change_12m': self._parse_numeric(price_changes.get('price_change_12m')),
                'earnings_yield': 1 / pe_ratio if pe_ratio and pe_ratio != 0 else None,
                'book_to_price': 1 / pb_ratio if pb_ratio and pb_ratio != 0 else None
            }
            return [transformed_data]
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

    def transform(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            stock_data = data['stock_data']
            statistics = data['statistics']
            cashflow = data.get('cashflow', {})
            
            # Get quarterly data
            market_cap = self._parse_numeric(statistics['statistics']['valuations_metrics']['market_capitalization'])
            sales = self._parse_numeric(statistics['statistics']['financials']['income_statement']['revenue_ttm'])
            ebitda = self._parse_numeric(statistics['statistics']['financials']['income_statement']['ebitda'])
            roe = self._parse_numeric(statistics['statistics']['financials']['return_on_equity_ttm'])
            roa = self._parse_numeric(statistics['statistics']['financials']['return_on_assets_ttm'])
            price_to_sales = self._parse_numeric(statistics['statistics']['valuations_metrics']['price_to_sales_ttm'])
            
            # New fields
            ev_ebitda = self._parse_numeric(statistics['statistics']['valuations_metrics']['enterprise_to_ebitda'])
            diluted_eps = self._parse_numeric(statistics['statistics']['financials']['income_statement']['diluted_eps_ttm'])
            book_value_per_share = self._parse_numeric(statistics['statistics']['financials']['balance_sheet']['book_value_per_share_mrq'])
            quarterly_earnings_growth_yoy = self._parse_numeric(statistics['statistics']['financials']['income_statement']['quarterly_earnings_growth_yoy'])
            
            # Process cash flow data
            try:
                if cashflow and 'cash_flow' in cashflow and len(cashflow['cash_flow']) > 0:
                    most_recent_quarter = cashflow['cash_flow'][0]
                    free_cash_flow = self._parse_numeric(most_recent_quarter.get('free_cash_flow'))
                    
                    financing = most_recent_quarter.get('financing_activities', {})
                    dividend_raw = financing.get('common_dividends')
                    repurchase_raw = financing.get('common_stock_repurchase')
                    
                    dividend_payments = abs(self._parse_numeric(dividend_raw)) if dividend_raw is not None else None
                    share_repurchases = abs(self._parse_numeric(repurchase_raw)) if repurchase_raw is not None else None
                else:
                    free_cash_flow = None
                    dividend_payments = None
                    share_repurchases = None
            except Exception as e:
                print(f"Error processing cash flow data: {str(e)}")
                free_cash_flow = None
                dividend_payments = None
                share_repurchases = None
                
            # Calculate derived metrics
            fcf_yield = ((free_cash_flow / market_cap) * 100 
                        if all(v is not None and market_cap != 0 for v in [free_cash_flow, market_cap]) 
                        else None)
            
            total_shareholder_return = ((dividend_payments or 0) + (share_repurchases or 0) 
                                      if dividend_payments is not None or share_repurchases is not None 
                                      else None)
            
            shareholder_yield = ((total_shareholder_return / market_cap) * 100 
                               if all(v is not None and market_cap != 0 for v in [total_shareholder_return, market_cap]) 
                               else None)
            
            # Get datetime
            if cashflow and 'cash_flow' in cashflow and len(cashflow['cash_flow']) > 0:
                datetime_value = cashflow['cash_flow'][0].get('fiscal_date')
            else:
                datetime_value = statistics['statistics']['financials']['most_recent_quarter']
            
            transformed_data = {
                'datetime': datetime_value,
                'stock': stock_data['symbol'],
                'sales': sales,
                'ebitda': ebitda,
                'free_cash_flow': free_cash_flow,
                'market_cap': market_cap,
                'return_on_equity': roe,
                'return_on_assets': roa,
                'price_to_sales': price_to_sales,
                'free_cash_flow_yield': fcf_yield,
                'dividend_payments': dividend_payments,
                'share_repurchases': share_repurchases,
                'shareholder_yield': shareholder_yield,
                # New fields added
                'ev_ebitda': ev_ebitda,
                'diluted_eps': diluted_eps,
                'book_value_per_share': book_value_per_share,
                'quarterly_earnings_growth_yoy': quarterly_earnings_growth_yoy
            }
            return [transformed_data]
        except KeyError as e:
            print(f"Error in StatisticsDataTransformer: Missing key {str(e)}")
            return []
        except Exception as e:
            print(f"Error in StatisticsDataTransformer: {str(e)}")
            return []

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
            FROM in_weekly_table
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
                FROM in_weekly_table 
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

############################## ANCHOR OBV CALCULATIONS #############################################################

class AnchoredOBVTransformer(BaseTransformer):
    def __init__(self, db_connection_params):
        self.db_connection_params = db_connection_params

    def transform(self, data: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
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
            print(f"Error in AnchoredOBVTransformer for {symbol}: {str(e)}")
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
        
        # Initialize OBV
        df['obv'] = 0
        
        # Calculate daily price changes
        df['price_change'] = df['c'].diff()
        
        # Calculate OBV
        for i in range(1, len(df)):
            if df['price_change'].iloc[i] > 0:
                df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1] + df['v'].iloc[i]
            elif df['price_change'].iloc[i] < 0:
                df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1] - df['v'].iloc[i]
            else:
                df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1]
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
        
        try:
            cur.execute("""
                SELECT datetime, anchored_obv, anchor_date
                FROM in_weekly_table
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


####################################################################################################################

def get_transformer(source: str, db_params=None) -> BaseTransformer:
    if source.lower() == 'core_data':
        return CoreDataTransformer(db_params)
    elif source.lower() == 'daily_data':
        return DailyDataTransformer()
    elif source.lower() == 'williams_r':
        return WilliamsRTransformer(db_params)
    elif source.lower() == 'force_index':
        return ForceIndexTransformer(db_params)
    elif source.lower() == 'statistics':
        return StatisticsDataTransformer()
    elif source.lower() == 'anchored_obv': 
        return AnchoredOBVTransformer(db_params)
    else:
        raise ValueError(f"Unsupported data source: {source}")
