from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from decimal import Decimal
from datetime import datetime
import logging
from us_stocks.us_stock_screener_table_process import update_us_screener_table
from in_stocks.in_stock_screener_table_process import update_in_screener_table
from crypto.crypto_screener_table_process import update_screener_table_usd
from crypto.crypto_screener_table_process_btc import update_screener_table_btc
from crypto.crypto_screener_table_process_eth import update_screener_table_eth
import numpy as np
from typing import Dict, List

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(filename='app.log', level=logging.DEBUG)

app = Flask(__name__)
CORS(app)

# Database connection details
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

app.json_encoder = CustomJSONEncoder

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise


########### US STOCK APIS #############################################
@app.route('/api/stocks/latest')
def get_latest_stock_data():
    logging.info("Fetching latest data for all stocks")
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 100))
        sort_column = request.args.get('sortColumn', 'datetime')
        sort_direction = request.args.get('sortDirection', 'DESC')
        
        # Get date parameter
        date_str = request.args.get('date')
        if date_str:
            selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            selected_date = datetime.now().date() - timedelta(days=1)

        # Get filter parameters
        filters = {}
        text_filters = ['stock', 'stock_name']
        numeric_filters = [
            'market_cap', 'pe_ratio', 'ev_ebitda', 'pb_ratio', 'peg_ratio',
            'current_quarter_sales', 'current_quarter_ebitda', 'ema',
            'price_change_3m', 'price_change_6m', 'price_change_12m',
            'earnings_yield', 'book_to_price',
            'return_on_equity', 'return_on_assets', 'price_to_sales',
            'free_cash_flow_yield', 'shareholder_yield',
            'pe_ratio_rank', 'ev_ebitda_rank', 'pb_ratio_rank', 'peg_ratio_rank',
            'earnings_yield_rank', 'book_to_price_rank', 'erp5_rank'
        ]
        alert_filters = [
            'williams_r_momentum_alert_state',
            'force_index_alert_state',
            'anchored_obv_alert_state'
        ]

        # Process text filters
        for column in text_filters:
            value = request.args.get(column)
            if value:
                filters[column] = value.lower()

        # Process numeric filters
        for column in numeric_filters:
            min_value = request.args.get(f'min_{column}')
            max_value = request.args.get(f'max_{column}')
            if min_value or max_value:
                filters[column] = {
                    'min': float(min_value) if min_value else None,
                    'max': float(max_value) if max_value else None
                }

        # Process alert state filters
        for column in alert_filters:
            values = request.args.getlist(f'{column}[]')
            if values:
                filters[column] = values

        # Calculate offset
        offset = (page - 1) * page_size

        # Build the WHERE clause dynamically
        where_conditions = ["DATE(datetime) = DATE(%s)"]
        params = [selected_date]

        # Add text filters
        for column, value in filters.items():
            if column in text_filters:
                where_conditions.append(f"LOWER({column}) LIKE %s")
                params.append(f"%{value}%")
            elif column in numeric_filters and isinstance(value, dict):
                if value.get('min') is not None:
                    where_conditions.append(f"{column} >= %s")
                    params.append(value['min'])
                if value.get('max') is not None:
                    where_conditions.append(f"{column} <= %s")
                    params.append(value['max'])
            elif column in alert_filters:
                if value:
                    placeholders = ','.join(['%s'] * len(value))
                    where_conditions.append(f"{column} IN ({placeholders})")
                    params.extend(value)

        where_clause = " AND ".join(where_conditions)

        # Update screener table for the selected date
        update_us_screener_table(selected_date)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Get total count
        count_query = f"""
            SELECT COUNT(*) 
            FROM us_screener_table 
            WHERE {where_clause}
        """
        cur.execute(count_query, params)
        total_count = cur.fetchone()['count']
        
        # Get paginated data
        query = f"""
            SELECT *
            FROM us_screener_table
            WHERE {where_clause}
            ORDER BY {sort_column} {sort_direction}
            LIMIT %s OFFSET %s
        """
        cur.execute(query, params + [page_size, offset])
        data = cur.fetchall()
        
        response = {
            'data': data,
            'totalCount': total_count,
            'page': page,
            'pageSize': page_size,
            'totalPages': -(-total_count // page_size)  # Ceiling division
        }
        
        logging.info(f"Fetched page {page} of stock data for date {selected_date}")
        cur.close()
        conn.close()
        
        return jsonify(response)
        
    except Exception as e:
        logging.error(f"Error fetching stock data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stocks/alerts')
def get_alerts_data():
    logging.info("Fetching alerts for all stocks")
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = """
                    SELECT 
                        datetime,
                        stock,
                        stock_name,
                        alerts
                    FROM us_alerts_table
                    WHERE datetime >= NOW() - INTERVAL '30 days'
                    ORDER BY datetime DESC
                """
                
                cur.execute(query)
                data = cur.fetchall()
                
                # Process the data to ensure alerts is properly parsed from JSON
                processed_data = []
                for row in data:
                    # Ensure alerts is in the correct format (parsed JSON)
                    if isinstance(row['alerts'], str):
                        try:
                            row['alerts'] = json.loads(row['alerts'])
                        except json.JSONDecodeError:
                            row['alerts'] = []
                    
                    processed_data.append(row)
                
                logging.info(f"Fetched {len(processed_data)} alerts from the last 30 days")
                return jsonify(processed_data)
                
    except Exception as e:
        logging.error(f"Error fetching alerts data: {e}")
        return jsonify({"error": "Failed to fetch alerts data"}), 500

@app.route('/api/stocks/<symbol>/historical')
def get_stock_historical_data(symbol):
    logging.info(f"Fetching historical data for stock {symbol}")
    try:
        # Get date parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query for technical indicators (existing)
        cur.execute("""
            SELECT 
                datetime,
                force_index_7_week,
                force_index_52_week,
                williams_r,
                williams_r_ema
            FROM us_weekly_table
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        technical_data = cur.fetchall()
        
        # Query for price history
        cur.execute("""
            SELECT 
                datetime,
                open,
                high,
                low,
                close,
                volume,
                ema
            FROM us_daily_table
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        price_history = cur.fetchall()

        # Get latest daily data
        cur.execute("""
            SELECT 
                datetime,
                close,
                market_cap,
                pe_ratio,
                ev_ebitda,
                pb_ratio,
                peg_ratio,
                price_change_3m,
                price_change_6m,
                price_change_12m,
                ema,
                volume,
                high,
                low,
                open
            FROM us_daily_table
            WHERE stock = %s
            ORDER BY datetime DESC
            LIMIT 1
        """, (symbol,))
        
        current_data = cur.fetchone()
        
        # Also fetch basic stock info (existing)
        cur.execute("""
            SELECT DISTINCT stock_name
            FROM us_daily_table
            WHERE stock = %s
            LIMIT 1
        """, (symbol,))
        stock_info = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not technical_data and not price_history:
            return jsonify({
                "error": f"No data found for stock {symbol}"
            }), 404
            
        return jsonify({
            "symbol": symbol,
            "stock_name": stock_info['stock_name'] if stock_info else None,
            "current_data": current_data,
            "technical_data": technical_data,
            "price_history": price_history
        })
        
    except Exception as e:
        logging.error(f"Error fetching historical data for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500

# ########### IN STOCK APIS #############################################
@app.route('/api/in_stocks/latest')
def get_latest_in_stock_data():
    logging.info("Fetching latest data for all stocks")
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 100))
        sort_column = request.args.get('sortColumn', 'datetime')
        sort_direction = request.args.get('sortDirection', 'DESC')
        
        # Get date parameter
        date_str = request.args.get('date')
        if date_str:
            selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            selected_date = datetime.now().date() - timedelta(days=1)

        # Get filter parameters
        filters = {}
        text_filters = ['stock', 'stock_name']
        numeric_filters = [
            'market_cap', 'pe_ratio', 'ev_ebitda', 'pb_ratio', 'peg_ratio',
            'current_quarter_sales', 'current_quarter_ebitda', 'ema',
            'price_change_3m', 'price_change_6m', 'price_change_12m',
            'earnings_yield', 'book_to_price',
            'return_on_equity', 'return_on_assets', 'price_to_sales',
            'free_cash_flow_yield', 'shareholder_yield',
            'pe_ratio_rank', 'ev_ebitda_rank', 'pb_ratio_rank', 'peg_ratio_rank',
            'earnings_yield_rank', 'book_to_price_rank', 'erp5_rank'
        ]
        alert_filters = [
            'williams_r_momentum_alert_state',
            'force_index_alert_state',
            'anchored_obv_alert_state'
        ]

        # Process text filters
        for column in text_filters:
            value = request.args.get(column)
            if value:
                filters[column] = value.lower()

        # Process numeric filters
        for column in numeric_filters:
            min_value = request.args.get(f'min_{column}')
            max_value = request.args.get(f'max_{column}')
            if min_value or max_value:
                filters[column] = {
                    'min': float(min_value) if min_value else None,
                    'max': float(max_value) if max_value else None
                }

        # Process alert state filters
        for column in alert_filters:
            values = request.args.getlist(f'{column}[]')
            if values:
                filters[column] = values

        # Calculate offset
        offset = (page - 1) * page_size

        # Build the WHERE clause dynamically
        where_conditions = ["DATE(datetime) = DATE(%s)"]
        params = [selected_date]

        # Add text filters
        for column, value in filters.items():
            if column in text_filters:
                where_conditions.append(f"LOWER({column}) LIKE %s")
                params.append(f"%{value}%")
            elif column in numeric_filters and isinstance(value, dict):
                if value.get('min') is not None:
                    where_conditions.append(f"{column} >= %s")
                    params.append(value['min'])
                if value.get('max') is not None:
                    where_conditions.append(f"{column} <= %s")
                    params.append(value['max'])
            elif column in alert_filters:
                if value:
                    placeholders = ','.join(['%s'] * len(value))
                    where_conditions.append(f"{column} IN ({placeholders})")
                    params.extend(value)

        where_clause = " AND ".join(where_conditions)

        # Update screener table for the selected date
        update_in_screener_table(selected_date)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Get total count
        count_query = f"""
            SELECT COUNT(*) 
            FROM in_screener_table 
            WHERE {where_clause}
        """
        cur.execute(count_query, params)
        total_count = cur.fetchone()['count']
        
        # Get paginated data
        query = f"""
            SELECT *
            FROM in_screener_table
            WHERE {where_clause}
            ORDER BY {sort_column} {sort_direction}
            LIMIT %s OFFSET %s
        """
        cur.execute(query, params + [page_size, offset])
        data = cur.fetchall()
        
        response = {
            'data': data,
            'totalCount': total_count,
            'page': page,
            'pageSize': page_size,
            'totalPages': -(-total_count // page_size)  # Ceiling division
        }
        
        logging.info(f"Fetched page {page} of stock data for date {selected_date}")
        cur.close()
        conn.close()
        
        return jsonify(response)
        
    except Exception as e:
        logging.error(f"Error fetching stock data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/in_stocks/alerts')
def get_in_alerts_data():
    logging.info("Fetching alerts for all Indian stocks")
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = """
                    SELECT 
                        datetime,
                        stock,
                        stock_name,
                        alerts
                    FROM in_alerts_table
                    WHERE datetime >= NOW() - INTERVAL '30 days'
                    ORDER BY datetime DESC
                """
                
                cur.execute(query)
                data = cur.fetchall()
                
                # Process the data to ensure alerts is properly parsed from JSON
                processed_data = []
                for row in data:
                    # Ensure alerts is in the correct format (parsed JSON)
                    if isinstance(row['alerts'], str):
                        try:
                            row['alerts'] = json.loads(row['alerts'])
                        except json.JSONDecodeError:
                            row['alerts'] = []
                    
                    processed_data.append(row)
                
                logging.info(f"Fetched {len(processed_data)} alerts for Indian stocks from the last 30 days")
                return jsonify(processed_data)
                
    except Exception as e:
        logging.error(f"Error fetching India alerts data: {e}")
        return jsonify({"error": "Failed to fetch India alerts data"}), 500

@app.route('/api/in_stocks/<symbol>/historical')
def get_in_stock_historical_data(symbol):
    logging.info(f"Fetching historical data for stock {symbol}")
    try:
        # Get date parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query for technical indicators (existing)
        cur.execute("""
            SELECT 
                datetime,
                force_index_7_week,
                force_index_52_week,
                williams_r,
                williams_r_ema
            FROM in_weekly_table
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        technical_data = cur.fetchall()
        
        # Query for price history
        cur.execute("""
            SELECT 
                datetime,
                open,
                high,
                low,
                close,
                volume,
                ema
            FROM in_daily_table
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        price_history = cur.fetchall()

        # Get latest daily data
        cur.execute("""
            SELECT 
                datetime,
                close,
                market_cap,
                pe_ratio,
                ev_ebitda,
                pb_ratio,
                peg_ratio,
                price_change_3m,
                price_change_6m,
                price_change_12m,
                ema,
                volume,
                high,
                low,
                open
            FROM in_daily_table
            WHERE stock = %s
            ORDER BY datetime DESC
            LIMIT 1
        """, (symbol,))
        
        current_data = cur.fetchone()
        
        # Also fetch basic stock info (existing)
        cur.execute("""
            SELECT DISTINCT stock_name
            FROM in_daily_table
            WHERE stock = %s
            LIMIT 1
        """, (symbol,))
        stock_info = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not technical_data and not price_history:
            return jsonify({
                "error": f"No data found for stock {symbol}"
            }), 404
            
        return jsonify({
            "symbol": symbol,
            "stock_name": stock_info['stock_name'] if stock_info else None,
            "current_data": current_data,
            "technical_data": technical_data,
            "price_history": price_history
        })
        
    except Exception as e:
        logging.error(f"Error fetching historical data for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500


################## CRYPTO APIS #############################################
@app.route('/api/crypto/latest')
def get_latest_crypto_data():
    logging.info("Fetching latest data for all stocks")
    try:
        # Get date parameter, default to yesterday if not provided
        date_str = request.args.get('date')
        if date_str:
            selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            selected_date = datetime.now().date() - timedelta(days=1)
        # print(f{selected_date})
        # Update screener table for the selected date
        update_screener_table_usd(selected_date)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT *
            FROM crypto_screener_table
            WHERE DATE(datetime) = %s
            ORDER BY stock
        """
        
        cur.execute(query, (selected_date,))
        data = cur.fetchall()
        
        logging.info(f"Fetched data for {len(data)} crypto for date {selected_date}")
        cur.close()
        conn.close()
        
        return jsonify(data)
    except Exception as e:
        logging.error(f"Error fetching crypto data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/latest_eth')
def get_latest_crypto_eth_data():
    logging.info("Fetching latest data for all stocks")
    try:
        # Get date parameter, default to yesterday if not provided
        date_str = request.args.get('date')
        if date_str:
            selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            selected_date = datetime.now().date() - timedelta(days=1)
        # print(f{selected_date})
        # Update screener table for the selected date
        update_screener_table_usd(selected_date)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT *
            FROM crypto_screener_table_eth
            WHERE DATE(datetime) = %s
            ORDER BY stock
        """
        
        cur.execute(query, (selected_date,))
        data = cur.fetchall()
        
        logging.info(f"Fetched data for {len(data)} crypto for date {selected_date}")
        cur.close()
        conn.close()
        
        return jsonify(data)
    except Exception as e:
        logging.error(f"Error fetching crypto data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/latest_btc')
def get_latest_crypto_btc_data():
    logging.info("Fetching latest data for all stocks")
    try:
        # Get date parameter, default to yesterday if not provided
        date_str = request.args.get('date')
        if date_str:
            selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            selected_date = datetime.now().date() - timedelta(days=1)
        # print(f{selected_date})
        # Update screener table for the selected date
        update_screener_table_usd(selected_date)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT *
            FROM crypto_screener_table_btc
            WHERE DATE(datetime) = %s
            ORDER BY stock
        """
        
        cur.execute(query, (selected_date,))
        data = cur.fetchall()
        
        logging.info(f"Fetched data for {len(data)} crypto for date {selected_date}")
        cur.close()
        conn.close()
        
        return jsonify(data)
    except Exception as e:
        logging.error(f"Error fetching crypto data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/alerts', methods=['GET'])
def get_crypto_alerts():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get the last 10 days of alerts
        cur.execute("""
            SELECT 
                datetime,
                stock,
                crypto_name,
                alerts
            FROM crypto_alerts_table
            WHERE datetime >= CURRENT_DATE - INTERVAL '10 days'
            ORDER BY datetime DESC
        """)
        
        alerts = cur.fetchall()
        
        # Process the data to ensure alerts is properly parsed from JSON
        processed_alerts = []
        for row in alerts:
            # Ensure alerts is in the correct format (parsed JSON)
            if isinstance(row['alerts'], str):
                try:
                    row['alerts'] = json.loads(row['alerts'])
                except json.JSONDecodeError:
                    row['alerts'] = []
            
            processed_alerts.append(row)
        
        cur.close()
        conn.close()
        
        return jsonify(processed_alerts)
    except Exception as e:
        logging.error(f"Error fetching crypto alerts: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/alerts_eth', methods=['GET'])
def get_crypto_alerts_eth():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get the last 10 days of ETH-based alerts
        cur.execute("""
            SELECT 
                datetime,
                stock,
                crypto_name,
                alerts
            FROM crypto_alerts_table_eth
            WHERE datetime >= CURRENT_DATE - INTERVAL '10 days'
            ORDER BY datetime DESC
        """)
        
        alerts = cur.fetchall()
        
        # Process the data to ensure alerts is properly parsed from JSON
        processed_alerts = []
        for row in alerts:
            # Ensure alerts is in the correct format (parsed JSON)
            if isinstance(row['alerts'], str):
                try:
                    row['alerts'] = json.loads(row['alerts'])
                except json.JSONDecodeError:
                    row['alerts'] = []
            
            processed_alerts.append(row)
        
        cur.close()
        conn.close()
        
        return jsonify(processed_alerts)
    except Exception as e:
        logging.error(f"Error fetching ETH-based crypto alerts: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/alerts_btc', methods=['GET'])
def get_crypto_alerts_btc():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get the last 10 days of BTC-based alerts
        cur.execute("""
            SELECT 
                datetime,
                stock,
                crypto_name,
                alerts
            FROM crypto_alerts_table_btc
            WHERE datetime >= CURRENT_DATE - INTERVAL '10 days'
            ORDER BY datetime DESC
        """)
        
        alerts = cur.fetchall()
        
        # Process the data to ensure alerts is properly parsed from JSON
        processed_alerts = []
        for row in alerts:
            # Ensure alerts is in the correct format (parsed JSON)
            if isinstance(row['alerts'], str):
                try:
                    row['alerts'] = json.loads(row['alerts'])
                except json.JSONDecodeError:
                    row['alerts'] = []
            
            processed_alerts.append(row)
        
        cur.close()
        conn.close()
        
        return jsonify(processed_alerts)
    except Exception as e:
        logging.error(f"Error fetching BTC-based crypto alerts: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/<symbol>/historical')
def get_crypto_historical_data(symbol):
    logging.info(f"Fetching historical data for crypto {symbol} (USD base)")
    try:
        # Get date parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query for technical indicators
        cur.execute("""
            SELECT 
                datetime,
                force_index_7_week,
                force_index_52_week,
                williams_r,
                williams_r_ema
            FROM crypto_weekly_table
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        technical_data = cur.fetchall()
        
        # Query for price history
        cur.execute("""
            SELECT 
                datetime,
                open,
                high,
                low,
                close,
                volume,
                ema
            FROM crypto_daily_table
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        price_history = cur.fetchall()

        # Get latest daily data
        cur.execute("""
            SELECT 
                datetime,
                close,
                volume,
                ema,
                high,
                low,
                open,
                all_time_high,
                ath_percentage,
                price_change_3m,
                price_change_6m,
                price_change_12m
            FROM crypto_daily_table
            WHERE stock = %s
            ORDER BY datetime DESC
            LIMIT 1
        """, (symbol,))
        
        current_data = cur.fetchone()
        
        # Also fetch basic crypto info
        cur.execute("""
            SELECT DISTINCT stock_name, crypto_name
            FROM crypto_daily_table
            WHERE stock = %s
            LIMIT 1
        """, (symbol,))
        crypto_info = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not technical_data and not price_history:
            return jsonify({
                "error": f"No data found for crypto {symbol}"
            }), 404
            
        return jsonify({
            "symbol": symbol,
            "stock_name": crypto_info['stock_name'] if crypto_info else None,
            "crypto_name": crypto_info['crypto_name'] if crypto_info else None,
            "current_data": current_data,
            "technical_data": technical_data,
            "price_history": price_history
        })
        
    except Exception as e:
        logging.error(f"Error fetching historical data for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/<symbol>/historical_eth')
def get_crypto_historical_data_eth(symbol):
    logging.info(f"Fetching historical data for crypto {symbol} (ETH base)")
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query for technical indicators
        cur.execute("""
            SELECT 
                datetime,
                force_index_7_week,
                force_index_52_week,
                williams_r,
                williams_r_ema
            FROM crypto_weekly_table_eth
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        technical_data = cur.fetchall()
        
        # Query for price history
        cur.execute("""
            SELECT 
                datetime,
                open,
                high,
                low,
                close,
                volume,
                ema
            FROM crypto_daily_table_eth
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        price_history = cur.fetchall()

        # Get latest daily data
        cur.execute("""
            SELECT 
                datetime,
                close,
                volume,
                ema,
                high,
                low,
                open,
                all_time_high,
                ath_percentage,
                price_change_3m,
                price_change_6m,
                price_change_12m
            FROM crypto_daily_table_eth
            WHERE stock = %s
            ORDER BY datetime DESC
            LIMIT 1
        """, (symbol,))
        
        current_data = cur.fetchone()
        
        # Also fetch basic crypto info
        cur.execute("""
            SELECT DISTINCT stock_name, crypto_name
            FROM crypto_daily_table_eth
            WHERE stock = %s
            LIMIT 1
        """, (symbol,))
        crypto_info = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not technical_data and not price_history:
            return jsonify({
                "error": f"No data found for crypto {symbol} (ETH base)"
            }), 404
            
        return jsonify({
            "symbol": symbol,
            "stock_name": crypto_info['stock_name'] if crypto_info else None,
            "crypto_name": crypto_info['crypto_name'] if crypto_info else None,
            "current_data": current_data,
            "technical_data": technical_data,
            "price_history": price_history
        })
        
    except Exception as e:
        logging.error(f"Error fetching historical ETH base data for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/<symbol>/historical_btc')
def get_crypto_historical_data_btc(symbol):
    logging.info(f"Fetching historical data for crypto {symbol} (BTC base)")
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query for technical indicators
        cur.execute("""
            SELECT 
                datetime,
                force_index_7_week,
                force_index_52_week,
                williams_r,
                williams_r_ema
            FROM crypto_weekly_table_btc
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        technical_data = cur.fetchall()
        
        # Query for price history
        cur.execute("""
            SELECT 
                datetime,
                open,
                high,
                low,
                close,
                volume,
                ema
            FROM crypto_daily_table_btc
            WHERE stock = %s
            AND datetime BETWEEN %s AND %s
            ORDER BY datetime ASC
        """, (symbol, start_date, end_date))
        
        price_history = cur.fetchall()

        # Get latest daily data
        cur.execute("""
            SELECT 
                datetime,
                close,
                volume,
                ema,
                high,
                low,
                open,
                all_time_high,
                ath_percentage,
                price_change_3m,
                price_change_6m,
                price_change_12m
            FROM crypto_daily_table_btc
            WHERE stock = %s
            ORDER BY datetime DESC
            LIMIT 1
        """, (symbol,))
        
        current_data = cur.fetchone()
        
        # Also fetch basic crypto info
        cur.execute("""
            SELECT DISTINCT stock_name, crypto_name
            FROM crypto_daily_table_btc
            WHERE stock = %s
            LIMIT 1
        """, (symbol,))
        crypto_info = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not technical_data and not price_history:
            return jsonify({
                "error": f"No data found for crypto {symbol} (BTC base)"
            }), 404
            
        return jsonify({
            "symbol": symbol,
            "stock_name": crypto_info['stock_name'] if crypto_info else None,
            "crypto_name": crypto_info['crypto_name'] if crypto_info else None,
            "current_data": current_data,
            "technical_data": technical_data,
            "price_history": price_history
        })
        
    except Exception as e:
        logging.error(f"Error fetching historical BTC base data for {symbol}: {e}")
        return jsonify({"error": str(e)}), 500

############## US INSIDER TRADING API ##############################################
@app.route('/api/stocks/insider')
def get_insider_trading_data():
    logging.info("Fetching insider trading data")
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 100))
        sort_column = request.args.get('sortColumn', 'datetime')
        sort_direction = request.args.get('sortDirection', 'DESC')
        
        # Get filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        insider_name_filter = request.args.get('insider_name')
        stock_filter = request.args.get('stock')
        transaction_types = request.args.getlist('transaction_type[]')
        min_shares = request.args.get('min_shares')
        max_shares = request.args.get('max_shares')
        min_total_value = request.args.get('min_total_value')
        max_total_value = request.args.get('max_total_value')
        min_one_month_return = request.args.get('min_one_month_return')
        max_one_month_return = request.args.get('max_one_month_return')
        min_three_month_return = request.args.get('min_three_month_return')
        max_three_month_return = request.args.get('max_three_month_return')
        
        # Calculate offset
        offset = (page - 1) * page_size

        # Build the WHERE clause dynamically
        where_conditions = []
        params = []

        if start_date and end_date:
            where_conditions.append("DATE(datetime) BETWEEN %s AND %s")
            params.extend([start_date, end_date])
        
        if insider_name_filter:
            where_conditions.append("LOWER(insider_name) LIKE %s")
            params.append(f"%{insider_name_filter.lower()}%")
            
        if stock_filter:
            where_conditions.append("LOWER(stock) LIKE %s")
            params.append(f"%{stock_filter.lower()}%")
            
        if transaction_types:
            placeholders = ','.join(['%s'] * len(transaction_types))
            where_conditions.append(f"transaction_type IN ({placeholders})")
            params.extend(transaction_types)
            
        # Add numeric range filters
        if min_shares:
            where_conditions.append("shares_traded >= %s")
            params.append(float(min_shares))
        if max_shares:
            where_conditions.append("shares_traded <= %s")
            params.append(float(max_shares))
            
        if min_total_value:
            where_conditions.append("total_value >= %s")
            params.append(float(min_total_value))
        if max_total_value:
            where_conditions.append("total_value <= %s")
            params.append(float(max_total_value))
            
        if min_one_month_return:
            where_conditions.append("one_month_return >= %s")
            params.append(float(min_one_month_return))
        if max_one_month_return:
            where_conditions.append("one_month_return <= %s")
            params.append(float(max_one_month_return))
            
        if min_three_month_return:
            where_conditions.append("three_month_return >= %s")
            params.append(float(min_three_month_return))
        if max_three_month_return:
            where_conditions.append("three_month_return <= %s")
            params.append(float(max_three_month_return))

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Get total count for pagination
        count_query = f"""
            SELECT COUNT(*) 
            FROM us_insider_trading_table 
            WHERE {where_clause}
        """
        cur.execute(count_query, params)
        total_count = cur.fetchone()['count']
        
        # Get paginated and filtered data
        query = f"""
            SELECT 
                datetime,
                stock,
                stock_name,
                insider_name,
                transaction_type,
                relationship_is_director,
                relationship_is_officer,
                relationship_is_ten_percent_owner,
                relationship_is_other,
                shares_traded,
                price_per_share,
                total_value,
                shares_owned_following,
                one_month_price,
                three_month_price,
                one_month_return,
                three_month_return
            FROM us_insider_trading_table
            WHERE {where_clause}
            ORDER BY {sort_column} {sort_direction}
            LIMIT %s OFFSET %s
        """
        params.extend([page_size, offset])
        cur.execute(query, params)
        data = cur.fetchall()
        
        response = {
            'data': data,
            'totalCount': total_count,
            'page': page,
            'pageSize': page_size,
            'totalPages': -(-total_count // page_size)  # Ceiling division
        }
        
        logging.info(f"Fetched page {page} of insider trading records")
        cur.close()
        conn.close()
        
        return jsonify(response)
        
    except Exception as e:
        logging.error(f"Error fetching insider trading data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/stocks/insider/stats/<insider_name>')
def get_insider_stats(insider_name):
    logging.info(f"Fetching statistics for insider: {insider_name}")
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Get summary statistics
        stats_query = """
            SELECT 
                COUNT(*) FILTER (WHERE transaction_type = 'P') as total_purchases,
                COUNT(*) FILTER (WHERE transaction_type = 'P' AND one_month_return > 0) as one_month_wins,
                AVG(one_month_return) FILTER (WHERE transaction_type = 'P') as avg_one_month_return,
                COUNT(*) FILTER (WHERE transaction_type = 'P' AND three_month_return > 0) as three_month_wins,
                AVG(three_month_return) FILTER (WHERE transaction_type = 'P') as avg_three_month_return
            FROM us_insider_trading_table
            WHERE insider_name = %s
              AND one_month_return IS NOT NULL
              AND three_month_return IS NOT NULL
        """
        
        cur.execute(stats_query, (insider_name,))
        stats = cur.fetchone()

        # Get all purchase transactions
        transactions_query = """
            SELECT 
                datetime,
                stock,
                stock_name,
                shares_traded,
                price_per_share,
                total_value,
                one_month_return,
                three_month_return
            FROM us_insider_trading_table
            WHERE insider_name = %s
            AND transaction_type = 'P'
            ORDER BY datetime DESC
        """
        
        cur.execute(transactions_query, (insider_name,))
        transactions = cur.fetchall()
        
        if not stats:
            return jsonify({
                "error": "No statistics found for this insider"
            }), 404

        response = {
            "insiderName": insider_name,
            "stats": {
                "totalPurchases": stats['total_purchases'],
                "oneMonthWins": stats['one_month_wins'],
                "avgOneMonthReturn": float(stats['avg_one_month_return']) if stats['avg_one_month_return'] else 0,
                "threeMonthWins": stats['three_month_wins'],
                "avgThreeMonthReturn": float(stats['avg_three_month_return']) if stats['avg_three_month_return'] else 0
            },
            "transactions": transactions
        }
        
        cur.close()
        conn.close()
        
        return jsonify(response)
        
    except Exception as e:
        logging.error(f"Error fetching insider statistics: {e}")
        return jsonify({"error": str(e)}), 500
        


################## FIBONACCI LEVELS API ###################################

def calculate_fibonacci_levels(all_time_high: float, all_time_low: float) -> Dict[str, float]:
    """Calculate Fibonacci retracement and extension levels."""
    diff = all_time_high - all_time_low
    levels = {
        "0": all_time_low,
        "23.6": all_time_low + (diff * 0.236),
        "38.2": all_time_low + (diff * 0.382),
        "50.0": all_time_low + (diff * 0.5),
        "61.8": all_time_low + (diff * 0.618),
        "78.6": all_time_low + (diff * 0.786),
        "100.0": all_time_high,
        "161.8": all_time_high + (diff * 0.618),
        "261.8": all_time_high + (diff * 1.618)
    }
    return levels

def calculate_price_distances(current_price: float, fib_levels: Dict[str, float]) -> Dict[str, float]:
    """Calculate percentage distance from current price to each Fibonacci level."""
    distances = {}
    for level, price in fib_levels.items():
        if price != 0:  # Avoid division by zero
            distance = ((current_price - price) / price) * 100
            distances[level] = distance
        else:
            distances[level] = None
    return distances

@app.route('/api/crypto/fibonacci', methods=['GET'])
def get_fibonacci_data():
    try:
        # Get date parameter, default to yesterday if not provided
        date_str = request.args.get('date')
        if date_str:
            selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            selected_date = datetime.now().date() - timedelta(days=1)

        # Get base currency from query parameter (default to USD)
        base = request.args.get('base', 'usd').lower()
        
        # Select appropriate table based on base currency
        table_name = f"crypto_daily_table{'_' + base if base != 'usd' else ''}"
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Execute query to get price data
        cur.execute(f"""
            WITH latest_prices AS (
                SELECT DISTINCT ON (stock)
                    stock,
                    crypto_name,
                    close as current_price,
                    datetime as price_date
                FROM {table_name}
                WHERE DATE(datetime) = %s
                ORDER BY stock, datetime DESC
            ),
            price_ranges AS (
                SELECT 
                    stock,
                    MAX(high) as all_time_high,
                    MIN(low) as all_time_low
                FROM {table_name}
                GROUP BY stock
            )
            SELECT 
                l.stock,
                l.crypto_name,
                l.current_price,
                l.price_date,
                r.all_time_high,
                r.all_time_low
            FROM latest_prices l
            JOIN price_ranges r ON l.stock = r.stock
            WHERE LOWER(l.stock) LIKE '%%usd%%'
            ORDER BY l.stock;
        """, (selected_date,))
        
        raw_data = cur.fetchall()
        
        # Process data and calculate Fibonacci levels
        fibonacci_data = []
        for row in raw_data:
            if row['all_time_high'] and row['all_time_low'] and row['current_price']:
                fib_levels = calculate_fibonacci_levels(
                    float(row['all_time_high']), 
                    float(row['all_time_low'])
                )
                price_distances = calculate_price_distances(
                    float(row['current_price']), 
                    fib_levels
                )
                
                fibonacci_data.append({
                    'stock': row['stock'],
                    'crypto_name': row['crypto_name'],
                    'current_price': float(row['current_price']),
                    'price_date': row['price_date'],
                    'all_time_high': float(row['all_time_high']),
                    'all_time_low': float(row['all_time_low']),
                    'fibonacci_levels': fib_levels,
                    'level_distances': price_distances
                })
        
        cur.close()
        conn.close()
        
        return jsonify(fibonacci_data)
        
    except Exception as e:
        logging.error(f"Error fetching Fibonacci data: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)