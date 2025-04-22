from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from decimal import Decimal
from datetime import datetime, timedelta
import logging
from us_stocks.us_stock_screener_table_process import update_us_screener_table
from in_stocks.in_stock_screener_table_process import update_in_screener_table
from crypto.crypto_screener_table_process import update_screener_table_usd
from crypto.crypto_screener_table_process_btc import update_screener_table_btc
from crypto.crypto_screener_table_process_eth import update_screener_table_eth
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity, verify_jwt_in_request, JWTManager
import bcrypt
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

app.config["JWT_SECRET_KEY"] = os.getenv('JWT_SECRET_KEY', '3d6f45a5fc12445dbac2f59c3b6c7cb1d2c2d9c7b8eb4157e2c611f6b3f8ac83')
jwt = JWTManager(app)

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
    logging.info("Fetching alerts for US stocks")
    try:
        # Add parameter for watch list filtering
        watch_list_only = request.args.get('watch_list_only', 'false').lower() == 'true'
        
        # Get user_id from JWT if authenticated
        user_id = None
        if watch_list_only:
            try:
                # Verify JWT but don't require it for the whole endpoint
                verify_jwt_in_request()
                user_id = get_jwt_identity()
            except Exception as e:
                return jsonify({"error": "Authentication required for watch list filtering"}), 401
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Base query
                query = """
                    SELECT 
                        a.datetime,
                        a.stock,
                        a.stock_name,
                        a.alerts
                    FROM us_alerts_table a
                """
                
                # Add join with watch_lists if filtering by watch list
                params = []
                where_clauses = ["a.datetime >= NOW() - INTERVAL '30 days'"]
                
                if watch_list_only:
                    query += """
                        INNER JOIN watch_lists w ON 
                            a.stock = w.symbol AND 
                            w.entity_type = 'us_stock' AND
                            w.user_id = %s
                    """
                    params.append(user_id)
                
                # Complete the query
                query += f"""
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY a.datetime DESC
                """
                
                cur.execute(query, params)
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
                
                logging.info(f"Fetched {len(processed_data)} US alerts")
                return jsonify(processed_data)

    except Exception as e:
        logging.error(f"Error fetching US alerts data:: {str(e)}")
        return jsonify({"error": str(e)}), 500  # <-- send actual error back

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
    logging.info("Fetching alerts for Indian stocks")
    try:
        # Add parameter for watch list filtering
        watch_list_only = request.args.get('watch_list_only', 'false').lower() == 'true'
        
        # Get user_id from JWT if authenticated
        user_id = None
        if watch_list_only:
            try:
                # Verify JWT but don't require it for the whole endpoint
                verify_jwt_in_request()
                user_id = get_jwt_identity()
            except Exception as e:
                return jsonify({"error": "Authentication required for watch list filtering"}), 401
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Base query
                query = """
                    SELECT 
                        a.datetime,
                        a.stock,
                        a.stock_name,
                        a.alerts
                    FROM in_alerts_table a
                """
                
                # Add join with watch_lists if filtering by watch list
                params = []
                where_clauses = ["a.datetime >= NOW() - INTERVAL '30 days'"]
                
                if watch_list_only:
                    query += """
                        INNER JOIN watch_lists w ON 
                            a.stock = w.symbol AND 
                            w.entity_type = 'in_stock' AND
                            w.user_id = %s
                    """
                    params.append(user_id)
                
                # Complete the query
                query += f"""
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY a.datetime DESC
                """
                
                cur.execute(query, params)
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
                
                logging.info(f"Fetched {len(processed_data)} India alerts")
                return jsonify(processed_data)
                
    except Exception as e:
        logging.error(f"Error fetching India alerts data: {e}")
        return jsonify({"error": "Failed to fetch alerts data"}), 500

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

@app.route('/api/crypto/alerts')
def get_crypto_alerts():
    try:
        # Add parameter for watch list filtering
        watch_list_only = request.args.get('watch_list_only', 'false').lower() == 'true'
        
        # Get user_id from JWT if authenticated
        user_id = None
        if watch_list_only:
            try:
                # Verify JWT but don't require it for the whole endpoint
                verify_jwt_in_request()
                user_id = get_jwt_identity()
            except Exception as e:
                return jsonify({"error": "Authentication required for watch list filtering"}), 401
            
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Base query
        query = """
            SELECT 
                a.datetime,
                a.stock,
                a.crypto_name,
                a.alerts
            FROM crypto_alerts_table a
        """
        
        # Add join with watch_lists if filtering by watch list
        params = []
        where_clauses = ["a.datetime >= CURRENT_DATE - INTERVAL '10 days'"]
        
        if watch_list_only:
            query += """
                INNER JOIN watch_lists w ON 
                    a.stock = w.symbol AND 
                    w.entity_type = 'crypto' AND
                    w.user_id = %s
            """
            params.append(user_id)
        
        # Complete the query
        query += f"""
            WHERE {' AND '.join(where_clauses)}
            ORDER BY a.datetime DESC
        """
        
        cur.execute(query, params)
        
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

@app.route('/api/crypto/alerts_eth')
def get_crypto_alerts_eth():
    try:
        # Add parameter for watch list filtering
        watch_list_only = request.args.get('watch_list_only', 'false').lower() == 'true'
        
        # Get user_id from JWT if authenticated
        user_id = get_jwt_identity() if watch_list_only else None
        
        # If watch list filtering is requested but user is not logged in
        if watch_list_only and user_id is None:
            return jsonify({"error": "Authentication required for watch list filtering"}), 401
            
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Base query
        query = """
            SELECT 
                a.datetime,
                a.stock,
                a.crypto_name,
                a.alerts
            FROM crypto_alerts_table_eth a
        """
        
        # Add join with watch_lists if filtering by watch list
        params = []
        where_clauses = ["a.datetime >= CURRENT_DATE - INTERVAL '10 days'"]
        
        if watch_list_only:
            query += """
                INNER JOIN watch_lists w ON 
                    a.stock = w.symbol AND 
                    w.entity_type = 'crypto' AND
                    w.user_id = %s
            """
            params.append(user_id)
        
        # Complete the query
        query += f"""
            WHERE {' AND '.join(where_clauses)}
            ORDER BY a.datetime DESC
        """
        
        cur.execute(query, params)
        
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

@app.route('/api/crypto/alerts_btc')
def get_crypto_alerts_btc():
    try:
        # Add parameter for watch list filtering
        watch_list_only = request.args.get('watch_list_only', 'false').lower() == 'true'
        
        # Get user_id from JWT if authenticated
        user_id = get_jwt_identity() if watch_list_only else None
        
        # If watch list filtering is requested but user is not logged in
        if watch_list_only and user_id is None:
            return jsonify({"error": "Authentication required for watch list filtering"}), 401
            
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Base query
        query = """
            SELECT 
                a.datetime,
                a.stock,
                a.crypto_name,
                a.alerts
            FROM crypto_alerts_table_btc a
        """
        
        # Add join with watch_lists if filtering by watch list
        params = []
        where_clauses = ["a.datetime >= CURRENT_DATE - INTERVAL '10 days'"]
        
        if watch_list_only:
            query += """
                INNER JOIN watch_lists w ON 
                    a.stock = w.symbol AND 
                    w.entity_type = 'crypto' AND
                    w.user_id = %s
            """
            params.append(user_id)
        
        # Complete the query
        query += f"""
            WHERE {' AND '.join(where_clauses)}
            ORDER BY a.datetime DESC
        """
        
        cur.execute(query, params)
        
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
        
        # Get paginated and filtered data with aggregated stats
        # Using a Common Table Expression (CTE) to calculate insider averages once
        query = f"""
            WITH insider_averages AS (
                SELECT 
                    insider_name,
                    AVG(one_month_return) FILTER (WHERE transaction_type = 'P' AND one_month_return IS NOT NULL) AS avg_one_month_return,
                    AVG(three_month_return) FILTER (WHERE transaction_type = 'P' AND three_month_return IS NOT NULL) AS avg_three_month_return,
                    COUNT(*) FILTER (WHERE transaction_type = 'P') AS total_purchases,
                    COUNT(*) FILTER (WHERE transaction_type = 'P' AND one_month_return > 0) AS one_month_wins,
                    COUNT(*) FILTER (WHERE transaction_type = 'P' AND three_month_return > 0) AS three_month_wins
                FROM us_insider_trading_table
                GROUP BY insider_name
            )
            SELECT 
                t.datetime,
                t.stock,
                t.stock_name,
                t.insider_name,
                t.transaction_type,
                t.relationship_is_director,
                t.relationship_is_officer,
                t.relationship_is_ten_percent_owner,
                t.relationship_is_other,
                t.shares_traded,
                t.price_per_share,
                t.total_value,
                t.shares_owned_following,
                t.one_month_price,
                t.three_month_price,
                t.one_month_return,
                t.three_month_return,
                a.avg_one_month_return,
                a.avg_three_month_return,
                a.total_purchases,
                a.one_month_wins,
                a.three_month_wins
            FROM us_insider_trading_table t
            LEFT JOIN insider_averages a ON t.insider_name = a.insider_name
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
        
        logging.info(f"Fetched page {page} of insider trading records with aggregated stats")
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

################## AUTHENTICATION API ##########################################

# User registration
@app.route('/api/auth/register', methods=['POST'])
def register():
    try:
        data = request.get_json()
        email = data.get('email')
        password = data.get('password')
        
        # Input validation
        if not email or not password:
            return jsonify({"error": "Email and password are required"}), 400
            
        if len(password) < 8:
            return jsonify({"error": "Password must be at least 8 characters"}), 400
            
        # Check if user already exists
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        existing_user = cur.fetchone()
        
        if existing_user:
            cur.close()
            conn.close()
            return jsonify({"error": "Email already registered"}), 409
        
        # Hash the password
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Insert new user
        cur.execute(
            "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
            (email, hashed_password)
        )
        
        user_id = cur.fetchone()[0]
        conn.commit()
        
        # Generate JWT token
        access_token = create_access_token(identity=str(user_id), expires_delta=timedelta(days=7))

        
        cur.close()
        conn.close()
        
        return jsonify({
            "message": "Registration successful",
            "token": access_token,
            "user": {"id": user_id, "email": email}
        })
        
    except Exception as e:
        logging.error(f"Registration error: {str(e)}")
        return jsonify({"error": str(e)}), 500  # <-- send actual error back

# User login
@app.route('/api/auth/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        email = data.get('email')
        password = data.get('password')
        
        # Input validation
        if not email or not password:
            return jsonify({"error": "Email and password are required"}), 400
        
        # Fetch user from database
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT id, password_hash FROM users WHERE email = %s", (email,))
        user_data = cur.fetchone()
        
        if not user_data:
            cur.close()
            conn.close()
            return jsonify({"error": "Invalid email or password"}), 401
        
        user_id, hashed_password = user_data
        
        # Verify password
        if not bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8')):
            cur.close()
            conn.close()
            return jsonify({"error": "Invalid email or password"}), 401
        
        # Generate JWT token
        access_token = create_access_token(identity=str(user_id), expires_delta=timedelta(days=7))

        cur.close()
        conn.close()
        
        return jsonify({
            "message": "Login successful",
            "token": access_token,
            "user": {"id": user_id, "email": email}
        })
        
    except Exception as e:
        logging.error(f"Login error: {str(e)}")
        return jsonify({"error": "Login failed"}), 500

# Get current user info
@app.route('/api/auth/me', methods=['GET'])
@jwt_required()
def get_user():
    try:
        user_id = get_jwt_identity()
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT id, email, created_at FROM users WHERE id = %s", (user_id,))
        user_data = cur.fetchone()
        
        if not user_data:
            cur.close()
            conn.close()
            return jsonify({"error": "User not found"}), 404
        
        user_id, email, created_at = user_data
        
        cur.close()
        conn.close()
        
        return jsonify({
            "id": user_id,
            "email": email,
            "created_at": created_at.isoformat() if created_at else None
        })
        
    except Exception as e:
        logging.error(f"Get user error: {str(e)}")
        return jsonify({"error": "Failed to retrieve user data"}), 500

############## WATCH LIST API ###################################################

# Get user's watch list
@app.route('/api/watchlist', methods=['GET'])
@jwt_required()
def get_watchlist():
    user_id = get_jwt_identity()
    
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT entity_type, symbol, added_at
        FROM watch_lists
        WHERE user_id = %s
        ORDER BY added_at DESC
    """, (user_id,))
    
    watch_list = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return jsonify(watch_list)

# Add item to watch list
@app.route('/api/watchlist', methods=['POST'])
@jwt_required()
def add_to_watchlist():
    user_id = get_jwt_identity()
    data = request.get_json()
    
    entity_type = data.get('entity_type')
    symbol = data.get('symbol')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO watch_lists (user_id, entity_type, symbol)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, entity_type, symbol) DO NOTHING
            RETURNING id
        """, (user_id, entity_type, symbol))
        
        result = cur.fetchone()
        conn.commit()
        
        success = result is not None
        
        return jsonify({"success": success})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# Remove item from watch list
@app.route('/api/watchlist', methods=['DELETE'])
@jwt_required()
def remove_from_watchlist():
    user_id = get_jwt_identity()
    data = request.get_json()
    
    entity_type = data.get('entity_type')
    symbol = data.get('symbol')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            DELETE FROM watch_lists
            WHERE user_id = %s AND entity_type = %s AND symbol = %s
            RETURNING id
        """, (user_id, entity_type, symbol))
        
        result = cur.fetchone()
        conn.commit()
        
        success = result is not None
        
        return jsonify({"success": success})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)