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
from us_stocks.us_stock_screener_table_process import update_screener_table
from crypto.crypto_screener_table_process import update_screener_table_usd
from crypto.crypto_screener_table_process_btc import update_screener_table_btc
from crypto.crypto_screener_table_process_eth import update_screener_table_eth


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

########### STOCK APIS #############################################
@app.route('/api/stocks/latest')
def get_latest_stock_data():
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
        update_screener_table(selected_date)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT *
            FROM us_screener_table
            WHERE DATE(datetime) = %s
            ORDER BY stock
        """
        
        cur.execute(query, (selected_date,))
        data = cur.fetchall()
        
        logging.info(f"Fetched data for {len(data)} stocks for date {selected_date}")
        cur.close()
        conn.close()
        
        return jsonify(data)
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
                    SELECT *
                    FROM us_alerts_table
                    WHERE datetime >= NOW() - INTERVAL '30 days'
                    ORDER BY datetime DESC
                """
                
                cur.execute(query)
                data = cur.fetchall()
                
                logging.info(f"Fetched {len(data)} alerts from the last 30 days")
                return jsonify(data)
                
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
            # Default to last month if no dates provided
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=30)
        else:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query for both force index and Williams %R data
        query = """
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
        """
        
        cur.execute(query, (symbol, start_date, end_date))
        data = cur.fetchall()
        
        # Also fetch basic stock info
        cur.execute("""
            SELECT DISTINCT stock_name
            FROM us_daily_table
            WHERE stock = %s
            LIMIT 1
        """, (symbol,))
        stock_info = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not data:
            return jsonify({
                "error": f"No data found for stock {symbol}"
            }), 404
            
        return jsonify({
            "symbol": symbol,
            "stock_name": stock_info['stock_name'] if stock_info else None,
            "data": data
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
        
        # Get the last 7 days of alerts
        cur.execute("""
            SELECT *
            FROM crypto_alerts_table
            WHERE datetime >= CURRENT_DATE - INTERVAL '10 days'
            ORDER BY datetime DESC
        """)
        
        alerts = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify(alerts)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/alerts_eth', methods=['GET'])
def get_crypto_alerts_eth():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get the last 7 days of ETH-based alerts
        cur.execute("""
            SELECT *
            FROM crypto_alerts_table_eth
            WHERE datetime >= CURRENT_DATE - INTERVAL '10 days'
            ORDER BY datetime DESC
        """)
        
        alerts = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify(alerts)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/crypto/alerts_btc', methods=['GET'])
def get_crypto_alerts_btc():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get the last 7 days of BTC-based alerts
        cur.execute("""
            SELECT *
            FROM crypto_alerts_table_btc
            WHERE datetime >= CURRENT_DATE - INTERVAL '10 days'
            ORDER BY datetime DESC
        """)
        
        alerts = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify(alerts)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)