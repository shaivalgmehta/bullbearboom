import os
import time
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_values
from sec_api import InsiderTradingApi
from typing import Dict, Any, List, Optional
import pandas as pd
from dotenv import load_dotenv
import math

# Load environment variables
load_dotenv()

# Database connection parameters
db_params = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

SEC_API_KEY = os.getenv('SEC_API_KEY')

def get_insider_transactions(start_date: str, end_date: str) -> List[Dict]:
    """
    Fetch insider transactions from SEC API with pagination.
    Only fetches buy ('P') and sell ('S') transactions.
    """
    insider_api = InsiderTradingApi(SEC_API_KEY)
    transactions = []
    from_param = 0
    size = 50  # API max size
    
    while True:
        try:
            # Updated query to specifically look for P and S transaction codes in both non-derivative and derivative tables
            query = {
                "query": f"""(nonDerivativeTable.transactions.coding.code:(P OR S) OR 
                            derivativeTable.transactions.coding.code:(P OR S)) AND 
                            periodOfReport:[{start_date} TO {end_date}]""",
                "from": str(from_param),
                "size": str(size),
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            response = insider_api.get_data(query)
            print(f"Fetched batch starting at {from_param}")
            
            if not response.get('transactions'):
                break
                
            transactions.extend(response['transactions'])
            
            # Check if we've got all results or reached the API limit
            if len(response['transactions']) < size:
                break
            
            # Move to next batch    
            from_param += size
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            print(f"Error fetching transactions: {e}")
            break
    
    return transactions

def process_filing_transactions(filing: Dict) -> List[Dict]:
    """
    Process a single Form 4 filing and extract relevant transaction details.
    Returns a list as there can be multiple transactions per filing.
    """
    # Skip if no non-derivative transactions
    if 'nonDerivativeTable' not in filing or 'transactions' not in filing['nonDerivativeTable']:
        return []
        
    processed_transactions = []

    try:
        base_data = {
            'filing_date': filing.get('filedAt'),
            'stock': filing.get('issuer', {}).get('tradingSymbol'),
            'stock_name': filing.get('issuer', {}).get('name'),
            'insider_name': filing.get('reportingOwner', {}).get('name'),
            'form_type': filing.get('documentType'),
            'relationship_is_director': filing.get('reportingOwner', {}).get('relationship', {}).get('isDirector', False),
            'relationship_is_officer': filing.get('reportingOwner', {}).get('relationship', {}).get('isOfficer', False),
            'relationship_is_ten_percent_owner': filing.get('reportingOwner', {}).get('relationship', {}).get('isTenPercentOwner', False),
            'relationship_is_other': filing.get('reportingOwner', {}).get('relationship', {}).get('isOther', False),
            'officer_title': filing.get('reportingOwner', {}).get('relationship', {}).get('officerTitle'),
            'sec_link': f"https://www.sec.gov/Archives/edgar/data/{filing.get('issuer', {}).get('cik')}/{filing.get('accessionNo')}"
        }

        # Get the transaction ID from the root level transactions array
        transaction_id = filing.get('id', '')
        
        for transaction in filing['nonDerivativeTable']['transactions']:
            # Only process buy ('P') and sell ('S') transactions
            if transaction.get('coding', {}).get('code') not in ['P', 'S']:
                continue

            # Safely get transaction amounts
            amounts = transaction.get('amounts', {})
            shares = amounts.get('shares')
            price_per_share = amounts.get('pricePerShare')

            # Skip if essential data is missing
            if not shares or not price_per_share:
                print(f"Skipping transaction due to missing shares or price data: {transaction_id}")
                continue

            try:
                shares = float(shares)
                price_per_share = float(price_per_share)
            except (ValueError, TypeError):
                print(f"Skipping transaction due to invalid numeric values: {transaction_id}")
                continue

            processed_transaction = {
                'datetime': transaction.get('transactionDate'),
                'transaction_type': transaction.get('coding', {}).get('code'),
                'shares_traded': shares,
                'price_per_share': price_per_share,
                'total_value': shares * price_per_share,
                'shares_owned_following': float(transaction.get('postTransactionAmounts', {}).get('sharesOwnedFollowingTransaction', 0)),
                'transaction_id': transaction_id,
                **base_data
            }
            processed_transactions.append(processed_transaction)

    except Exception as e:
        print(f"Error processing filing {filing.get('id', 'unknown')}: {str(e)}")
        return []
    
    return processed_transactions

def get_follow_up_prices(stock: str, transaction_date: datetime, 
                        cur) -> Dict[str, Optional[float]]:
    """
    Get 1-month and 3-month follow-up prices from us_daily_table.
    Handles weekends and holidays by finding next available trading day.
    """
    result = {
        'one_month_price': None,
        'three_month_price': None,
        'one_month_date': None,
        'three_month_date': None,
        'one_month_return': None,
        'three_month_return': None
    }
    
    # Get one month price
    one_month_date = transaction_date + timedelta(days=30)
    cur.execute("""
        SELECT datetime, close
        FROM us_daily_table
        WHERE stock = %s AND datetime >= %s
        ORDER BY datetime ASC
        LIMIT 1
    """, (stock, one_month_date))
    one_month_data = cur.fetchone()
    
    # Get three month price
    three_month_date = transaction_date + timedelta(days=90)
    cur.execute("""
        SELECT datetime, close
        FROM us_daily_table
        WHERE stock = %s AND datetime >= %s
        ORDER BY datetime ASC
        LIMIT 1
    """, (stock, three_month_date))
    three_month_data = cur.fetchone()
    
    # Get transaction date price for return calculation
    cur.execute("""
        SELECT close
        FROM us_daily_table
        WHERE stock = %s AND DATE(datetime) = DATE(%s)
    """, (stock, transaction_date))
    base_price_data = cur.fetchone()
    
    if one_month_data:
        result['one_month_date'] = one_month_data[0]
        result['one_month_price'] = float(one_month_data[1])
        if base_price_data:
            result['one_month_return'] = (result['one_month_price'] - float(base_price_data[0])) / float(base_price_data[0])
    
    if three_month_data:
        result['three_month_date'] = three_month_data[0]
        result['three_month_price'] = float(three_month_data[1])
        if base_price_data:
            result['three_month_return'] = (result['three_month_price'] - float(base_price_data[0])) / float(base_price_data[0])
    
    return result

def find_duplicate_transactions(trades):
    """
    Find duplicate transactions that would violate the unique constraint.
    The constraint is on (datetime, stock, shares_traded, transaction_id, price_per_share, shares_owned_following)
    """
    seen_records = {}
    duplicates = []
    
    for trade in trades:
        # Create a key tuple with the constraint fields
        key = (
            trade['datetime'],
            trade['stock'],
            trade['shares_traded'],
            trade['transaction_id'],
            trade['price_per_share'],
            trade['shares_owned_following']
        )
        
        if key in seen_records:
            duplicates.append({
                'key': key,
                'first_record': seen_records[key],
                'duplicate_record': trade
            })
        else:
            seen_records[key] = trade
            
    return duplicates

def store_insider_trades(trades: List[Dict], batch_size: int = 50):
    """
    Store processed insider trading data in the database in batches.
    Now includes duplicate detection and reporting.
    """
    # First, check for duplicates in the incoming data
    duplicates = find_duplicate_transactions(trades)
    if duplicates:
        print("\nFound duplicate transactions that would violate constraints:")
        for dup in duplicates:
            print("\nDuplicate Set:")
            print("First Record:")
            print(f"  DateTime: {dup['first_record']['datetime']}")
            print(f"  Stock: {dup['first_record']['stock']}")
            print(f"  Shares Traded: {dup['first_record']['shares_traded']}")
            print(f"  Transaction ID: {dup['first_record']['transaction_id']}")
            print(f"  Price Per Share: {dup['first_record']['price_per_share']}")
            print(f"  Shares Owned Following: {dup['first_record']['shares_owned_following']}")
            print(f"  Insider Name: {dup['first_record']['insider_name']}")
            
            print("\nDuplicate Record:")
            print(f"  DateTime: {dup['duplicate_record']['datetime']}")
            print(f"  Stock: {dup['duplicate_record']['stock']}")
            print(f"  Shares Traded: {dup['duplicate_record']['shares_traded']}")
            print(f"  Transaction ID: {dup['duplicate_record']['transaction_id']}")
            print(f"  Price Per Share: {dup['duplicate_record']['price_per_share']}")
            print(f"  Shares Owned Following: {dup['duplicate_record']['shares_owned_following']}")
            print(f"  Insider Name: {dup['duplicate_record']['insider_name']}")
            print("-" * 80)
            
        # Remove duplicates by keeping only the first occurrence
        unique_trades = []
        seen_keys = set()
        for trade in trades:
            key = (
                trade['datetime'],
                trade['stock'],
                trade['shares_traded'],
                trade['transaction_id'],
                trade['price_per_share'],
                trade['shares_owned_following']
            )
            if key not in seen_keys:
                seen_keys.add(key)
                unique_trades.append(trade)
        trades = unique_trades
        print(f"\nRemoved {len(duplicates)} duplicate records. Proceeding with {len(trades)} unique records.")

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    total_trades = len(trades)
    num_batches = math.ceil(total_trades / batch_size)

    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_trades)
        batch_trades = trades[start_idx:end_idx]

        values = []
        for trade in batch_trades:
            # Get follow-up prices if transaction is old enough
            follow_up_prices = {}
            transaction_date = datetime.strptime(trade['datetime'], '%Y-%m-%d')
            if transaction_date < (datetime.now() - timedelta(days=90)):
                follow_up_prices = get_follow_up_prices(trade['stock'], transaction_date, cur)

            values.append((
                transaction_date,
                trade['stock'],
                trade['stock_name'],
                trade['insider_name'],
                None,  # insider_title - derived from relationships
                trade['officer_title'],
                trade['transaction_type'],
                trade['shares_traded'],
                trade['price_per_share'],
                trade['total_value'],
                trade['shares_owned_following'],
                follow_up_prices.get('one_month_price'),
                follow_up_prices.get('three_month_price'),
                follow_up_prices.get('one_month_date'),
                follow_up_prices.get('three_month_date'),
                follow_up_prices.get('one_month_return'),
                follow_up_prices.get('three_month_return'),
                datetime.strptime(trade['filing_date'].split('T')[0], '%Y-%m-%d'),
                trade['relationship_is_director'],
                trade['relationship_is_officer'],
                trade['relationship_is_ten_percent_owner'],
                trade['relationship_is_other'],
                trade['form_type'],
                trade['sec_link'],
                trade['transaction_id'],
                datetime.now(timezone.utc)
            ))

        try:
            execute_values(cur, """
                INSERT INTO us_insider_trading_table (
                    datetime, stock, stock_name, insider_name, insider_title,
                    officer_title, transaction_type, shares_traded, price_per_share,
                    total_value, shares_owned_following, one_month_price, 
                    three_month_price, one_month_date, three_month_date, 
                    one_month_return, three_month_return, filing_date,
                    relationship_is_director, relationship_is_officer,
                    relationship_is_ten_percent_owner, relationship_is_other,
                    form_type, sec_link, transaction_id, last_modified_date
                ) VALUES %s
                ON CONFLICT (datetime, stock, shares_traded, transaction_type, transaction_id, price_per_share, shares_owned_following) DO UPDATE SET
                    stock_name = EXCLUDED.stock_name,
                    officer_title = EXCLUDED.officer_title,
                    total_value = EXCLUDED.total_value,
                    one_month_price = EXCLUDED.one_month_price,
                    three_month_price = EXCLUDED.three_month_price,
                    one_month_date = EXCLUDED.one_month_date,
                    three_month_date = EXCLUDED.three_month_date,
                    one_month_return = EXCLUDED.one_month_return,
                    three_month_return = EXCLUDED.three_month_return,
                    relationship_is_director = EXCLUDED.relationship_is_director,
                    relationship_is_officer = EXCLUDED.relationship_is_officer,
                    relationship_is_ten_percent_owner = EXCLUDED.relationship_is_ten_percent_owner,
                    relationship_is_other = EXCLUDED.relationship_is_other,
                    form_type = EXCLUDED.form_type,
                    sec_link = EXCLUDED.sec_link,
                    last_modified_date = EXCLUDED.last_modified_date
            """, values)

            conn.commit()
            print(f"Successfully stored batch {batch_num + 1} of {num_batches} with {len(batch_trades)} records")

        except Exception as e:
            conn.rollback()
            print(f"Error storing batch {batch_num + 1}: {e}")
            raise

    conn.close()
    cur.close()

def update_missing_follow_up_prices():
    """
    Update follow-up prices for any transactions that were too recent
    when initially stored.
    """
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    # Get transactions missing follow-up prices that are now old enough
    cur.execute("""
        SELECT datetime, stock
        FROM us_insider_trading_table
        WHERE (datetime < NOW() - INTERVAL '91 days')
        AND (one_month_price IS NULL OR three_month_price IS NULL)
    """)
    
    transactions = cur.fetchall()
    
    for transaction_date, stock in transactions:
        follow_up_prices = get_follow_up_prices(stock, transaction_date, cur)
        
        cur.execute("""
            UPDATE us_insider_trading_table
            SET one_month_price = %s,
                three_month_price = %s,
                one_month_date = %s,
                three_month_date = %s,
                one_month_return = %s,
                three_month_return = %s,
                last_modified_date = NOW()
            WHERE datetime = %s AND stock = %s
        """, (
            follow_up_prices.get('one_month_price'),
            follow_up_prices.get('three_month_price'),
            follow_up_prices.get('one_month_date'),
            follow_up_prices.get('three_month_date'),
            follow_up_prices.get('one_month_return'),
            follow_up_prices.get('three_month_return'),
            transaction_date,
            stock
        ))
    
    conn.commit()
    cur.close()
    conn.close()