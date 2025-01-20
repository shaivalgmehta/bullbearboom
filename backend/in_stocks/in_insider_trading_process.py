#!/usr/bin/env python3

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from in_insider_trading_functions import (
    get_insider_transactions,
    process_filing_transactions,
    store_insider_trades,
    update_missing_follow_up_prices
)

def main():
    # Default to fetching last 30 days of transactions
    end_date = datetime.now().strftime('%Y-%m-%d')

    # end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    
    print(f"Fetching insider trades from {start_date} to {end_date}")
    
    # Get raw filings from SEC API
    filings = get_insider_transactions(start_date, end_date)
    
    # Process each filing
    all_transactions = []
    for filing in filings:
        transactions = process_filing_transactions(filing)
        all_transactions.extend(transactions)
    
    if all_transactions:
        print(f"Storing {len(all_transactions)} insider transactions")
        store_insider_trades(all_transactions)
    else:
        print("No new transactions to store")
    
    # Update any missing follow-up prices
    print("Updating missing follow-up prices")
    update_missing_follow_up_prices()
    
    print("Process completed successfully")

if __name__ == "__main__":
    main()