#!/usr/bin/env python3
"""
Polymarket Trading Data Retrieval
Gets live trading data from official Polymarket endpoints
"""

import requests
from datetime import datetime
import pytz

def get_live_trades():
    """Get and display live Polymarket trading data"""
    print('=== GETTING LIVE POLYMARKET TRADING DATA ===')
    
    # Get current time in EST
    est_tz = pytz.timezone('US/Eastern')
    current_time_est = datetime.now(est_tz)
    current_timestamp_ms = int(current_time_est.timestamp() * 1000)
    
    print(f'Current EST: {current_time_est.strftime("%Y-%m-%d %H:%M:%S EST")}')
    print()

    # Get recent trades
    response = requests.get('https://data-api.polymarket.com/trades?limit=1000')
    trades = response.json()

    print(f'Found {len(trades)} recent trades:')
    print()
    
    # Show schema at the top
    print('price | size | side | outcome | timestamp | market')
    print('-' * 100)
    
    for i, trade in enumerate(reversed(trades)):
        # Convert price to thousands of dollars (multiply by 1000)
        price_k = int(trade["price"] * 1000)
        
        # Calculate milliseconds ago
        trade_timestamp_ms = int(trade["timestamp"] * 1000)
        ms_ago = current_timestamp_ms - trade_timestamp_ms
        
        # Format size to 3 significant figures
        size = float(trade["size"])
        if size >= 100:
            size_str = f"{size:.0f}"
        elif size >= 10:
            size_str = f"{size:.1f}"
        else:
            size_str = f"{size:.2f}"
        
        # Format the data row with fixed widths (price | size | side | outcome | timestamp | market)
        market = trade["title"]
        print(f'{price_k:>5} | {size_str:>8} | {trade["side"]:>4} | {trade["outcome"]:<12} | {ms_ago:>8}ms | {market}')
    
    # Show schema at the bottom
    print('-' * 100)
    print('price | size | side | outcome | timestamp | market')
    print()
    print(f'Current EST: {current_time_est.strftime("%Y-%m-%d %H:%M:%S EST")}')

if __name__ == "__main__":
    get_live_trades()