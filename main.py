#!/usr/bin/env python3
"""
Polymarket Trading Data Retrieval
Gets live trading data from official Polymarket endpoints
"""

import requests
import json
from datetime import datetime

def get_live_trades():
    """Get and display live Polymarket trading data"""
    print('=== GETTING LIVE POLYMARKET TRADING DATA ===')
    print(f'Unix Timestamp: {int(datetime.now().timestamp())}')
    print()

    # Get recent trades
    response = requests.get('https://data-api.polymarket.com/trades?limit=200')
    trades = response.json()

    print(f'Found {len(trades)} recent trades:')
    print()
    
    # Show schema at the top
    print('market | price | size | side | outcome')
    print('-' * 60)
    
    for i, trade in enumerate(trades):
        # Convert price to thousands of dollars (multiply by 1000)
        price_k = int(trade["price"] * 1000)
        
        # Format the data row
        market = trade["title"][:30] + "..." if len(trade["title"]) > 100 else trade["title"]
        print(f'{market} | {price_k} | {trade["size"]} | {trade["side"]} | {trade["outcome"]}')
        
        # Show schema every 100 trades
        if (i + 1) % 100 == 0:
            print('-' * 60)
            print('market | price | size | side | outcome')
            print('-' * 60)
    
    # Show schema at the bottom
    print('-' * 60)
    print('market | price | size | side | outcome')
    print()
    print(f'Raw timestamps from API: {trades[0]["timestamp"] if trades else "N/A"}')

if __name__ == "__main__":
    get_live_trades()