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
    print(f'Timestamp: {datetime.now()}')
    print()

    # Get recent trades
    response = requests.get('https://data-api.polymarket.com/trades?limit=20')
    trades = response.json()

    print(f'Found {len(trades)} recent trades:')
    print()

    for i, trade in enumerate(trades[:5]):
        print(f'{i+1}. Market: {trade["title"]}')
        print(f'   Price: ${trade["price"]}')
        print(f'   Size: ${trade["size"]}')
        print(f'   Side: {trade["side"]}')
        print(f'   Outcome: {trade["outcome"]}')
        print()

if __name__ == "__main__":
    get_live_trades()