#!/usr/bin/env python3
"""
Polymarket Markets Fetcher
Fetches recently traded markets and their token IDs from Polymarket.
Uses the "hot & fresh" approach via recent trades endpoint.
"""

import requests
import json
import time
from typing import List, Dict, Any, Set

DATA_API_BASE_URL = "https://data-api.polymarket.com"
TIMEOUT = 20

def fetch_recently_traded_markets() -> Dict[str, Any]:
    """
    Fetch recently traded markets by getting recent trades and deduplicating by condition_id.
    
    Returns:
        Dictionary with unique markets and their token information
    """
    print("Fetching recent trades to find active markets...")
    
    try:
        # Get recent trades (newest first)
        response = requests.get(
            f"{DATA_API_BASE_URL}/trades?limit=500",
            timeout=TIMEOUT
        )
        response.raise_for_status()
        trades = response.json()
        
        print(f"Retrieved {len(trades)} recent trades")
        
        # Deduplicate by condition_id to get unique markets
        unique_markets = {}
        seen_conditions: Set[str] = set()
        
        for trade in trades:
            condition_id = trade.get("conditionId")  # Fixed field name
            if condition_id and condition_id not in seen_conditions:
                seen_conditions.add(condition_id)
                
                # Store market info from the trade
                unique_markets[condition_id] = {
                    "condition_id": condition_id,
                    "question": trade.get("title", ""),
                    "market_slug": trade.get("slug", ""),  # Fixed field name
                    "asset_id": trade.get("asset"),  # This is the token_id
                    "outcome": trade.get("outcome"),
                    "category": trade.get("category"),
                    "end_date": trade.get("end_date"),
                    "latest_trade_timestamp": trade.get("timestamp"),
                    "latest_trade_price": trade.get("price"),
                    "latest_trade_size": trade.get("size")
                }
        
        print(f"Found {len(unique_markets)} unique recently traded markets")
        return unique_markets
        
    except requests.RequestException as e:
        print(f"Error fetching recent trades: {e}")
        return {}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {}

def get_all_token_ids_for_markets(markets: Dict[str, Any]) -> List[str]:
    """
    Extract all unique token IDs from the markets.
    Note: Each trade only shows one token_id, but markets typically have 2 tokens.
    This gets the "active" tokens that are actually being traded.
    """
    token_ids = []
    
    for condition_id, market_data in markets.items():
        asset_id = market_data.get("asset_id")
        if asset_id:
            token_ids.append(asset_id)
    
    # Remove duplicates while preserving order
    unique_token_ids = []
    seen = set()
    for token_id in token_ids:
        if token_id not in seen:
            seen.add(token_id)
            unique_token_ids.append(token_id)
    
    return unique_token_ids

def fetch_all_markets_from_trades(limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Get a comprehensive list of recently active markets by fetching more trades.
    
    Args:
        limit: Number of trades to fetch
    """
    print(f"Fetching {limit} recent trades for comprehensive market list...")
    
    try:
        response = requests.get(
            f"{DATA_API_BASE_URL}/trades?limit={limit}",
            timeout=TIMEOUT
        )
        response.raise_for_status()
        trades = response.json()
        
        # Group trades by condition_id
        markets_by_condition = {}
        
        for trade in trades:
            condition_id = trade.get("conditionId")  # Fixed field name
            if not condition_id:
                continue
                
            if condition_id not in markets_by_condition:
                markets_by_condition[condition_id] = {
                    "condition_id": condition_id,
                    "question": trade.get("title", ""),
                    "market_slug": trade.get("slug", ""),  # Fixed field name
                    "category": trade.get("category"),
                    "end_date": trade.get("end_date"),
                    "token_ids": set(),
                    "outcomes": set(),
                    "latest_trade_timestamp": trade.get("timestamp"),
                    "trade_count": 0,
                    "total_volume": 0.0
                }
            
            market = markets_by_condition[condition_id]
            
            # Add token_id and outcome
            if trade.get("asset"):
                market["token_ids"].add(trade.get("asset"))
            if trade.get("outcome"):
                market["outcomes"].add(trade.get("outcome"))
            
            # Update stats
            market["trade_count"] += 1
            if trade.get("size"):
                market["total_volume"] += float(trade.get("size", 0))
            
            # Keep latest timestamp
            if trade.get("timestamp") and trade["timestamp"] > market.get("latest_trade_timestamp", ""):
                market["latest_trade_timestamp"] = trade["timestamp"]
        
        # Convert sets to lists for JSON serialization
        markets_list = []
        for market in markets_by_condition.values():
            market["token_ids"] = list(market["token_ids"])
            market["outcomes"] = list(market["outcomes"])
            markets_list.append(market)
        
        # Sort by trade count (most active first)
        markets_list.sort(key=lambda x: x["trade_count"], reverse=True)
        
        print(f"Found {len(markets_list)} unique markets from {len(trades)} trades")
        return markets_list
        
    except requests.RequestException as e:
        print(f"Error fetching trades: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []

def save_results(token_ids: List[str], markets: List[Dict[str, Any]]):
    """Save results to JSON files."""
    
    # Save token IDs
    with open("token_ids.json", "w") as f:
        json.dump({
            "timestamp": time.time(),
            "count": len(token_ids),
            "token_ids": token_ids
        }, f, indent=2)
    
    # Save market details
    with open("markets.json", "w") as f:
        json.dump({
            "timestamp": time.time(),
            "count": len(markets),
            "markets": markets
        }, f, indent=2)
    
    print(f"Saved {len(token_ids)} token IDs to token_ids.json")
    print(f"Saved {len(markets)} market details to markets.json")

def main():
    """Main execution function."""
    print("=== Polymarket Recently Traded Markets Fetcher ===\n")
    
    # Fetch comprehensive market data from recent trades
    markets = fetch_all_markets_from_trades(limit=10000)
    
    # Extract all unique token IDs
    all_token_ids = []
    for market in markets:
        all_token_ids.extend(market.get("token_ids", []))
    
    # Remove duplicates
    unique_token_ids = list(set(all_token_ids))
    
    print(f"\n=== Summary ===")
    print(f"Recently active markets: {len(markets)}")
    print(f"Total unique token IDs: {len(unique_token_ids)}")
    
    if markets:
        # Show top 10 most active markets
        print(f"\nTop 10 most active markets:")
        for i, market in enumerate(markets[:10], 1):
            question = market.get("question", "No question")[:60]
            trade_count = market.get("trade_count", 0)
            volume = market.get("total_volume", 0)
            print(f"  {i:2}. {question}... ({trade_count} trades, ${volume:.0f})")
        
        # Show markets by category
        categories = {}
        for market in markets:
            cat = market.get("category", "Unknown")
            categories[cat] = categories.get(cat, 0) + 1
        
        print(f"\nMarkets by category:")
        for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"  {cat}: {count}")
    
    # Save results
    save_results(unique_token_ids, markets)
    
    print(f"\nAll {len(unique_token_ids)} token IDs ready for WebSocket subscription!")

if __name__ == "__main__":
    main()