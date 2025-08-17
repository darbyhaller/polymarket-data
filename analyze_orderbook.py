#!/usr/bin/env python3
"""
Analyze orderbook_clip.jsonl file to gather statistics on Polymarket L2 orderbook data.
"""

import json
import pandas as pd
from collections import defaultdict, Counter
from datetime import datetime, timezone
import numpy as np
from typing import Dict, List, Any, Optional
import argparse
import sys
import os

class OrderbookAnalyzer:
    def __init__(self, jsonl_file: str = "orderbook_clip.jsonl"):
        """Initialize the analyzer with the JSONL file path."""
        self.jsonl_file = jsonl_file
        self.events = []
        self.stats = {}
        
    def load_data(self) -> bool:
        """Load data from the JSONL file."""
        if not os.path.exists(self.jsonl_file):
            print(f"Error: File {self.jsonl_file} not found.")
            return False
            
        print(f"Loading data from {self.jsonl_file}...")
        try:
            with open(self.jsonl_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    if line.strip():
                        try:
                            event = json.loads(line)
                            self.events.append(event)
                        except json.JSONDecodeError as e:
                            print(f"Warning: Skipping malformed JSON at line {line_num}: {e}")
                            continue
            
            print(f"Successfully loaded {len(self.events)} events.")
            return len(self.events) > 0
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return False
    
    def analyze_basic_stats(self):
        """Analyze basic statistics about the events."""
        if not self.events:
            return
            
        print("\n" + "="*50)
        print("BASIC EVENT STATISTICS")
        print("="*50)
        
        # Event type distribution
        event_types = Counter(event.get('event_type', 'unknown') for event in self.events)
        print(f"\nEvent Type Distribution:")
        for event_type, count in event_types.most_common():
            percentage = (count / len(self.events)) * 100
            print(f"  {event_type:20}: {count:6} ({percentage:5.1f}%)")
        
        # Time range analysis
        timestamps = [event.get('ts_ms') for event in self.events if event.get('ts_ms')]
        if timestamps:
            start_time = min(timestamps)
            end_time = max(timestamps)
            duration_seconds = (end_time - start_time) / 1000
            
            start_dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)
            
            print(f"\nTime Range:")
            print(f"  Start: {start_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"  End:   {end_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"  Duration: {duration_seconds:.1f} seconds ({duration_seconds/60:.1f} minutes)")
            
            if duration_seconds > 0:
                events_per_second = len(self.events) / duration_seconds
                print(f"  Rate: {events_per_second:.2f} events/second")
        
        self.stats['basic'] = {
            'total_events': len(self.events),
            'event_types': dict(event_types),
            'duration_seconds': duration_seconds if timestamps else 0
        }
    
    def analyze_markets_and_assets(self):
        """Analyze market and asset statistics."""
        print("\n" + "="*50)
        print("MARKET & ASSET ANALYSIS")
        print("="*50)
        
        # Market analysis
        markets = Counter()
        assets = Counter()
        outcomes = Counter()
        
        for event in self.events:
            market_title = event.get('market_title', '').strip()
            asset_id = event.get('asset_id', '').strip()
            outcome = event.get('outcome', '').strip()
            
            if market_title:
                markets[market_title] += 1
            if asset_id:
                assets[asset_id] += 1
            if outcome:
                outcomes[outcome] += 1
        
        print(f"\nMarket Activity (Top 10):")
        for market, count in markets.most_common(10):
            print(f"  {market[:60]:60}: {count:6} events")
        
        print(f"\nAsset Activity (Top 10):")
        for asset, count in assets.most_common(10):
            print(f"  {asset:42}: {count:6} events")
            
        print(f"\nOutcome Distribution:")
        for outcome, count in outcomes.most_common():
            percentage = (count / len(self.events)) * 100
            print(f"  {outcome:10}: {count:6} events ({percentage:5.1f}%)")
        
        print(f"\nSummary:")
        print(f"  Total unique markets: {len(markets)}")
        print(f"  Total unique assets:  {len(assets)}")
        print(f"  Total unique outcomes: {len(outcomes)}")
        
        self.stats['markets'] = {
            'unique_markets': len(markets),
            'unique_assets': len(assets),
            'unique_outcomes': len(outcomes),
            'top_markets': dict(markets.most_common(10)),
            'outcomes': dict(outcomes)
        }
    
    def analyze_orderbook_data(self):
        """Analyze orderbook-specific metrics."""
        print("\n" + "="*50)
        print("ORDERBOOK ANALYSIS")
        print("="*50)
        
        book_events = [e for e in self.events if e.get('event_type') == 'book']
        price_change_events = [e for e in self.events if e.get('event_type') == 'price_change']
        
        print(f"Book snapshot events: {len(book_events)}")
        print(f"Price change events:  {len(price_change_events)}")
        
        if book_events:
            self._analyze_book_snapshots(book_events)
        
        if price_change_events:
            self._analyze_price_changes(price_change_events)
    
    def _analyze_book_snapshots(self, book_events: List[Dict[str, Any]]):
        """Analyze book snapshot events."""
        print(f"\nBook Snapshot Analysis:")
        
        spreads = []
        bid_depths = []
        ask_depths = []
        
        for event in book_events:
            bids = event.get('bids', [])
            asks = event.get('asks', [])
            
            if bids and asks:
                try:
                    best_bid = max(float(bid.get('price', 0)) for bid in bids if bid.get('price'))
                    best_ask = min(float(ask.get('price', float('inf'))) for ask in asks if ask.get('price'))
                    
                    if best_bid > 0 and best_ask < float('inf'):
                        spread = best_ask - best_bid
                        if spread >= 0:  # Valid spread
                            spreads.append(spread)
                            
                            # Calculate relative spread (bps)
                            mid_price = (best_bid + best_ask) / 2
                            if mid_price > 0:
                                spread_bps = (spread / mid_price) * 10000
                                
                except (ValueError, TypeError):
                    continue
            
            # Calculate depth
            if bids:
                bid_depth = sum(float(bid.get('size', 0)) for bid in bids if bid.get('size'))
                bid_depths.append(bid_depth)
            
            if asks:
                ask_depth = sum(float(ask.get('size', 0)) for ask in asks if ask.get('size'))
                ask_depths.append(ask_depth)
        
        if spreads:
            print(f"  Spread Statistics:")
            print(f"    Count: {len(spreads)}")
            print(f"    Mean:  {np.mean(spreads):.6f}")
            print(f"    Std:   {np.std(spreads):.6f}")
            print(f"    Min:   {np.min(spreads):.6f}")
            print(f"    Max:   {np.max(spreads):.6f}")
        
        if bid_depths:
            print(f"  Bid Depth Statistics:")
            print(f"    Mean:  {np.mean(bid_depths):.2f}")
            print(f"    Std:   {np.std(bid_depths):.2f}")
        
        if ask_depths:
            print(f"  Ask Depth Statistics:")
            print(f"    Mean:  {np.mean(ask_depths):.2f}")
            print(f"    Std:   {np.std(ask_depths):.2f}")
    
    def _analyze_price_changes(self, price_change_events: List[Dict[str, Any]]):
        """Analyze price change events."""
        print(f"\nPrice Change Analysis:")
        
        total_changes = 0
        side_distribution = Counter()
        
        for event in price_change_events:
            changes = event.get('changes', [])
            total_changes += len(changes)
            
            for change in changes:
                side = change.get('side', 'unknown')
                side_distribution[side] += 1
        
        print(f"  Total price level changes: {total_changes}")
        print(f"  Side distribution:")
        for side, count in side_distribution.items():
            percentage = (count / total_changes) * 100 if total_changes > 0 else 0
            print(f"    {side:8}: {count:6} ({percentage:5.1f}%)")
    
    def analyze_trades(self):
        """Analyze trade data."""
        print("\n" + "="*50)
        print("TRADE ANALYSIS")
        print("="*50)
        
        trade_events = [e for e in self.events if e.get('event_type') == 'last_trade_price']
        
        if not trade_events:
            print("No trade events found.")
            return
        
        print(f"Total trade events: {len(trade_events)}")
        
        # Trade side distribution
        sides = Counter(event.get('side', 'unknown') for event in trade_events)
        print(f"\nTrade Side Distribution:")
        for side, count in sides.items():
            percentage = (count / len(trade_events)) * 100
            print(f"  {side:8}: {count:6} ({percentage:5.1f}%)")
        
        # Trade size analysis
        sizes = []
        prices = []
        
        for event in trade_events:
            try:
                size = float(event.get('size', 0))
                price = float(event.get('price', 0))
                if size > 0:
                    sizes.append(size)
                if price > 0:
                    prices.append(price)
            except (ValueError, TypeError):
                continue
        
        if sizes:
            print(f"\nTrade Size Statistics:")
            print(f"  Count: {len(sizes)}")
            print(f"  Mean:  {np.mean(sizes):.2f}")
            print(f"  Std:   {np.std(sizes):.2f}")
            print(f"  Min:   {np.min(sizes):.2f}")
            print(f"  Max:   {np.max(sizes):.2f}")
        
        if prices:
            print(f"\nTrade Price Statistics:")
            print(f"  Count: {len(prices)}")
            print(f"  Mean:  {np.mean(prices):.6f}")
            print(f"  Std:   {np.std(prices):.6f}")
            print(f"  Min:   {np.min(prices):.6f}")
            print(f"  Max:   {np.max(prices):.6f}")
    
    def generate_summary_report(self):
        """Generate a summary report of all statistics."""
        print("\n" + "="*50)
        print("SUMMARY REPORT")
        print("="*50)
        
        if not self.stats:
            print("No statistics available. Run analysis first.")
            return
        
        basic_stats = self.stats.get('basic', {})
        market_stats = self.stats.get('markets', {})
        
        print(f"Data Overview:")
        print(f"  File: {self.jsonl_file}")
        print(f"  Total Events: {basic_stats.get('total_events', 0):,}")
        print(f"  Duration: {basic_stats.get('duration_seconds', 0):.1f} seconds")
        print(f"  Unique Markets: {market_stats.get('unique_markets', 0)}")
        print(f"  Unique Assets: {market_stats.get('unique_assets', 0)}")
        
        event_types = basic_stats.get('event_types', {})
        print(f"\nEvent Breakdown:")
        for event_type, count in event_types.items():
            print(f"  {event_type}: {count:,}")
        
        print(f"\nTop Markets by Activity:")
        top_markets = market_stats.get('top_markets', {})
        for i, (market, count) in enumerate(list(top_markets.items())[:5], 1):
            print(f"  {i}. {market[:50]}... ({count:,} events)")
    
    def run_full_analysis(self):
        """Run the complete analysis pipeline."""
        if not self.load_data():
            return False
        
        self.analyze_basic_stats()
        self.analyze_markets_and_assets()
        self.analyze_orderbook_data()
        self.analyze_trades()
        self.generate_summary_report()
        
        return True

def main():
    parser = argparse.ArgumentParser(description='Analyze Polymarket orderbook data from JSONL file')
    parser.add_argument('--file', '-f', default='orderbook_clip.jsonl', 
                       help='Path to JSONL file (default: orderbook_clip.jsonl)')
    parser.add_argument('--basic', action='store_true', help='Run only basic statistics')
    parser.add_argument('--markets', action='store_true', help='Run only market analysis')
    parser.add_argument('--orderbook', action='store_true', help='Run only orderbook analysis')
    parser.add_argument('--trades', action='store_true', help='Run only trade analysis')
    
    args = parser.parse_args()
    
    analyzer = OrderbookAnalyzer(args.file)
    
    if not analyzer.load_data():
        sys.exit(1)
    
    # If no specific analysis is requested, run full analysis
    if not any([args.basic, args.markets, args.orderbook, args.trades]):
        analyzer.run_full_analysis()
    else:
        # Run specific analyses
        if args.basic:
            analyzer.analyze_basic_stats()
        if args.markets:
            analyzer.analyze_markets_and_assets()
        if args.orderbook:
            analyzer.analyze_orderbook_data()
        if args.trades:
            analyzer.analyze_trades()
        
        analyzer.generate_summary_report()

if __name__ == "__main__":
    main()