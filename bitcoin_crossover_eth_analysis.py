#!/usr/bin/env python3
"""
Bitcoin Crossover Multi-Crypto Analysis.
Detects when Bitcoin bid reaches WINDOW ms ago's ask (or vice versa) and measures crypto price changes.
"""

import json
import sys
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import deque

# ============================================================================
# CONFIGURATION - Modify these lists to change which cryptos to analyze
# ============================================================================

# Independent variables (cryptos that trigger crossover events)
INDEPENDENT_CRYPTOS = ['bitcoin']

# Dependent variables (cryptos whose responses we measure)
DEPENDENT_CRYPTOS = ['ethereum', 'solana', 'xrp']

# Color mapping for each crypto (used in plots)
CRYPTO_COLORS = {
    'bitcoin': 'red',
    'ethereum': 'blue',
    'solana': 'purple',
    'xrp': 'orange'
}

# Short names for display (maps full name to abbreviation)
CRYPTO_SHORT_NAMES = {
    'bitcoin': 'BTC',
    'ethereum': 'ETH',
    'solana': 'SOL',
    'xrp': 'XRP'
}

# ============================================================================

def load_crypto_data(filename: str, title_filter: str = None) -> Dict[str, pd.DataFrame]:
    """Load crypto L1 data for all configured cryptos with optional filtering."""
    all_cryptos = INDEPENDENT_CRYPTOS + DEPENDENT_CRYPTOS
    print(f"Loading crypto data from {filename}...")
    print(f"Looking for: {', '.join(all_cryptos)}")
    if title_filter:
        print(f"Filtering for markets containing: '{title_filter}'")
    
    # Initialize data containers for each crypto
    crypto_data = {crypto: [] for crypto in all_cryptos}
    
    with open(filename, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                record = json.loads(line)
                
                market_title = record.get('market_title', '').lower()
                
                # Apply additional title filter if specified
                if title_filter and title_filter.lower() not in market_title:
                    continue
                
                # Separate crypto data by type
                for crypto in all_cryptos:
                    if crypto in market_title:
                        crypto_data[crypto].append(record)
                        break  # Only assign to first matching crypto
                
                if line_num % 10000 == 0:
                    print(f"Processed {line_num} records...")
                    
            except json.JSONDecodeError as e:
                print(f"JSON decode error on line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    # Print loaded record counts
    for crypto in all_cryptos:
        count = len(crypto_data[crypto])
        print(f"Loaded {count} {crypto.title()} records")
    
    # Convert to DataFrames
    crypto_dfs = {}
    for crypto in all_cryptos:
        df = pd.DataFrame(crypto_data[crypto]) if crypto_data[crypto] else pd.DataFrame()
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['ts_ms'], unit='ms')
            # Convert price columns to numeric
            for col in ['best_bid_price', 'best_ask_price', 'mid_price']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        crypto_dfs[crypto] = df
    
    return crypto_dfs

class CrossoverDetector:
    """Detects Bitcoin bid/ask crossovers and measures responses from multiple dependent variables."""
    
    def __init__(self, window_ms: int = 1000):
        self.window_ms = window_ms
        self.crossover_events = []
        
    def detect_crossovers(self, bitcoin_df: pd.DataFrame) -> List[Dict]:
        """
        Detect when Bitcoin bid reaches WINDOW ms ago's ask, or vice versa.
        
        Returns list of crossover events with timing and price information.
        """
        print(f"Detecting Bitcoin crossovers with {self.window_ms}ms window...")
        
        # Sort by timestamp
        bitcoin_df = bitcoin_df.sort_values('ts_ms').copy()
        
        # Remove rows with missing bid/ask data
        bitcoin_df = bitcoin_df.dropna(subset=['best_bid_price', 'best_ask_price'])
        
        if len(bitcoin_df) < 2:
            print("Not enough Bitcoin data for crossover detection")
            return []
        
        crossovers = []
        
        # Use a sliding window approach
        for i in range(len(bitcoin_df)):
            current_row = bitcoin_df.iloc[i]
            current_time = current_row['ts_ms']
            current_bid = current_row['best_bid_price']
            current_ask = current_row['best_ask_price']
            
            # Find data from WINDOW ms ago
            window_start_time = current_time - self.window_ms
            
            # Get the closest record to window_start_time
            time_diffs = abs(bitcoin_df['ts_ms'] - window_start_time)
            closest_idx = time_diffs.idxmin()
            
            # Only consider if the closest record is reasonably close to our target time
            if time_diffs[closest_idx] > self.window_ms * 0.5:  # Allow 50% tolerance
                continue
                
            window_row = bitcoin_df.loc[closest_idx]
            window_bid = window_row['best_bid_price']
            window_ask = window_row['best_ask_price']
            
            # Check for crossovers
            # Type 1: Current bid >= window ask (bid crosses up through ask)
            if current_bid >= window_ask:
                crossovers.append({
                    'type': 'bid_crosses_ask',
                    'time': current_time,
                    'datetime': current_row['datetime'],
                    'current_bid': current_bid,
                    'current_ask': current_ask,
                    'window_bid': window_bid,
                    'window_ask': window_ask,
                    'crossover_amount': current_bid - window_ask,
                    'asset_id': current_row['asset_id'],
                    'market_title': current_row['market_title']
                })
            
            # Type 2: Current ask <= window bid (ask crosses down through bid)  
            if current_ask <= window_bid:
                crossovers.append({
                    'type': 'ask_crosses_bid',
                    'time': current_time,
                    'datetime': current_row['datetime'],
                    'current_bid': current_bid,
                    'current_ask': current_ask,
                    'window_bid': window_bid,
                    'window_ask': window_ask,
                    'crossover_amount': window_bid - current_ask,
                    'asset_id': current_row['asset_id'],
                    'market_title': current_row['market_title']
                })
        
        print(f"Found {len(crossovers)} Bitcoin crossover events")
        return crossovers
    def measure_crypto_responses(self, crossovers: List[Dict], crypto_dfs: Dict[str, pd.DataFrame],
                               stability_threshold: float = 0.001) -> List[Dict]:
        """
        For each crossover event, measure price changes for multiple cryptocurrencies at 200ms, 400ms, 600ms, 800ms, 1000ms intervals.
        Filter out events where any crypto has already changed significantly in the last second.
        
        Args:
            crossovers: List of crossover events
            crypto_dfs: Dictionary with crypto names as keys and DataFrames as values
            stability_threshold: Maximum allowed price change in the last 1000ms (default: 0.001 = 0.1%)
        """
        crypto_names = list(crypto_dfs.keys())
        print(f"Measuring {', '.join(crypto_names)} responses for {len(crossovers)} crossover events...")
        print(f"Filtering out events where any crypto changed more than {stability_threshold*100:.1f}% in the last 1000ms...")
        
        # Sort all crypto data by timestamp and clean
        for name, df in crypto_dfs.items():
            crypto_dfs[name] = df.sort_values('ts_ms').copy().dropna(subset=['mid_price'])
            if len(crypto_dfs[name]) == 0:
                print(f"No {name} data available for response measurement")
        
        # Remove empty dataframes
        crypto_dfs = {name: df for name, df in crypto_dfs.items() if len(df) > 0}
        
        if not crypto_dfs:
            print("No crypto data available for response measurement")
            return []
        
        response_intervals = [200, 400, 600, 800, 1000]  # 200ms, 400ms, 600ms, 800ms, 1000ms
        results = []
        filtered_count = 0
        
        for crossover in crossovers:
            crossover_time = crossover['time']
            
            # Check stability and get baseline prices for all cryptos
            crypto_prices_before = {}
            stable_event = True
            
            for name, df in crypto_dfs.items():
                # Find price at crossover time (or closest before)
                before_mask = df['ts_ms'] <= crossover_time
                if not before_mask.any():
                    stable_event = False
                    break
                    
                before_idx = df[before_mask]['ts_ms'].idxmax()
                price_before = df.loc[before_idx, 'mid_price']
                crypto_prices_before[name] = price_before
                
                # Check stability in the last 1000ms before crossover
                stability_check_time = crossover_time - 1000  # 1 second ago
                stability_mask = df['ts_ms'] <= stability_check_time
                
                if stability_mask.any():
                    stability_idx = df[stability_mask]['ts_ms'].idxmax()
                    price_1s_ago = df.loc[stability_idx, 'mid_price']
                    
                    # Calculate relative price change in the last second
                    if price_1s_ago > 0:
                        change_last_1s = abs(price_before - price_1s_ago) / price_1s_ago
                        
                        # Filter out if this crypto has changed too much in the last second
                        if change_last_1s > stability_threshold:
                            stable_event = False
                            break
            
            if not stable_event:
                filtered_count += 1
                continue
            
            # Measure price changes at each interval for all cryptos
            all_interval_changes = {}
            
            for name, df in crypto_dfs.items():
                interval_changes = {}
                price_before = crypto_prices_before[name]
                
                for interval_ms in response_intervals:
                    target_time = crossover_time + interval_ms
                    
                    # Find closest price at target time
                    time_diffs = abs(df['ts_ms'] - target_time)
                    if len(time_diffs) == 0:
                        continue
                        
                    closest_idx = time_diffs.idxmin()
                    
                    # Only use if reasonably close to target time (within 50% of interval)
                    if time_diffs[closest_idx] <= interval_ms * 0.5:
                        price_after = df.loc[closest_idx, 'mid_price']
                        price_change = price_after - price_before
                        interval_changes[f'{interval_ms}ms'] = price_change
                
                all_interval_changes[name] = interval_changes
            
            # Only include if we have at least some interval measurements for at least one crypto
            if any(changes for changes in all_interval_changes.values()):
                result = crossover.copy()
                for name in crypto_names:
                    result[f'{name.lower()}_price_before'] = crypto_prices_before.get(name, 0)
                    result[f'{name.lower()}_changes'] = all_interval_changes.get(name, {})
                results.append(result)
        
        print(f"Filtered out {filtered_count} events where cryptos were unstable in the last 1000ms")
        print(f"Successfully measured responses for {len(results)} crossover events")
        return results

def analyze_crossover_results(results: List[Dict], crypto_names: List[str]) -> Dict:
    """Analyze and average the crossover results for multiple cryptocurrencies."""
    if not results:
        return {}
    
    print(f"Analyzing {len(results)} crossover events with {', '.join(crypto_names)} responses...")
    
    # Separate by crossover type
    bid_crosses_ask = [r for r in results if r['type'] == 'bid_crosses_ask']
    ask_crosses_bid = [r for r in results if r['type'] == 'ask_crosses_bid']
    
    analysis = {
        'total_events': len(results),
        'bid_crosses_ask_count': len(bid_crosses_ask),
        'ask_crosses_bid_count': len(ask_crosses_bid),
        'intervals': ['200ms', '400ms', '600ms', '800ms', '1000ms'],
        'crypto_names': crypto_names
    }
    
    # Calculate averages for each type, crypto, and interval
    for event_type, events in [('bid_crosses_ask', bid_crosses_ask), ('ask_crosses_bid', ask_crosses_bid)]:
        if not events:
            continue
            
        type_analysis = {
            'count': len(events),
            'avg_crossover_amount': np.mean([e['crossover_amount'] for e in events]),
            'crypto_responses': {}
        }
        
        # Calculate average changes for each crypto and interval
        for crypto_name in crypto_names:
            crypto_key = f'{crypto_name.lower()}_changes'
            crypto_analysis = {'interval_averages': {}}
            
            for interval in analysis['intervals']:
                changes = []
                for event in events:
                    if crypto_key in event and interval in event[crypto_key]:
                        changes.append(event[crypto_key][interval])
                
                if changes:
                    crypto_analysis['interval_averages'][interval] = {
                        'mean': np.mean(changes),
                        'std': np.std(changes),
                        'count': len(changes),
                        'median': np.median(changes)
                    }
            
            type_analysis['crypto_responses'][crypto_name] = crypto_analysis
        
        analysis[event_type] = type_analysis
    
    return analysis

def create_analysis_plot(analysis: Dict, output_file: str = None):
    """Create visualization of crossover analysis results for multiple cryptocurrencies."""
    if not analysis or analysis['total_events'] == 0:
        print("No data to plot")
        return None
    
    crypto_names = analysis.get('crypto_names', [])
    intervals = analysis['intervals']
    
    if not crypto_names:
        print("No crypto names found in analysis")
        return None
    
    # Calculate grid size based on number of cryptos (2 plots per crypto)
    num_plots = len(crypto_names) * 2
    rows = (num_plots + 1) // 2  # Round up division
    cols = 2
    
    fig, axes = plt.subplots(rows, cols, figsize=(16, 6 * rows))
    
    # Ensure axes is always 2D for consistent indexing
    if rows == 1:
        axes = axes.reshape(1, -1)
    
    plot_idx = 0
    for crypto_name in crypto_names:
        # Get color from configuration, fallback to gray
        crypto_color = CRYPTO_COLORS.get(crypto_name.lower(), 'gray')
        
        # Plot for bid crosses ask
        ax_bid = axes[plot_idx // 2, plot_idx % 2]
        
        if ('bid_crosses_ask' in analysis and
            'crypto_responses' in analysis['bid_crosses_ask'] and
            crypto_name in analysis['bid_crosses_ask']['crypto_responses'] and
            analysis['bid_crosses_ask']['crypto_responses'][crypto_name]['interval_averages']):
            
            bid_data = analysis['bid_crosses_ask']['crypto_responses'][crypto_name]['interval_averages']
            means = [bid_data.get(interval, {}).get('mean', 0) for interval in intervals]
            stds = [bid_data.get(interval, {}).get('std', 0) for interval in intervals]
            
            ax_bid.bar(intervals, means, yerr=stds, alpha=0.7, color=crypto_color, capsize=5)
            
            # Get independent crypto name for title
            independent_crypto = CRYPTO_SHORT_NAMES.get(INDEPENDENT_CRYPTOS[0], INDEPENDENT_CRYPTOS[0].upper())
            ax_bid.set_title(f'{crypto_name} Response to {independent_crypto} Bid Crossing Ask\n({analysis["bid_crosses_ask"]["count"]} events)')
        else:
            ax_bid.text(0.5, 0.5, f'No bid crosses ask events\nfor {crypto_name}',
                       ha='center', va='center', transform=ax_bid.transAxes)
            independent_crypto = CRYPTO_SHORT_NAMES.get(INDEPENDENT_CRYPTOS[0], INDEPENDENT_CRYPTOS[0].upper())
            ax_bid.set_title(f'{crypto_name} Response to {independent_crypto} Bid Crossing Ask')
        
        ax_bid.set_ylabel(f'Average {crypto_name} Price Change')
        ax_bid.set_xlabel('Time Interval')
        ax_bid.grid(True, alpha=0.3)
        ax_bid.axhline(y=0, color='black', linestyle='-', alpha=0.5)
        
        plot_idx += 1
        
        # Plot for ask crosses bid
        ax_ask = axes[plot_idx // 2, plot_idx % 2]
        
        if ('ask_crosses_bid' in analysis and
            'crypto_responses' in analysis['ask_crosses_bid'] and
            crypto_name in analysis['ask_crosses_bid']['crypto_responses'] and
            analysis['ask_crosses_bid']['crypto_responses'][crypto_name]['interval_averages']):
            
            ask_data = analysis['ask_crosses_bid']['crypto_responses'][crypto_name]['interval_averages']
            means = [ask_data.get(interval, {}).get('mean', 0) for interval in intervals]
            stds = [ask_data.get(interval, {}).get('std', 0) for interval in intervals]
            
            ax_ask.bar(intervals, means, yerr=stds, alpha=0.7, color=crypto_color, capsize=5)
            
            # Get independent crypto name for title
            independent_crypto = CRYPTO_SHORT_NAMES.get(INDEPENDENT_CRYPTOS[0], INDEPENDENT_CRYPTOS[0].upper())
            ax_ask.set_title(f'{crypto_name} Response to {independent_crypto} Ask Crossing Bid\n({analysis["ask_crosses_bid"]["count"]} events)')
        else:
            ax_ask.text(0.5, 0.5, f'No ask crosses bid events\nfor {crypto_name}',
                       ha='center', va='center', transform=ax_ask.transAxes)
            independent_crypto = CRYPTO_SHORT_NAMES.get(INDEPENDENT_CRYPTOS[0], INDEPENDENT_CRYPTOS[0].upper())
            ax_ask.set_title(f'{crypto_name} Response to {independent_crypto} Ask Crossing Bid')
        
        ax_ask.set_ylabel(f'Average {crypto_name} Price Change')
        ax_ask.set_xlabel('Time Interval')
        ax_ask.grid(True, alpha=0.3)
        ax_ask.axhline(y=0, color='black', linestyle='-', alpha=0.5)
        
        plot_idx += 1
    
    # Hide any unused subplots
    for i in range(plot_idx, rows * cols):
        axes[i // cols, i % cols].set_visible(False)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Analysis plot saved as: {output_file}")
    
    return fig

def extract_time_period(market_title: str) -> Optional[str]:
    """
    Extract time period from market title.
    E.g., "Bitcoin Up or Down - August 24, 6AM ET" -> "August 24, 6AM ET"
    """
    import re
    
    # Look for patterns like "August 24, 6AM ET" or "August 23, 3PM ET"
    pattern = r'(August \d+, \d+[AP]M ET)'
    match = re.search(pattern, market_title)
    if match:
        return match.group(1)
    
    # Look for other date/time patterns if needed
    # Add more patterns here as needed
    
    return None

def find_matching_markets(crypto_dfs: Dict[str, pd.DataFrame]) -> List[Dict]:
    """
    Find independent crypto markets and their corresponding dependent crypto markets with matching time periods.
    Returns list of market groups with matching time periods.
    """
    independent_crypto = INDEPENDENT_CRYPTOS[0]  # Use first independent crypto
    independent_short = CRYPTO_SHORT_NAMES.get(independent_crypto, independent_crypto.upper())
    
    dependent_shorts = [CRYPTO_SHORT_NAMES.get(crypto, crypto.upper()) for crypto in DEPENDENT_CRYPTOS]
    print(f"Finding {independent_short} markets with matching {', '.join(dependent_shorts)} markets...")
    
    independent_df = crypto_dfs.get(independent_crypto, pd.DataFrame())
    if independent_df.empty:
        print(f"No {independent_crypto} data available")
        return []
    
    # Get all unique independent crypto markets that match "Up or Down" pattern
    independent_markets = []
    for _, row in independent_df.iterrows():
        title = row['market_title']
        if 'up or down' in title.lower() and 'august' in title.lower():
            time_period = extract_time_period(title)
            if time_period:
                independent_markets.append({
                    'asset_id': row['asset_id'],
                    'title': title,
                    'time_period': time_period
                })
    
    # Remove duplicates based on asset_id
    independent_markets = {m['asset_id']: m for m in independent_markets}.values()
    independent_markets = list(independent_markets)
    
    print(f"Found {len(independent_markets)} {independent_short} 'Up or Down' markets")
    
    # For each independent market, find matching dependent markets
    market_groups = []
    
    for indep_market in independent_markets:
        time_period = indep_market['time_period']
        print(f"\nLooking for matches for {independent_short} market: {indep_market['title']}")
        print(f"Time period: {time_period}")
        
        group = {
            'time_period': time_period,
            independent_crypto: indep_market,
            'matches': {}
        }
        
        # Find matching dependent markets
        for dependent_crypto in DEPENDENT_CRYPTOS:
            dependent_df = crypto_dfs.get(dependent_crypto, pd.DataFrame())
            if dependent_df.empty:
                continue
                
            dependent_short = CRYPTO_SHORT_NAMES.get(dependent_crypto, dependent_crypto.upper())
            
            for _, row in dependent_df.iterrows():
                title = row['market_title']
                if ('up or down' in title.lower() and
                    dependent_crypto in title.lower() and
                    time_period in title):
                    group['matches'][dependent_short] = {
                        'asset_id': row['asset_id'],
                        'title': title,
                        'crypto_name': dependent_crypto
                    }
                    print(f"  Found {dependent_short} match: {title}")
                    break
        
        # Only include groups that have at least one match
        if group['matches']:
            market_groups.append(group)
            print(f"  Added group with {len(group['matches'])} matches")
        else:
            print(f"  No matches found for this {independent_short} market")
    
    print(f"\nFound {len(market_groups)} {independent_short} markets with at least one matching crypto market")
    return market_groups

def main():
    independent_crypto = INDEPENDENT_CRYPTOS[0]
    independent_short = CRYPTO_SHORT_NAMES.get(independent_crypto, independent_crypto.upper())
    dependent_shorts = [CRYPTO_SHORT_NAMES.get(crypto, crypto.upper()) for crypto in DEPENDENT_CRYPTOS]
    
    parser = argparse.ArgumentParser(
        description=f'{independent_short} Crossover Multi-Crypto Analysis - Detects when {independent_short} bid reaches WINDOW ms ago\'s ask (or vice versa) and measures {", ".join(dependent_shorts)} price changes.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python bitcoin_crossover_eth_analysis.py l1_data.jsonl
  python bitcoin_crossover_eth_analysis.py l1_data.jsonl --window 500
  python bitcoin_crossover_eth_analysis.py l1_data.jsonl --title-filter "12AM ET" --window 2000
  python bitcoin_crossover_eth_analysis.py l1_data.jsonl --stability-threshold 0.005
        '''
    )
    
    parser.add_argument('input_file',
                       help='Path to the L1 data file (JSONL format)')
    
    parser.add_argument('--window', '-w',
                       type=int,
                       default=1000,
                       help='Analysis window in milliseconds (default: 1000ms)')
    
    parser.add_argument('--title-filter', '-t',
                       help='Filter markets by title containing this string')
    
    parser.add_argument('--stability-threshold', '-s',
                       type=float,
                       default=0.001,
                       help='Maximum allowed price change in the last 1000ms to consider stable (default: 0.001 = 0.1%%)')
    
    args = parser.parse_args()
    
    input_file = args.input_file
    title_filter = args.title_filter
    window_ms = args.window
    stability_threshold = args.stability_threshold
    
    try:
        # Load all configured crypto data
        crypto_dfs = load_crypto_data(input_file, title_filter)
        
        if crypto_dfs.get(independent_crypto, pd.DataFrame()).empty:
            print(f"Need {independent_crypto} data for crossover detection!")
            sys.exit(1)
        
        # Find matching markets by time period
        market_groups = find_matching_markets(crypto_dfs)
        
        if not market_groups:
            print(f"No {independent_short} markets with matching dependent crypto markets found!")
            sys.exit(1)
        
        # Process each market group
        all_results = []
        all_crypto_names = set()
        
        for group in market_groups:
            print(f"\n" + "="*80)
            print(f"PROCESSING MARKET GROUP: {group['time_period']}")
            print(f"="*80)
            
            # Get independent crypto data for this group
            independent_asset_id = group[independent_crypto]['asset_id']
            independent_data = crypto_dfs[independent_crypto][crypto_dfs[independent_crypto]['asset_id'] == independent_asset_id].copy()
            
            if independent_data.empty:
                print(f"No {independent_crypto} data found for asset {independent_asset_id}")
                continue
            
            # Prepare dependent variable data for this group
            group_crypto_dfs = {}
            crypto_names = []
            
            for crypto_short, match_info in group['matches'].items():
                crypto_name = match_info['crypto_name']
                if crypto_name in DEPENDENT_CRYPTOS and not crypto_dfs.get(crypto_name, pd.DataFrame()).empty:
                    crypto_asset_id = match_info['asset_id']
                    crypto_data = crypto_dfs[crypto_name][crypto_dfs[crypto_name]['asset_id'] == crypto_asset_id].copy()
                    if not crypto_data.empty:
                        group_crypto_dfs[crypto_short] = crypto_data
                        crypto_names.append(crypto_short)
                        all_crypto_names.add(crypto_short)
            
            if not group_crypto_dfs:
                print(f"No matching crypto data found for this group")
                continue
            
            print(f"\nAnalyzing:")
            print(f"{independent_short}: {independent_data['market_title'].iloc[0]} ({len(independent_data)} points)")
            for name, df in group_crypto_dfs.items():
                print(f"{name}: {df['market_title'].iloc[0]} ({len(df)} points)")
            print(f"Window: {window_ms}ms")
            
            # Initialize crossover detector
            detector = CrossoverDetector(window_ms=window_ms)
            
            # Detect crossovers
            crossovers = detector.detect_crossovers(independent_data)
            
            if not crossovers:
                print("No crossover events found for this group!")
                continue
            
            # Measure crypto responses
            results = detector.measure_crypto_responses(crossovers, group_crypto_dfs, stability_threshold)
            
            if not results:
                print("No crypto response data could be measured for this group!")
                continue
            
            # Add group info to results
            for result in results:
                result['market_group'] = group['time_period']
                result[f'{independent_crypto}_title'] = group[independent_crypto]['title']
            
            all_results.extend(results)
        
        if not all_results:
            print("No crossover events found across all market groups!")
            sys.exit(1)
        
        # Analyze combined results
        all_crypto_names = sorted(list(all_crypto_names))
        analysis = analyze_crossover_results(all_results, all_crypto_names)
        
        # Create visualization
        filter_suffix = ""
        if title_filter:
            filter_suffix += f"_filtered_{title_filter.replace(' ', '_')}"
        
        crypto_suffix = "_".join(all_crypto_names)
        output_file = f"{independent_crypto}_crossover_{crypto_suffix.lower()}_analysis{filter_suffix}_w{window_ms}ms.png"
        
        fig = create_analysis_plot(analysis, output_file=output_file)
        
        # Print detailed results
        print(f"\n" + "="*70)
        print(f"{independent_short} CROSSOVER → MULTI-CRYPTO RESPONSE ANALYSIS")
        print(f"="*70)
        print(f"Analyzed {len(market_groups)} market groups with matching time periods")
        print(f"Total events across all groups: {len(all_results)}")
        print(f"Analysis Window: {window_ms} ms")
        
        print(f"\nCROSSOVER EVENTS:")
        print(f"  Total crossover events: {analysis['total_events']}")
        print(f"  Bid crosses ask events: {analysis['bid_crosses_ask_count']}")
        print(f"  Ask crosses bid events: {analysis['ask_crosses_bid_count']}")
        
        intervals = ['200ms', '400ms', '600ms', '800ms', '1000ms']
        
        for crypto_name in all_crypto_names:
            print(f"\nAVERAGE {crypto_name} RESPONSES:")
            
            for event_type in ['bid_crosses_ask', 'ask_crosses_bid']:
                if (event_type in analysis and
                    'crypto_responses' in analysis[event_type] and
                    crypto_name in analysis[event_type]['crypto_responses'] and
                    'interval_averages' in analysis[event_type]['crypto_responses'][crypto_name]):
                    
                    print(f"\n  {event_type.replace('_', ' ').title()}:")
                    crypto_data = analysis[event_type]['crypto_responses'][crypto_name]['interval_averages']
                    for interval in intervals:
                        if interval in crypto_data:
                            data = crypto_data[interval]
                            print(f"    {interval}: {data['mean']:.6f} ± {data['std']:.6f} (n={data['count']})")
        
        print(f"\nVisualization saved as: {output_file}")
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()