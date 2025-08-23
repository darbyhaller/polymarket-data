#!/usr/bin/env python3
"""
Bitcoin Crossover ETH Analysis.
Detects when Bitcoin bid reaches WINDOW ms ago's ask (or vice versa) and measures ETH price changes.
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

def load_crypto_data(filename: str, title_filter: str = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load Bitcoin, Ethereum, Solana, and XRP L1 data with optional filtering."""
    print(f"Loading crypto data from {filename}...")
    if title_filter:
        print(f"Filtering for markets containing: '{title_filter}'")
    
    bitcoin_data = []
    ethereum_data = []
    solana_data = []
    xrp_data = []
    
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
                if 'bitcoin' in market_title:
                    bitcoin_data.append(record)
                elif 'ethereum' in market_title:
                    ethereum_data.append(record)
                elif 'solana' in market_title:
                    solana_data.append(record)
                elif 'xrp' in market_title:
                    xrp_data.append(record)
                
                if line_num % 10000 == 0:
                    print(f"Processed {line_num} records...")
                    
            except json.JSONDecodeError as e:
                print(f"JSON decode error on line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    print(f"Loaded {len(bitcoin_data)} Bitcoin records, {len(ethereum_data)} Ethereum records, {len(solana_data)} Solana records, {len(xrp_data)} XRP records")
    
    # Convert to DataFrames
    bitcoin_df = pd.DataFrame(bitcoin_data) if bitcoin_data else pd.DataFrame()
    ethereum_df = pd.DataFrame(ethereum_data) if ethereum_data else pd.DataFrame()
    solana_df = pd.DataFrame(solana_data) if solana_data else pd.DataFrame()
    xrp_df = pd.DataFrame(xrp_data) if xrp_data else pd.DataFrame()
    
    for df in [bitcoin_df, ethereum_df, solana_df, xrp_df]:
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['ts_ms'], unit='ms')
            # Convert price columns to numeric
            for col in ['best_bid_price', 'best_ask_price', 'mid_price']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return bitcoin_df, ethereum_df, solana_df, xrp_df

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
    
    crypto_names = analysis.get('crypto_names', ['ETH'])
    intervals = analysis['intervals']
    
    # Create 6 plots: 2 for each cryptocurrency (bid crosses ask, ask crosses bid)
    fig, axes = plt.subplots(3, 2, figsize=(16, 18))
    
    colors = {'ETH': 'blue', 'SOL': 'purple', 'XRP': 'orange'}
    
    plot_idx = 0
    for crypto_name in crypto_names:
        crypto_color = colors.get(crypto_name, 'gray')
        
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
            ax_bid.set_title(f'{crypto_name} Response to Bitcoin Bid Crossing Ask\n({analysis["bid_crosses_ask"]["count"]} events)')
        else:
            ax_bid.text(0.5, 0.5, f'No bid crosses ask events\nfor {crypto_name}',
                       ha='center', va='center', transform=ax_bid.transAxes)
            ax_bid.set_title(f'{crypto_name} Response to Bitcoin Bid Crossing Ask')
        
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
            ax_ask.set_title(f'{crypto_name} Response to Bitcoin Ask Crossing Bid\n({analysis["ask_crosses_bid"]["count"]} events)')
        else:
            ax_ask.text(0.5, 0.5, f'No ask crosses bid events\nfor {crypto_name}',
                       ha='center', va='center', transform=ax_ask.transAxes)
            ax_ask.set_title(f'{crypto_name} Response to Bitcoin Ask Crossing Bid')
        
        ax_ask.set_ylabel(f'Average {crypto_name} Price Change')
        ax_ask.set_xlabel('Time Interval')
        ax_ask.grid(True, alpha=0.3)
        ax_ask.axhline(y=0, color='black', linestyle='-', alpha=0.5)
        
        plot_idx += 1
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Analysis plot saved as: {output_file}")
    
    return fig

def main():
    parser = argparse.ArgumentParser(
        description='Bitcoin Crossover Multi-Crypto Analysis - Detects when Bitcoin bid reaches WINDOW ms ago\'s ask (or vice versa) and measures ETH, SOL, and XRP price changes.',
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
        # Load Bitcoin, Ethereum, Solana, and XRP data
        bitcoin_df, ethereum_df, solana_df, xrp_df = load_crypto_data(input_file, title_filter)
        
        if bitcoin_df.empty:
            print("Need Bitcoin data for crossover detection!")
            sys.exit(1)
        
        # Get the most active Bitcoin asset
        bitcoin_asset = bitcoin_df['asset_id'].value_counts().index[0]
        bitcoin_data = bitcoin_df[bitcoin_df['asset_id'] == bitcoin_asset].copy()
        
        # Prepare dependent variable data
        crypto_dfs = {}
        crypto_names = []
        
        if not ethereum_df.empty:
            ethereum_asset = ethereum_df['asset_id'].value_counts().index[0]
            crypto_dfs['ETH'] = ethereum_df[ethereum_df['asset_id'] == ethereum_asset].copy()
            crypto_names.append('ETH')
        
        if not solana_df.empty:
            solana_asset = solana_df['asset_id'].value_counts().index[0]
            crypto_dfs['SOL'] = solana_df[solana_df['asset_id'] == solana_asset].copy()
            crypto_names.append('SOL')
        
        if not xrp_df.empty:
            xrp_asset = xrp_df['asset_id'].value_counts().index[0]
            crypto_dfs['XRP'] = xrp_df[xrp_df['asset_id'] == xrp_asset].copy()
            crypto_names.append('XRP')
        
        if not crypto_dfs:
            print("Need at least one dependent variable (ETH, SOL, or XRP) data!")
            sys.exit(1)
        
        print(f"\nAnalyzing:")
        print(f"Bitcoin: {bitcoin_data['market_title'].iloc[0]} ({len(bitcoin_data)} points)")
        for name, df in crypto_dfs.items():
            print(f"{name}: {df['market_title'].iloc[0]} ({len(df)} points)")
        print(f"Window: {window_ms}ms")
        
        # Initialize crossover detector
        detector = CrossoverDetector(window_ms=window_ms)
        
        # Detect crossovers
        crossovers = detector.detect_crossovers(bitcoin_data)
        
        if not crossovers:
            print("No crossover events found!")
            sys.exit(1)
        
        # Measure crypto responses
        results = detector.measure_crypto_responses(crossovers, crypto_dfs, stability_threshold)
        
        if not results:
            print("No crypto response data could be measured!")
            sys.exit(1)
        
        # Analyze results
        analysis = analyze_crossover_results(results, crypto_names)
        
        # Create visualization
        filter_suffix = ""
        if title_filter:
            filter_suffix += f"_filtered_{title_filter.replace(' ', '_')}"
        
        crypto_suffix = "_".join(crypto_names)
        output_file = f"bitcoin_crossover_{crypto_suffix.lower()}_analysis{filter_suffix}_w{window_ms}ms.png"
        
        fig = create_analysis_plot(analysis, output_file=output_file)
        
        # Print detailed results
        print(f"\n" + "="*70)
        print(f"BITCOIN CROSSOVER → MULTI-CRYPTO RESPONSE ANALYSIS")
        print(f"="*70)
        print(f"Bitcoin Market: {bitcoin_data['market_title'].iloc[0]}")
        for name, df in crypto_dfs.items():
            print(f"{name} Market: {df['market_title'].iloc[0]}")
        print(f"Analysis Window: {window_ms} ms")
        
        print(f"\nCROSSOVER EVENTS:")
        print(f"  Total crossover events: {analysis['total_events']}")
        print(f"  Bid crosses ask events: {analysis['bid_crosses_ask_count']}")
        print(f"  Ask crosses bid events: {analysis['ask_crosses_bid_count']}")
        
        intervals = ['200ms', '400ms', '600ms', '800ms', '1000ms']
        
        for crypto_name in crypto_names:
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