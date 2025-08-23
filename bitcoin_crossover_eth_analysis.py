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
    """Load Bitcoin, Ethereum, XRP, and Solana L1 data with optional filtering."""
    print(f"Loading crypto data from {filename}...")
    if title_filter:
        print(f"Filtering for markets containing: '{title_filter}'")
    
    bitcoin_data = []
    ethereum_data = []
    xrp_data = []
    solana_data = []
    
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
                elif 'xrp' in market_title:
                    xrp_data.append(record)
                elif 'solana' in market_title:
                    solana_data.append(record)
                
                if line_num % 10000 == 0:
                    print(f"Processed {line_num} records...")
                    
            except json.JSONDecodeError as e:
                print(f"JSON decode error on line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    print(f"Loaded {len(bitcoin_data)} Bitcoin, {len(ethereum_data)} Ethereum, {len(xrp_data)} XRP, {len(solana_data)} Solana records")
    
    # Convert to DataFrames
    bitcoin_df = pd.DataFrame(bitcoin_data) if bitcoin_data else pd.DataFrame()
    ethereum_df = pd.DataFrame(ethereum_data) if ethereum_data else pd.DataFrame()
    xrp_df = pd.DataFrame(xrp_data) if xrp_data else pd.DataFrame()
    solana_df = pd.DataFrame(solana_data) if solana_data else pd.DataFrame()
    
    for df in [bitcoin_df, ethereum_df, xrp_df, solana_df]:
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['ts_ms'], unit='ms')
            # Convert price columns to numeric
            for col in ['best_bid_price', 'best_ask_price', 'mid_price']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return bitcoin_df, ethereum_df, xrp_df, solana_df

class CrossoverDetector:
    """Detects Bitcoin bid/ask crossovers and measures ETH responses."""
    
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
        For each crossover event, measure combined crypto price changes at 200ms, 400ms, 600ms, 800ms, 1000ms intervals.
        Filter out events where any crypto has already changed significantly in the last second.
        Treats ETH, XRP, and Solana changes as equal and averages them together.
        
        Args:
            crossovers: List of crossover events
            crypto_dfs: Dict of crypto DataFrames {'ETH': df, 'XRP': df, 'SOL': df}
            stability_threshold: Maximum allowed price change in the last 1000ms (default: 0.001 = 0.1%)
        """
        crypto_names = list(crypto_dfs.keys())
        print(f"Measuring combined {', '.join(crypto_names)} responses for {len(crossovers)} crossover events...")
        print(f"Filtering out events where any crypto changed more than {stability_threshold*100:.1f}% in the last 1000ms...")
        
        # Sort and clean all crypto data
        for name, df in crypto_dfs.items():
            if not df.empty:
                crypto_dfs[name] = df.sort_values('ts_ms').copy().dropna(subset=['mid_price'])
        
        # Check if we have any data
        if all(df.empty for df in crypto_dfs.values()):
            print("No crypto data available for response measurement")
            return []
        
        response_intervals = [200, 400, 600, 800, 1000]  # 200ms, 400ms, 600ms, 800ms, 1000ms
        results = []
        filtered_count = 0
        
        for crossover in crossovers:
            crossover_time = crossover['time']
            should_filter = False
            crypto_prices_before = {}
            
            # Check stability and get baseline prices for each crypto
            for crypto_name, crypto_df in crypto_dfs.items():
                if crypto_df.empty:
                    continue
                    
                # Find crypto price at crossover time (or closest before)
                crypto_before_mask = crypto_df['ts_ms'] <= crossover_time
                if not crypto_before_mask.any():
                    continue
                    
                crypto_before_idx = crypto_df[crypto_before_mask]['ts_ms'].idxmax()
                crypto_price_before = crypto_df.loc[crypto_before_idx, 'mid_price']
                crypto_prices_before[crypto_name] = crypto_price_before
                
                # Check stability in the last 1000ms before crossover
                stability_check_time = crossover_time - 1000  # 1 second ago
                crypto_stability_mask = crypto_df['ts_ms'] <= stability_check_time
                
                if crypto_stability_mask.any():
                    crypto_stability_idx = crypto_df[crypto_stability_mask]['ts_ms'].idxmax()
                    crypto_price_1s_ago = crypto_df.loc[crypto_stability_idx, 'mid_price']
                    
                    # Calculate relative price change in the last second
                    if crypto_price_1s_ago > 0:
                        crypto_change_last_1s = abs(crypto_price_before - crypto_price_1s_ago) / crypto_price_1s_ago
                        
                        # Filter out if this crypto has changed too much in the last second
                        if crypto_change_last_1s > stability_threshold:
                            should_filter = True
                            break
            
            if should_filter:
                filtered_count += 1
                continue
            
            # Measure price changes at each interval and combine them
            combined_interval_changes = {}
            
            for interval_ms in response_intervals:
                target_time = crossover_time + interval_ms
                interval_changes = []
                
                # Collect changes from all cryptos for this interval
                for crypto_name, crypto_df in crypto_dfs.items():
                    if crypto_df.empty or crypto_name not in crypto_prices_before:
                        continue
                        
                    crypto_price_before = crypto_prices_before[crypto_name]
                    
                    # Find closest crypto price at target time
                    time_diffs = abs(crypto_df['ts_ms'] - target_time)
                    if len(time_diffs) == 0:
                        continue
                        
                    closest_idx = time_diffs.idxmin()
                    
                    # Only use if reasonably close to target time (within 50% of interval)
                    if time_diffs[closest_idx] <= interval_ms * 0.5:
                        crypto_price_after = crypto_df.loc[closest_idx, 'mid_price']
                        # Calculate relative price change (percentage)
                        if crypto_price_before > 0:
                            relative_change = (crypto_price_after - crypto_price_before) / crypto_price_before
                            interval_changes.append(relative_change)
                
                # Average the changes across all cryptos for this interval
                if interval_changes:
                    combined_interval_changes[f'{interval_ms}ms'] = np.mean(interval_changes)
            
            # Only include if we have at least some interval measurements
            if combined_interval_changes:
                result = crossover.copy()
                result['crypto_prices_before'] = crypto_prices_before
                result['combined_changes'] = combined_interval_changes
                results.append(result)
        
        print(f"Filtered out {filtered_count} events where cryptos were unstable in the last 1000ms")
        print(f"Successfully measured combined crypto responses for {len(results)} crossover events")
        return results

def analyze_crossover_results(results: List[Dict]) -> Dict:
    """Analyze and average the crossover results for combined cryptocurrency responses."""
    if not results:
        return {}
    
    print(f"Analyzing {len(results)} crossover events with combined crypto responses...")
    
    # Separate by crossover type
    bid_crosses_ask = [r for r in results if r['type'] == 'bid_crosses_ask']
    ask_crosses_bid = [r for r in results if r['type'] == 'ask_crosses_bid']
    
    analysis = {
        'total_events': len(results),
        'bid_crosses_ask_count': len(bid_crosses_ask),
        'ask_crosses_bid_count': len(ask_crosses_bid),
        'intervals': ['200ms', '400ms', '600ms', '800ms', '1000ms']
    }
    
    # Calculate averages for each type and interval
    for event_type, events in [('bid_crosses_ask', bid_crosses_ask), ('ask_crosses_bid', ask_crosses_bid)]:
        if not events:
            continue
            
        type_analysis = {
            'count': len(events),
            'avg_crossover_amount': np.mean([e['crossover_amount'] for e in events]),
            'interval_averages': {}
        }
        
        # Calculate average combined change for each interval
        for interval in analysis['intervals']:
            changes = []
            for event in events:
                if 'combined_changes' in event and interval in event['combined_changes']:
                    changes.append(event['combined_changes'][interval])
            
            if changes:
                type_analysis['interval_averages'][interval] = {
                    'mean': np.mean(changes),
                    'std': np.std(changes),
                    'count': len(changes),
                    'median': np.median(changes)
                }
        
        analysis[event_type] = type_analysis
    
    return analysis

def create_analysis_plot(analysis: Dict, output_file: str = None):
    """Create visualization of combined cryptocurrency crossover analysis results."""
    if not analysis or analysis['total_events'] == 0:
        print("No data to plot")
        return None
    
    intervals = analysis['intervals']
    
    # Create simple 2x2 subplot layout
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: Combined crypto response to bid crosses ask
    if 'bid_crosses_ask' in analysis and analysis['bid_crosses_ask']['interval_averages']:
        bid_data = analysis['bid_crosses_ask']['interval_averages']
        means = [bid_data.get(interval, {}).get('mean', 0) for interval in intervals]
        stds = [bid_data.get(interval, {}).get('std', 0) for interval in intervals]
        
        ax1.bar(intervals, means, yerr=stds, alpha=0.7, color='blue', capsize=5)
        ax1.set_title(f'Combined Crypto Response to Bitcoin Bid Crossing Ask\n({analysis["bid_crosses_ask"]["count"]} events)')
        ax1.set_ylabel('Average Relative Price Change')
        ax1.set_xlabel('Time Interval')
        ax1.grid(True, alpha=0.3)
        ax1.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    else:
        ax1.text(0.5, 0.5, 'No bid crosses ask events', ha='center', va='center', transform=ax1.transAxes)
        ax1.set_title('Combined Crypto Response to Bitcoin Bid Crossing Ask')
    
    # Plot 2: Combined crypto response to ask crosses bid
    if 'ask_crosses_bid' in analysis and analysis['ask_crosses_bid']['interval_averages']:
        ask_data = analysis['ask_crosses_bid']['interval_averages']
        means = [ask_data.get(interval, {}).get('mean', 0) for interval in intervals]
        stds = [ask_data.get(interval, {}).get('std', 0) for interval in intervals]
        
        ax2.bar(intervals, means, yerr=stds, alpha=0.7, color='red', capsize=5)
        ax2.set_title(f'Combined Crypto Response to Bitcoin Ask Crossing Bid\n({analysis["ask_crosses_bid"]["count"]} events)')
        ax2.set_ylabel('Average Relative Price Change')
        ax2.set_xlabel('Time Interval')
        ax2.grid(True, alpha=0.3)
        ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    else:
        ax2.text(0.5, 0.5, 'No ask crosses bid events', ha='center', va='center', transform=ax2.transAxes)
        ax2.set_title('Combined Crypto Response to Bitcoin Ask Crossing Bid')
    
    # Plot 3: Overall combined response
    combined_means = []
    combined_stds = []
    
    for interval in intervals:
        all_changes = []
        
        # Collect all changes for this interval across both event types
        for event_type in ['bid_crosses_ask', 'ask_crosses_bid']:
            if event_type in analysis and 'interval_averages' in analysis[event_type]:
                interval_data = analysis[event_type]['interval_averages'].get(interval)
                if interval_data and 'mean' in interval_data:
                    # Weight by count to get proper combined average
                    count = interval_data['count']
                    mean = interval_data['mean']
                    all_changes.extend([mean] * count)
        
        if all_changes:
            combined_means.append(np.mean(all_changes))
            combined_stds.append(np.std(all_changes))
        else:
            combined_means.append(0)
            combined_stds.append(0)
    
    ax3.bar(intervals, combined_means, yerr=combined_stds, alpha=0.7, color='green', capsize=5)
    ax3.set_title(f'Overall Combined Crypto Response to All Bitcoin Crossovers\n({analysis["total_events"]} total events)')
    ax3.set_ylabel('Average Relative Price Change')
    ax3.set_xlabel('Time Interval')
    ax3.grid(True, alpha=0.3)
    ax3.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    
    # Plot 4: Statistics table
    ax4.axis('off')
    
    stats_data = [
        ['Metric', 'Value'],
        ['Total Crossover Events', f"{analysis['total_events']}"],
        ['Bid Crosses Ask Events', f"{analysis['bid_crosses_ask_count']}"],
        ['Ask Crosses Bid Events', f"{analysis['ask_crosses_bid_count']}"],
        ['', ''],
        ['Average Combined Changes:', '']
    ]
    
    for interval in intervals:
        if combined_means[intervals.index(interval)] != 0:
            stats_data.append([f'  {interval}', f'{combined_means[intervals.index(interval)]:.6f}'])
    
    table = ax4.table(cellText=stats_data, cellLoc='left', loc='center',
                     colWidths=[0.6, 0.4])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.3)
    
    # Style the header row
    for i in range(2):
        table[(0, i)].set_facecolor('#E6E6FA')
        table[(0, i)].set_text_props(weight='bold')
    
    ax4.set_title('Analysis Statistics', fontsize=12, pad=20)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Analysis plot saved as: {output_file}")
    
    return fig

def main():
    parser = argparse.ArgumentParser(
        description='Bitcoin Crossover Crypto Analysis - Detects when Bitcoin bid reaches WINDOW ms ago\'s ask (or vice versa) and measures combined ETH/XRP/Solana price changes.',
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
                       help='Maximum allowed crypto price change in the last 1000ms to consider stable (default: 0.001 = 0.1%%)')
    
    args = parser.parse_args()
    
    input_file = args.input_file
    title_filter = args.title_filter
    window_ms = args.window
    stability_threshold = args.stability_threshold
    
    try:
        # Load all crypto data
        bitcoin_df, ethereum_df, xrp_df, solana_df = load_crypto_data(input_file, title_filter)
        
        if bitcoin_df.empty:
            print("Need Bitcoin data!")
            sys.exit(1)
        
        # Check which cryptos we have data for
        crypto_dfs = {}
        if not ethereum_df.empty:
            crypto_dfs['ETH'] = ethereum_df
        if not xrp_df.empty:
            crypto_dfs['XRP'] = xrp_df
        if not solana_df.empty:
            crypto_dfs['SOL'] = solana_df
        
        if not crypto_dfs:
            print("Need at least one of: Ethereum, XRP, or Solana data!")
            sys.exit(1)
        
        # Get the most active assets
        bitcoin_asset = bitcoin_df['asset_id'].value_counts().index[0]
        bitcoin_data = bitcoin_df[bitcoin_df['asset_id'] == bitcoin_asset].copy()
        
        # Get most active asset for each crypto
        for crypto_name, crypto_df in crypto_dfs.items():
            crypto_asset = crypto_df['asset_id'].value_counts().index[0]
            crypto_dfs[crypto_name] = crypto_df[crypto_df['asset_id'] == crypto_asset].copy()
        
        print(f"\nAnalyzing:")
        print(f"Bitcoin: {bitcoin_data['market_title'].iloc[0]} ({len(bitcoin_data)} points)")
        for crypto_name, crypto_df in crypto_dfs.items():
            print(f"{crypto_name}: {crypto_df['market_title'].iloc[0]} ({len(crypto_df)} points)")
        print(f"Window: {window_ms}ms")
        print(f"Stability threshold: {stability_threshold*100:.1f}%")
        
        # Initialize crossover detector
        detector = CrossoverDetector(window_ms=window_ms)
        
        # Detect crossovers
        crossovers = detector.detect_crossovers(bitcoin_data)
        
        if not crossovers:
            print("No crossover events found!")
            sys.exit(1)
        
        # Measure combined crypto responses
        results = detector.measure_crypto_responses(crossovers, crypto_dfs, stability_threshold)
        
        if not results:
            print("No crypto response data could be measured!")
            sys.exit(1)
        
        # Analyze results
        analysis = analyze_crossover_results(results)
        
        # Create visualization
        filter_suffix = ""
        if title_filter:
            filter_suffix += f"_filtered_{title_filter.replace(' ', '_')}"
        
        crypto_suffix = "_".join(crypto_dfs.keys())
        output_file = f"bitcoin_crossover_{crypto_suffix}_analysis{filter_suffix}_w{window_ms}ms.png"
        
        fig = create_analysis_plot(analysis, output_file=output_file)
        
        # Print detailed results
        print(f"\n" + "="*70)
        print(f"BITCOIN CROSSOVER → COMBINED CRYPTO RESPONSE ANALYSIS")
        print(f"="*70)
        print(f"Bitcoin Market: {bitcoin_data['market_title'].iloc[0]}")
        print(f"Analyzed Cryptos: {', '.join(crypto_dfs.keys())}")
        print(f"Analysis Window: {window_ms} ms")
        print(f"Stability Threshold: {stability_threshold*100:.1f}%")
        
        print(f"\nCROSSOVER EVENTS:")
        print(f"  Total crossover events: {analysis['total_events']}")
        print(f"  Bid crosses ask events: {analysis['bid_crosses_ask_count']}")
        print(f"  Ask crosses bid events: {analysis['ask_crosses_bid_count']}")
        
        print(f"\nAVERAGE COMBINED CRYPTO RESPONSES:")
        intervals = ['200ms', '400ms', '600ms', '800ms', '1000ms']
        
        for event_type in ['bid_crosses_ask', 'ask_crosses_bid']:
            if event_type in analysis and 'interval_averages' in analysis[event_type]:
                print(f"\n  {event_type.replace('_', ' ').title()}:")
                for interval in intervals:
                    if interval in analysis[event_type]['interval_averages']:
                        data = analysis[event_type]['interval_averages'][interval]
                        print(f"    {interval}: {data['mean']:.6f} ± {data['std']:.6f} (n={data['count']})")
        
        # Combined averages
        print(f"\n  Combined (All Crossovers):")
        for interval in intervals:
            all_changes = []
            for event_type in ['bid_crosses_ask', 'ask_crosses_bid']:
                if event_type in analysis and 'interval_averages' in analysis[event_type]:
                    interval_data = analysis[event_type]['interval_averages'].get(interval)
                    if interval_data:
                        count = interval_data['count']
                        mean = interval_data['mean']
                        all_changes.extend([mean] * count)
            
            if all_changes:
                combined_mean = np.mean(all_changes)
                combined_std = np.std(all_changes)
                print(f"    {interval}: {combined_mean:.6f} ± {combined_std:.6f} (n={len(all_changes)})")
        
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