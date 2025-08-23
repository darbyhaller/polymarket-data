#!/usr/bin/env python3
"""
Bitcoin Crossover PNL Tracker.
Tracks PNL over time for ETH, SOL, XRP based on Bitcoin crossover signals:
- Buy 1 coin at bid when Bitcoin bid crosses ask
- Sell 1 coin at ask when Bitcoin ask crosses bid
- Track PNL using mid prices over 1 second after each trade
"""

import json
import sys
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

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

class PNLTracker:
    """Tracks PNL for crypto assets based on Bitcoin crossover signals."""
    
    def __init__(self, window_ms: int = 1000):
        self.window_ms = window_ms
        self.trades = []
        self.positions = defaultdict(float)  # Current position for each crypto
        self.pnl_history = []
        
    def detect_crossovers_and_trade(self, bitcoin_df: pd.DataFrame, crypto_dfs: Dict[str, pd.DataFrame]) -> List[Dict]:
        """
        Detect Bitcoin crossovers and execute trades on other cryptos.
        Returns list of trade events with PNL tracking over 1 second.
        """
        print(f"Detecting Bitcoin crossovers and executing trades...")
        
        # Sort by timestamp
        bitcoin_df = bitcoin_df.sort_values('ts_ms').copy()
        bitcoin_df = bitcoin_df.dropna(subset=['best_bid_price', 'best_ask_price'])
        
        if len(bitcoin_df) < 2:
            print("Not enough Bitcoin data for crossover detection")
            return []
        
        # Sort all crypto data by timestamp
        for name, df in crypto_dfs.items():
            crypto_dfs[name] = df.sort_values('ts_ms').copy().dropna(subset=['best_bid_price', 'best_ask_price', 'mid_price'])
        
        trade_events = []
        
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
            
            trade_signal = None
            
            # Check for crossovers
            # Type 1: Current bid >= window ask (bid crosses up through ask) -> BUY signal
            if current_bid >= window_ask:
                trade_signal = 'BUY'
            
            # Type 2: Current ask <= window bid (ask crosses down through bid) -> SELL signal
            elif current_ask <= window_bid:
                trade_signal = 'SELL'
            
            if trade_signal:
                # Execute trades on all available cryptos
                trade_event = {
                    'bitcoin_time': current_time,
                    'bitcoin_datetime': current_row['datetime'],
                    'signal': trade_signal,
                    'bitcoin_bid': current_bid,
                    'bitcoin_ask': current_ask,
                    'window_bid': window_bid,
                    'window_ask': window_ask,
                    'trades': {},
                    'pnl_timeline': []
                }
                
                # Execute trades for each crypto
                for crypto_name, crypto_df in crypto_dfs.items():
                    trade_price, trade_executed = self._execute_trade(crypto_name, crypto_df, current_time, trade_signal)
                    if trade_executed:
                        trade_event['trades'][crypto_name] = {
                            'price': trade_price,
                            'quantity': 1 if trade_signal == 'BUY' else -1,
                            'position_after': self.positions[crypto_name]
                        }
                
                # Track PNL over the next 1000ms
                if trade_event['trades']:
                    pnl_timeline = self._track_pnl_over_time(crypto_dfs, current_time, 1000)
                    trade_event['pnl_timeline'] = pnl_timeline
                    trade_events.append(trade_event)
        
        print(f"Found {len(trade_events)} trade events")
        return trade_events
    
    def _execute_trade(self, crypto_name: str, crypto_df: pd.DataFrame, trade_time: int, signal: str) -> Tuple[float, bool]:
        """Execute a trade for a specific crypto at the given time."""
        # Find the closest crypto price data at trade time
        time_diffs = abs(crypto_df['ts_ms'] - trade_time)
        if len(time_diffs) == 0:
            return 0.0, False
            
        closest_idx = time_diffs.idxmin()
        
        # Only trade if we have reasonably recent data (within 500ms)
        if time_diffs[closest_idx] > 500:
            return 0.0, False
        
        crypto_row = crypto_df.loc[closest_idx]
        
        if signal == 'BUY':
            # Buy at bid price
            trade_price = crypto_row['best_bid_price']
            self.positions[crypto_name] += 1
        else:  # SELL
            # Sell at ask price
            trade_price = crypto_row['best_ask_price']
            self.positions[crypto_name] -= 1
        
        return trade_price, True
    
    def _track_pnl_over_time(self, crypto_dfs: Dict[str, pd.DataFrame], start_time: int, duration_ms: int) -> List[Dict]:
        """Track PNL over time for all positions."""
        pnl_timeline = []
        
        # Create time points every 50ms for 1 second
        time_points = range(start_time, start_time + duration_ms + 1, 50)
        
        for time_point in time_points:
            pnl_snapshot = {
                'time_offset_ms': time_point - start_time,
                'timestamp': time_point,
                'crypto_pnl': {},
                'total_pnl': 0.0
            }
            
            total_pnl = 0.0
            
            for crypto_name, crypto_df in crypto_dfs.items():
                if self.positions[crypto_name] != 0:
                    # Find mid price at this time point
                    time_diffs = abs(crypto_df['ts_ms'] - time_point)
                    if len(time_diffs) > 0:
                        closest_idx = time_diffs.idxmin()
                        
                        # Only use if reasonably close (within 200ms)
                        if time_diffs[closest_idx] <= 200:
                            mid_price = crypto_df.loc[closest_idx, 'mid_price']
                            
                            # Calculate PNL: position * current_mid_price
                            # (We'll subtract the cost basis when we calculate final PNL)
                            crypto_pnl = self.positions[crypto_name] * mid_price
                            pnl_snapshot['crypto_pnl'][crypto_name] = crypto_pnl
                            total_pnl += crypto_pnl
            
            pnl_snapshot['total_pnl'] = total_pnl
            pnl_timeline.append(pnl_snapshot)
        
        return pnl_timeline

def create_pnl_plots(trade_events: List[Dict], output_file: str = None):
    """Create PNL visualization over time for each trade event."""
    if not trade_events:
        print("No trade events to plot")
        return None
    
    # Determine how many cryptos we have
    all_cryptos = set()
    for event in trade_events:
        all_cryptos.update(event['trades'].keys())
    
    crypto_list = sorted(list(all_cryptos))
    n_cryptos = len(crypto_list)
    
    if n_cryptos == 0:
        print("No crypto trades to plot")
        return None
    
    # Create subplots: one row per crypto, plus one for total
    fig, axes = plt.subplots(n_cryptos + 1, 1, figsize=(12, 4 * (n_cryptos + 1)))
    
    if n_cryptos == 0:
        axes = [axes]  # Make it a list for consistency
    elif n_cryptos == 1:
        axes = [axes[0], axes[1]]  # Ensure we have both crypto and total axes
    
    colors = ['blue', 'purple', 'orange', 'green', 'red']
    
    # Plot each trade event
    for event_idx, event in enumerate(trade_events):
        signal_color = 'green' if event['signal'] == 'BUY' else 'red'
        signal_label = f"Event {event_idx + 1} ({event['signal']})"
        
        # Extract time series data
        if not event['pnl_timeline']:
            continue
            
        time_offsets = [point['time_offset_ms'] for point in event['pnl_timeline']]
        
        # Plot individual crypto PNLs
        for crypto_idx, crypto_name in enumerate(crypto_list):
            ax = axes[crypto_idx]
            
            if crypto_name in event['trades']:
                # Get PNL values for this crypto
                pnl_values = []
                trade_price = event['trades'][crypto_name]['price']
                quantity = event['trades'][crypto_name]['quantity']
                
                for point in event['pnl_timeline']:
                    if crypto_name in point['crypto_pnl']:
                        # PNL = quantity * (current_price - trade_price)
                        current_value = point['crypto_pnl'][crypto_name]
                        cost_basis = quantity * trade_price
                        pnl = current_value - cost_basis
                        pnl_values.append(pnl)
                    else:
                        pnl_values.append(0)
                
                ax.plot(time_offsets, pnl_values, 
                       color=colors[event_idx % len(colors)], 
                       alpha=0.7, 
                       label=signal_label,
                       linewidth=2)
            
            ax.set_title(f'{crypto_name} PNL Over Time')
            ax.set_xlabel('Time Offset (ms)')
            ax.set_ylabel('PNL ($)')
            ax.grid(True, alpha=0.3)
            ax.axhline(y=0, color='black', linestyle='-', alpha=0.5)
            ax.legend()
        
        # Plot total PNL
        total_ax = axes[-1]
        total_pnl_values = []
        
        for point in event['pnl_timeline']:
            total_pnl = 0.0
            for crypto_name in crypto_list:
                if crypto_name in event['trades'] and crypto_name in point['crypto_pnl']:
                    trade_price = event['trades'][crypto_name]['price']
                    quantity = event['trades'][crypto_name]['quantity']
                    current_value = point['crypto_pnl'][crypto_name]
                    cost_basis = quantity * trade_price
                    pnl = current_value - cost_basis
                    total_pnl += pnl
            total_pnl_values.append(total_pnl)
        
        total_ax.plot(time_offsets, total_pnl_values,
                     color=colors[event_idx % len(colors)],
                     alpha=0.7,
                     label=signal_label,
                     linewidth=2)
    
    total_ax.set_title('Total Portfolio PNL Over Time')
    total_ax.set_xlabel('Time Offset (ms)')
    total_ax.set_ylabel('Total PNL ($)')
    total_ax.grid(True, alpha=0.3)
    total_ax.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    total_ax.legend()
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"PNL plot saved as: {output_file}")
    
    return fig

def main():
    parser = argparse.ArgumentParser(
        description='Bitcoin Crossover PNL Tracker - Track PNL for ETH, SOL, XRP based on Bitcoin crossover signals.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python bitcoin_crossover_pnl_tracker.py l1_data.jsonl
  python bitcoin_crossover_pnl_tracker.py l1_data.jsonl --window 500
  python bitcoin_crossover_pnl_tracker.py l1_data.jsonl --title-filter "12AM ET"
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
    
    args = parser.parse_args()
    
    input_file = args.input_file
    title_filter = args.title_filter
    window_ms = args.window
    
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
        
        # Initialize PNL tracker
        tracker = PNLTracker(window_ms=window_ms)
        
        # Detect crossovers and execute trades
        trade_events = tracker.detect_crossovers_and_trade(bitcoin_data, crypto_dfs)
        
        if not trade_events:
            print("No trade events found!")
            sys.exit(1)
        
        # Create visualization
        filter_suffix = ""
        if title_filter:
            filter_suffix += f"_filtered_{title_filter.replace(' ', '_')}"
        
        crypto_suffix = "_".join(crypto_names)
        output_file = f"bitcoin_crossover_pnl_{crypto_suffix.lower()}{filter_suffix}_w{window_ms}ms.png"
        
        fig = create_pnl_plots(trade_events, output_file=output_file)
        
        # Print detailed results
        print(f"\n" + "="*70)
        print(f"BITCOIN CROSSOVER PNL TRACKING RESULTS")
        print(f"="*70)
        print(f"Bitcoin Market: {bitcoin_data['market_title'].iloc[0]}")
        for name, df in crypto_dfs.items():
            print(f"{name} Market: {df['market_title'].iloc[0]}")
        print(f"Analysis Window: {window_ms} ms")
        
        print(f"\nTRADE EVENTS:")
        print(f"  Total trade events: {len(trade_events)}")
        
        buy_events = [e for e in trade_events if e['signal'] == 'BUY']
        sell_events = [e for e in trade_events if e['signal'] == 'SELL']
        
        print(f"  BUY signals: {len(buy_events)}")
        print(f"  SELL signals: {len(sell_events)}")
        
        # Show final positions
        print(f"\nFINAL POSITIONS:")
        for crypto_name in crypto_names:
            position = tracker.positions[crypto_name]
            print(f"  {crypto_name}: {position:.2f} coins")
        
        # Show individual trade events
        print(f"\nTRADE EVENT DETAILS:")
        for i, event in enumerate(trade_events[:5], 1):  # Show first 5 events
            print(f"\nEvent #{i} ({event['signal']}):")
            print(f"  Time: {event['bitcoin_datetime']}")
            print(f"  Trades executed:")
            for crypto_name, trade_info in event['trades'].items():
                action = "Bought" if trade_info['quantity'] > 0 else "Sold"
                print(f"    {action} 1 {crypto_name} at ${trade_info['price']:.6f}")
                print(f"    Position after: {trade_info['position_after']:.2f}")
        
        if len(trade_events) > 5:
            print(f"\n  ... and {len(trade_events) - 5} more events")
        
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