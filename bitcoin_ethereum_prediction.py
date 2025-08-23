#!/usr/bin/env python3
"""
Bitcoin-Ethereum Predictive Analysis.
Analyzes when Bitcoin moves >1 cent and whether Ethereum follows with similar moves.
"""

import json
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from scipy import stats

def load_crypto_data(filename, title_filter=None, time_filter=None):
    """Load Bitcoin and Ethereum L1 data with optional filtering."""
    print(f"Loading crypto data from {filename}...")
    if title_filter:
        print(f"Filtering for markets containing: '{title_filter}'")
    if time_filter:
        print(f"Time filter: {time_filter[0]:02d}:00 - {time_filter[1]:02d}:00 UTC")
    
    bitcoin_data = []
    ethereum_data = []
    
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
                
                # Separate Bitcoin and Ethereum data
                if 'bitcoin' in market_title:
                    bitcoin_data.append(record)
                elif 'ethereum' in market_title:
                    ethereum_data.append(record)
                
                if line_num % 10000 == 0:
                    print(f"Processed {line_num} records...")
                    
            except json.JSONDecodeError as e:
                print(f"JSON decode error on line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    print(f"Loaded {len(bitcoin_data)} Bitcoin records, {len(ethereum_data)} Ethereum records")
    
    # Convert to DataFrames
    bitcoin_df = pd.DataFrame(bitcoin_data) if bitcoin_data else pd.DataFrame()
    ethereum_df = pd.DataFrame(ethereum_data) if ethereum_data else pd.DataFrame()
    
    for df in [bitcoin_df, ethereum_df]:
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['ts_ms'], unit='ms')
            
            # Apply time range filter if specified
            if time_filter:
                start_hour, end_hour = time_filter
                df['hour'] = df['datetime'].dt.hour
                df = df[(df['hour'] >= start_hour) & (df['hour'] < end_hour)]
    
    if time_filter:
        bitcoin_df = bitcoin_df[(bitcoin_df['hour'] >= time_filter[0]) & (bitcoin_df['hour'] < time_filter[1])]
        ethereum_df = ethereum_df[(ethereum_df['hour'] >= time_filter[0]) & (ethereum_df['hour'] < time_filter[1])]
        print(f"After time filtering: {len(bitcoin_df)} Bitcoin, {len(ethereum_df)} Ethereum records")
    
    return bitcoin_df, ethereum_df

def detect_significant_moves(price_series, threshold=0.01, min_interval_ms=1000):
    """
    Detect moves greater than threshold (1 cent = 0.01).
    
    Parameters:
    - price_series: pandas Series with datetime index and price values
    - threshold: minimum absolute change (0.01 = 1 cent)
    - min_interval_ms: minimum time between events
    
    Returns:
    - events: list of dictionaries with event info
    """
    events = []
    
    if len(price_series) < 2:
        return events
    
    # Calculate price changes
    price_changes = price_series.diff()
    
    # Find significant moves
    significant_mask = abs(price_changes) >= threshold
    significant_times = price_series.index[significant_mask]
    significant_changes = price_changes[significant_mask]
    
    # Filter out events that are too close together
    if len(significant_times) > 0:
        filtered_events = []
        last_time = None
        
        for time, change in zip(significant_times, significant_changes):
            if last_time is None or (time - last_time).total_seconds() * 1000 >= min_interval_ms:
                events.append({
                    'time': time,
                    'change': change,
                    'direction': 'up' if change > 0 else 'down',
                    'magnitude': abs(change),
                    'price_before': price_series.asof(time - pd.Timedelta(milliseconds=100)),
                    'price_after': price_series.asof(time + pd.Timedelta(milliseconds=100))
                })
                last_time = time
    
    return events

def analyze_predictive_relationship(bitcoin_events, ethereum_price_series, max_lag_ms=5000):
    """
    Analyze if Bitcoin events predict Ethereum moves.
    
    Parameters:
    - bitcoin_events: list of Bitcoin significant move events
    - ethereum_price_series: Ethereum price series
    - max_lag_ms: maximum time to look ahead for Ethereum response
    
    Returns:
    - predictions: list of prediction results
    """
    predictions = []
    
    for btc_event in bitcoin_events:
        btc_time = btc_event['time']
        btc_direction = btc_event['direction']
        btc_magnitude = btc_event['magnitude']
        
        # Look for Ethereum response within the lag window
        window_start = btc_time
        window_end = btc_time + pd.Timedelta(milliseconds=max_lag_ms)
        
        # Get Ethereum prices in the response window
        eth_window_mask = (ethereum_price_series.index > window_start) & (ethereum_price_series.index <= window_end)
        eth_window_prices = ethereum_price_series[eth_window_mask]
        
        if len(eth_window_prices) == 0:
            continue
        
        # Get Ethereum price just before Bitcoin event
        eth_price_before = ethereum_price_series.asof(btc_time)
        
        if pd.isna(eth_price_before):
            continue
        
        # Find the maximum Ethereum move in the response window
        eth_changes = eth_window_prices - eth_price_before
        
        if len(eth_changes) == 0:
            continue
        
        # Find the largest absolute change and when it occurred
        max_abs_change_idx = abs(eth_changes).idxmax()
        max_change = eth_changes[max_abs_change_idx]
        max_change_time = max_abs_change_idx
        
        # Calculate response lag
        response_lag_ms = (max_change_time - btc_time).total_seconds() * 1000
        
        # Determine if Ethereum responded significantly (>1 cent)
        eth_responded = abs(max_change) >= 0.01
        
        # Determine if direction matched
        eth_direction = 'up' if max_change > 0 else 'down'
        direction_match = btc_direction == eth_direction
        
        predictions.append({
            'btc_time': btc_time,
            'btc_direction': btc_direction,
            'btc_magnitude': btc_magnitude,
            'eth_response_time': max_change_time,
            'eth_response_lag_ms': response_lag_ms,
            'eth_change': max_change,
            'eth_magnitude': abs(max_change),
            'eth_direction': eth_direction,
            'eth_responded': eth_responded,
            'direction_match': direction_match,
            'successful_prediction': eth_responded and direction_match
        })
    
    return predictions

def create_prediction_analysis_plot(predictions, bitcoin_title, ethereum_title, output_file=None):
    """Create comprehensive prediction analysis visualization."""
    
    if len(predictions) == 0:
        print("No predictions to analyze!")
        return None
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Convert to DataFrame for easier analysis
    pred_df = pd.DataFrame(predictions)
    
    # 1. Success Rate Analysis
    total_predictions = len(pred_df)
    eth_responded = pred_df['eth_responded'].sum()
    direction_matched = pred_df['direction_match'].sum()
    successful_predictions = pred_df['successful_prediction'].sum()
    
    success_rates = [
        eth_responded / total_predictions * 100,
        direction_matched / total_predictions * 100,
        successful_predictions / total_predictions * 100
    ]
    
    categories = ['Ethereum\nResponded\n(>1¢)', 'Direction\nMatched', 'Both\n(Success)']
    colors = ['lightblue', 'orange', 'green']
    
    bars = ax1.bar(categories, success_rates, color=colors, alpha=0.7)
    ax1.set_ylabel('Success Rate (%)')
    ax1.set_title(f'Bitcoin → Ethereum Prediction Success\n({total_predictions} Bitcoin moves >1¢)', fontsize=12)
    ax1.set_ylim(0, 100)
    
    # Add percentage labels on bars
    for bar, rate in zip(bars, success_rates):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                f'{rate:.1f}%', ha='center', va='bottom', fontweight='bold')
    
    ax1.grid(True, alpha=0.3)
    
    # 2. Response Time Distribution
    responded_predictions = pred_df[pred_df['eth_responded']]
    if len(responded_predictions) > 0:
        response_times = responded_predictions['eth_response_lag_ms']
        
        ax2.hist(response_times, bins=20, alpha=0.7, color='steelblue', density=True)
        ax2.axvline(response_times.median(), color='red', linestyle='--', 
                   label=f'Median: {response_times.median():.0f}ms')
        ax2.axvline(response_times.mean(), color='orange', linestyle='--', 
                   label=f'Mean: {response_times.mean():.0f}ms')
        
        ax2.set_xlabel('Response Time (ms)')
        ax2.set_ylabel('Density')
        ax2.set_title(f'Ethereum Response Time Distribution\n({len(responded_predictions)} responses)', fontsize=12)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
    else:
        ax2.text(0.5, 0.5, 'No Ethereum responses detected', 
                ha='center', va='center', transform=ax2.transAxes, fontsize=12)
        ax2.set_title('Ethereum Response Time Distribution', fontsize=12)
    
    # 3. Magnitude Correlation
    successful_preds = pred_df[pred_df['successful_prediction']]
    if len(successful_preds) > 0:
        btc_mags = successful_preds['btc_magnitude']
        eth_mags = successful_preds['eth_magnitude']
        
        ax3.scatter(btc_mags, eth_mags, alpha=0.6, color='green')
        
        # Add correlation line
        if len(btc_mags) > 1:
            correlation = np.corrcoef(btc_mags, eth_mags)[0, 1]
            z = np.polyfit(btc_mags, eth_mags, 1)
            p = np.poly1d(z)
            ax3.plot(btc_mags, p(btc_mags), "r--", alpha=0.8, 
                    label=f'Correlation: {correlation:.3f}')
            ax3.legend()
        
        ax3.set_xlabel('Bitcoin Move Magnitude')
        ax3.set_ylabel('Ethereum Response Magnitude')
        ax3.set_title(f'Magnitude Correlation\n({len(successful_preds)} successful predictions)', fontsize=12)
        ax3.grid(True, alpha=0.3)
        
        # Add diagonal line for reference
        max_val = max(btc_mags.max(), eth_mags.max())
        ax3.plot([0, max_val], [0, max_val], 'k:', alpha=0.5, label='1:1 ratio')
    else:
        ax3.text(0.5, 0.5, 'No successful predictions', 
                ha='center', va='center', transform=ax3.transAxes, fontsize=12)
        ax3.set_title('Magnitude Correlation', fontsize=12)
    
    # 4. Statistics Summary Table
    ax4.axis('off')
    
    # Calculate detailed statistics
    stats_data = [
        ['Metric', 'Value'],
        ['Total Bitcoin Moves (>1¢)', f'{total_predictions}'],
        ['Ethereum Responded (>1¢)', f'{eth_responded} ({eth_responded/total_predictions*100:.1f}%)'],
        ['Direction Matched', f'{direction_matched} ({direction_matched/total_predictions*100:.1f}%)'],
        ['Successful Predictions', f'{successful_predictions} ({successful_predictions/total_predictions*100:.1f}%)'],
        ['', ''],
        ['Response Time Stats', ''],
    ]
    
    if len(responded_predictions) > 0:
        stats_data.extend([
            ['Median Response Time', f'{response_times.median():.0f} ms'],
            ['Mean Response Time', f'{response_times.mean():.0f} ms'],
            ['Std Response Time', f'{response_times.std():.0f} ms'],
            ['Min Response Time', f'{response_times.min():.0f} ms'],
            ['Max Response Time', f'{response_times.max():.0f} ms'],
        ])
    else:
        stats_data.append(['No responses detected', ''])
    
    stats_data.extend([
        ['', ''],
        ['Direction Analysis', ''],
        ['Bitcoin Up → Eth Up', f'{len(pred_df[(pred_df.btc_direction == "up") & (pred_df.eth_direction == "up") & pred_df.eth_responded])}'],
        ['Bitcoin Down → Eth Down', f'{len(pred_df[(pred_df.btc_direction == "down") & (pred_df.eth_direction == "down") & pred_df.eth_responded])}'],
        ['Bitcoin Up → Eth Down', f'{len(pred_df[(pred_df.btc_direction == "up") & (pred_df.eth_direction == "down") & pred_df.eth_responded])}'],
        ['Bitcoin Down → Eth Up', f'{len(pred_df[(pred_df.btc_direction == "down") & (pred_df.eth_direction == "up") & pred_df.eth_responded])}'],
    ])
    
    table = ax4.table(cellText=stats_data, cellLoc='left', loc='center',
                     colWidths=[0.6, 0.4])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.3)
    
    # Style the header row
    for i in range(2):
        table[(0, i)].set_facecolor('#E6E6FA')
        table[(0, i)].set_text_props(weight='bold')
    
    # Style section headers
    for row_idx in [6, 12, 14]:
        if row_idx < len(stats_data):
            table[(row_idx, 0)].set_facecolor('#F0F0F0')
            table[(row_idx, 0)].set_text_props(weight='bold')
    
    ax4.set_title('Prediction Statistics', fontsize=12, pad=20)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Prediction analysis plot saved as: {output_file}")
    
    return fig, pred_df

def main():
    if len(sys.argv) < 2:
        print("Usage: python bitcoin_ethereum_prediction.py <l1_data_file> [title_filter] [start_hour] [end_hour] [threshold] [max_lag_ms]")
        print("Example: python bitcoin_ethereum_prediction.py l1_data.jsonl")
        print("Example: python bitcoin_ethereum_prediction.py l1_data.jsonl '12AM ET' 4 5 0.01 5000")
        sys.exit(1)
    
    input_file = sys.argv[1]
    title_filter = sys.argv[2] if len(sys.argv) >= 3 else None
    
    # Parse time range if provided
    time_filter = None
    if len(sys.argv) >= 5:
        try:
            start_hour = int(sys.argv[3])
            end_hour = int(sys.argv[4])
            if 0 <= start_hour <= 23 and 0 <= end_hour <= 23:
                time_filter = (start_hour, end_hour)
        except ValueError:
            print("Error: Hours must be integers")
            sys.exit(1)
    
    # Parse threshold and max lag
    threshold = float(sys.argv[5]) if len(sys.argv) >= 6 else 0.01  # 1 cent
    max_lag_ms = int(sys.argv[6]) if len(sys.argv) >= 7 else 5000   # 5 seconds
    
    try:
        # Load Bitcoin and Ethereum data
        bitcoin_df, ethereum_df = load_crypto_data(input_file, title_filter, time_filter)
        
        if bitcoin_df.empty or ethereum_df.empty:
            print("Need both Bitcoin and Ethereum data!")
            sys.exit(1)
        
        # Get the most active assets
        bitcoin_asset = bitcoin_df['asset_id'].value_counts().index[0]
        ethereum_asset = ethereum_df['asset_id'].value_counts().index[0]
        
        bitcoin_data = bitcoin_df[bitcoin_df['asset_id'] == bitcoin_asset].copy()
        ethereum_data = ethereum_df[ethereum_df['asset_id'] == ethereum_asset].copy()
        
        print(f"\nAnalyzing:")
        print(f"Bitcoin: {bitcoin_data['market_title'].iloc[0]} ({len(bitcoin_data)} points)")
        print(f"Ethereum: {ethereum_data['market_title'].iloc[0]} ({len(ethereum_data)} points)")
        
        # Prepare price series
        for df in [bitcoin_data, ethereum_data]:
            df['mid_price'] = pd.to_numeric(df['mid_price'], errors='coerce')
            df.dropna(subset=['mid_price'], inplace=True)
            df.set_index('datetime', inplace=True)
            df.sort_index(inplace=True)
        
        bitcoin_prices = bitcoin_data['mid_price']
        ethereum_prices = ethereum_data['mid_price']
        
        bitcoin_title = bitcoin_data['market_title'].iloc[0]
        ethereum_title = ethereum_data['market_title'].iloc[0]
        
        print(f"\nDetecting Bitcoin moves >{threshold:.2f} (1 cent)...")
        bitcoin_events = detect_significant_moves(bitcoin_prices, threshold=threshold)
        
        print(f"Found {len(bitcoin_events)} significant Bitcoin moves")
        
        if len(bitcoin_events) == 0:
            print("No significant Bitcoin moves found!")
            sys.exit(1)
        
        # Analyze predictive relationship
        print(f"Analyzing Ethereum responses within {max_lag_ms}ms...")
        predictions = analyze_predictive_relationship(bitcoin_events, ethereum_prices, max_lag_ms)
        
        print(f"Analyzed {len(predictions)} Bitcoin → Ethereum predictions")
        
        if len(predictions) == 0:
            print("No predictions could be analyzed!")
            sys.exit(1)
        
        # Create visualization
        filter_suffix = ""
        if title_filter:
            filter_suffix += f"_filtered_{title_filter.replace(' ', '_')}"
        if time_filter:
            filter_suffix += f"_time_{time_filter[0]:02d}h-{time_filter[1]:02d}h"
        
        output_file = f"bitcoin_ethereum_prediction{filter_suffix}_t{threshold:.3f}.png"
        
        fig, pred_df = create_prediction_analysis_plot(
            predictions, bitcoin_title, ethereum_title, output_file=output_file
        )
        
        # Print detailed results
        print(f"\n" + "="*70)
        print(f"BITCOIN → ETHEREUM PREDICTION ANALYSIS")
        print(f"="*70)
        print(f"Bitcoin Market: {bitcoin_title}")
        print(f"Ethereum Market: {ethereum_title}")
        print(f"Analysis Threshold: {threshold:.3f} (1 cent)")
        print(f"Maximum Response Time: {max_lag_ms} ms")
        
        total_predictions = len(pred_df)
        eth_responded = pred_df['eth_responded'].sum()
        direction_matched = pred_df['direction_match'].sum()
        successful_predictions = pred_df['successful_prediction'].sum()
        
        print(f"\nPREDICTION RESULTS:")
        print(f"  Total Bitcoin moves >1¢: {total_predictions}")
        print(f"  Ethereum responded >1¢: {eth_responded} ({eth_responded/total_predictions*100:.1f}%)")
        print(f"  Direction matched: {direction_matched} ({direction_matched/total_predictions*100:.1f}%)")
        print(f"  Successful predictions: {successful_predictions} ({successful_predictions/total_predictions*100:.1f}%)")
        
        if eth_responded > 0:
            responded_preds = pred_df[pred_df['eth_responded']]
            response_times = responded_preds['eth_response_lag_ms']
            
            print(f"\nRESPONSE TIMING:")
            print(f"  Median response time: {response_times.median():.0f} ms")
            print(f"  Mean response time: {response_times.mean():.0f} ms")
            print(f"  Response time range: {response_times.min():.0f} - {response_times.max():.0f} ms")
        
        # Direction analysis
        print(f"\nDIRECTION ANALYSIS:")
        up_up = len(pred_df[(pred_df.btc_direction == "up") & (pred_df.eth_direction == "up") & pred_df.eth_responded])
        down_down = len(pred_df[(pred_df.btc_direction == "down") & (pred_df.eth_direction == "down") & pred_df.eth_responded])
        up_down = len(pred_df[(pred_df.btc_direction == "up") & (pred_df.eth_direction == "down") & pred_df.eth_responded])
        down_up = len(pred_df[(pred_df.btc_direction == "down") & (pred_df.eth_direction == "up") & pred_df.eth_responded])
        
        print(f"  Bitcoin UP → Ethereum UP: {up_up}")
        print(f"  Bitcoin DOWN → Ethereum DOWN: {down_down}")
        print(f"  Bitcoin UP → Ethereum DOWN: {up_down}")
        print(f"  Bitcoin DOWN → Ethereum UP: {down_up}")
        
        # Trading insights
        print(f"\nTRADING INSIGHTS:")
        if successful_predictions > 0:
            success_rate = successful_predictions / total_predictions * 100
            print(f"  Prediction accuracy: {success_rate:.1f}%")
            
            if success_rate > 50:
                print(f"  → PROFITABLE: Success rate above random (50%)")
            else:
                print(f"  → NOT PROFITABLE: Success rate at/below random")
                
            if eth_responded > 0:
                avg_response_time = response_times.mean()
                print(f"  → Average reaction time: {avg_response_time:.0f}ms")
                print(f"  → Trading window: {avg_response_time:.0f}ms to capture Ethereum move")
        else:
            print(f"  → NO PREDICTIVE POWER: No successful predictions found")
        
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