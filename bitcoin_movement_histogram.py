#!/usr/bin/env python3
"""
Bitcoin Movement Histogram Analysis.
Analyzes Bitcoin price movements in 100ms windows and creates histograms.
"""

import json
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from scipy import stats

def load_bitcoin_data(filename, title_filter=None, time_filter=None):
    """Load Bitcoin L1 data with optional filtering."""
    print(f"Loading Bitcoin data from {filename}...")
    if title_filter:
        print(f"Filtering for markets containing: '{title_filter}'")
    if time_filter:
        print(f"Time filter: {time_filter[0]:02d}:00 - {time_filter[1]:02d}:00 UTC")
    
    data = []
    
    with open(filename, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                record = json.loads(line)
                
                # Filter for Bitcoin markets
                market_title = record.get('market_title', '').lower()
                if 'bitcoin' not in market_title:
                    continue
                
                # Apply additional title filter if specified
                if title_filter and title_filter.lower() not in market_title:
                    continue
                
                data.append(record)
                
                if line_num % 10000 == 0:
                    print(f"Processed {line_num} records...")
                    
            except json.JSONDecodeError as e:
                print(f"JSON decode error on line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    print(f"Loaded {len(data)} Bitcoin records")
    
    if len(data) == 0:
        print("No Bitcoin data found!")
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    df['datetime'] = pd.to_datetime(df['ts_ms'], unit='ms')
    
    # Apply time range filter if specified
    if time_filter:
        start_hour, end_hour = time_filter
        df['hour'] = df['datetime'].dt.hour
        df = df[(df['hour'] >= start_hour) & (df['hour'] < end_hour)]
        print(f"After time filtering: {len(df)} records")
    
    return df

def calculate_windowed_movements(price_series, window_ms=100):
    """
    Calculate price movements in fixed time windows.
    
    Parameters:
    - price_series: pandas Series with datetime index and price values
    - window_ms: window size in milliseconds
    
    Returns:
    - movements: list of percentage movements in each window
    """
    if len(price_series) < 2:
        return []
    
    # Convert window to timedelta
    window_td = pd.Timedelta(milliseconds=window_ms)
    
    # Get time range
    start_time = price_series.index.min()
    end_time = price_series.index.max()
    
    movements = []
    current_time = start_time
    
    while current_time + window_td <= end_time:
        window_end = current_time + window_td
        
        # Get prices at start and end of window
        start_price = price_series.asof(current_time)
        end_price = price_series.asof(window_end)
        
        if pd.notna(start_price) and pd.notna(end_price) and start_price > 0:
            movement = (end_price - start_price) / start_price
            movements.append(movement)
        
        current_time += window_td
    
    return movements

def analyze_movements(movements):
    """Analyze statistical properties of movements."""
    if len(movements) == 0:
        return None
    
    movements_array = np.array(movements)
    
    # Remove extreme outliers (beyond 3 standard deviations)
    mean_mov = np.mean(movements_array)
    std_mov = np.std(movements_array)
    filtered_movements = movements_array[np.abs(movements_array - mean_mov) <= 3 * std_mov]
    
    stats_dict = {
        'count': len(movements),
        'count_filtered': len(filtered_movements),
        'mean': np.mean(filtered_movements),
        'median': np.median(filtered_movements),
        'std': np.std(filtered_movements),
        'min': np.min(filtered_movements),
        'max': np.max(filtered_movements),
        'q1': np.percentile(filtered_movements, 25),
        'q3': np.percentile(filtered_movements, 75),
        'skewness': stats.skew(filtered_movements),
        'kurtosis': stats.kurtosis(filtered_movements),
        'zero_movements': np.sum(filtered_movements == 0),
        'positive_movements': np.sum(filtered_movements > 0),
        'negative_movements': np.sum(filtered_movements < 0),
    }
    
    # Calculate percentage of movements by magnitude
    abs_movements = np.abs(filtered_movements)
    stats_dict['pct_tiny'] = np.sum(abs_movements < 0.001) / len(filtered_movements) * 100  # <0.1%
    stats_dict['pct_small'] = np.sum((abs_movements >= 0.001) & (abs_movements < 0.01)) / len(filtered_movements) * 100  # 0.1-1%
    stats_dict['pct_medium'] = np.sum((abs_movements >= 0.01) & (abs_movements < 0.05)) / len(filtered_movements) * 100  # 1-5%
    stats_dict['pct_large'] = np.sum(abs_movements >= 0.05) / len(filtered_movements) * 100  # >5%
    
    return stats_dict, filtered_movements

def create_movement_histogram(movements, window_ms, market_title, stats_dict, output_file=None):
    """Create histogram visualization of price movements."""
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # Convert to percentage for display
    movements_pct = np.array(movements) * 100
    
    # Main histogram
    n_bins = min(50, len(movements) // 10)
    ax1.hist(movements_pct, bins=n_bins, alpha=0.7, color='steelblue', density=True)
    
    # Add normal distribution overlay
    mu, sigma = np.mean(movements_pct), np.std(movements_pct)
    x = np.linspace(movements_pct.min(), movements_pct.max(), 100)
    ax1.plot(x, stats.norm.pdf(x, mu, sigma), 'r-', linewidth=2, label=f'Normal(μ={mu:.3f}, σ={sigma:.3f})')
    
    ax1.axvline(0, color='black', linestyle='--', alpha=0.5, label='No Change')
    ax1.axvline(np.median(movements_pct), color='orange', linestyle='--', alpha=0.7, label=f'Median: {np.median(movements_pct):.3f}%')
    
    ax1.set_title(f'Bitcoin Price Movements ({window_ms}ms windows)\n{market_title}', fontsize=12)
    ax1.set_xlabel('Price Movement (%)')
    ax1.set_ylabel('Density')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Log scale histogram for better visibility of tails
    ax2.hist(movements_pct, bins=n_bins, alpha=0.7, color='steelblue', density=True)
    ax2.set_yscale('log')
    ax2.axvline(0, color='black', linestyle='--', alpha=0.5)
    ax2.set_title('Log Scale (Better Tail Visibility)', fontsize=12)
    ax2.set_xlabel('Price Movement (%)')
    ax2.set_ylabel('Log Density')
    ax2.grid(True, alpha=0.3)
    
    # Absolute movements histogram
    abs_movements_pct = np.abs(movements_pct)
    ax3.hist(abs_movements_pct, bins=n_bins//2, alpha=0.7, color='green', density=True)
    ax3.set_title('Absolute Movement Magnitudes', fontsize=12)
    ax3.set_xlabel('|Price Movement| (%)')
    ax3.set_ylabel('Density')
    ax3.grid(True, alpha=0.3)
    
    # Add magnitude thresholds
    thresholds = [0.1, 1.0, 5.0]  # 0.1%, 1%, 5%
    colors = ['orange', 'red', 'darkred']
    for threshold, color in zip(thresholds, colors):
        ax3.axvline(threshold, color=color, linestyle=':', alpha=0.7, label=f'{threshold}%')
    ax3.legend()
    
    # Statistics summary
    ax4.axis('off')
    
    # Create statistics table
    if stats_dict:
        table_data = [
            ['Metric', 'Value'],
            ['Total Windows', f"{stats_dict['count']:,}"],
            ['After Filtering', f"{stats_dict['count_filtered']:,}"],
            ['Mean Movement', f"{stats_dict['mean']*100:.4f}%"],
            ['Median Movement', f"{stats_dict['median']*100:.4f}%"],
            ['Std Deviation', f"{stats_dict['std']*100:.4f}%"],
            ['Min Movement', f"{stats_dict['min']*100:.3f}%"],
            ['Max Movement', f"{stats_dict['max']*100:.3f}%"],
            ['Skewness', f"{stats_dict['skewness']:.3f}"],
            ['Kurtosis', f"{stats_dict['kurtosis']:.3f}"],
            ['', ''],
            ['Movement Distribution', ''],
            ['No Change (0%)', f"{stats_dict['zero_movements']:,}"],
            ['Positive Moves', f"{stats_dict['positive_movements']:,}"],
            ['Negative Moves', f"{stats_dict['negative_movements']:,}"],
            ['', ''],
            ['By Magnitude', ''],
            ['Tiny (<0.1%)', f"{stats_dict['pct_tiny']:.1f}%"],
            ['Small (0.1-1%)', f"{stats_dict['pct_small']:.1f}%"],
            ['Medium (1-5%)', f"{stats_dict['pct_medium']:.1f}%"],
            ['Large (>5%)', f"{stats_dict['pct_large']:.1f}%"],
        ]
        
        table = ax4.table(cellText=table_data, cellLoc='left', loc='center',
                         colWidths=[0.6, 0.4])
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.2)
        
        # Style the header row
        for i in range(2):
            table[(0, i)].set_facecolor('#E6E6FA')
            table[(0, i)].set_text_props(weight='bold')
        
        # Style section headers
        for row_idx in [11, 16]:
            if row_idx < len(table_data):
                table[(row_idx, 0)].set_facecolor('#F0F0F0')
                table[(row_idx, 0)].set_text_props(weight='bold')
    
    ax4.set_title('Statistical Summary', fontsize=12, pad=20)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Bitcoin movement histogram saved as: {output_file}")
    
    return fig

def main():
    if len(sys.argv) < 2:
        print("Usage: python bitcoin_movement_histogram.py <l1_data_file> [title_filter] [start_hour] [end_hour] [window_ms]")
        print("Example: python bitcoin_movement_histogram.py l1_data.jsonl")
        print("Example: python bitcoin_movement_histogram.py l1_data.jsonl '12AM ET' 4 5 100")
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
    
    # Parse window size
    window_ms = int(sys.argv[5]) if len(sys.argv) >= 6 else 100
    
    try:
        # Load Bitcoin data
        df = load_bitcoin_data(input_file, title_filter, time_filter)
        
        if df.empty:
            print("No Bitcoin data found!")
            sys.exit(1)
        
        # Get the most active Bitcoin asset
        asset_counts = df['asset_id'].value_counts()
        if len(asset_counts) == 0:
            print("No Bitcoin assets found!")
            sys.exit(1)
        
        top_bitcoin_asset = asset_counts.index[0]
        bitcoin_data = df[df['asset_id'] == top_bitcoin_asset].copy()
        
        print(f"\nAnalyzing Bitcoin asset: {top_bitcoin_asset}")
        print(f"Market: {bitcoin_data['market_title'].iloc[0]}")
        print(f"Data points: {len(bitcoin_data)}")
        
        # Prepare price series
        bitcoin_data['mid_price'] = pd.to_numeric(bitcoin_data['mid_price'], errors='coerce')
        bitcoin_data = bitcoin_data.dropna(subset=['mid_price'])
        bitcoin_data = bitcoin_data.set_index('datetime').sort_index()
        
        if len(bitcoin_data) < 10:
            print("Insufficient Bitcoin data for analysis!")
            sys.exit(1)
        
        price_series = bitcoin_data['mid_price']
        market_title = bitcoin_data['market_title'].iloc[0]
        
        print(f"Time range: {price_series.index.min()} to {price_series.index.max()}")
        print(f"Price range: {price_series.min():.4f} to {price_series.max():.4f}")
        
        # Calculate windowed movements
        print(f"\nCalculating {window_ms}ms windowed movements...")
        movements = calculate_windowed_movements(price_series, window_ms=window_ms)
        
        if len(movements) == 0:
            print("No movements calculated!")
            sys.exit(1)
        
        print(f"Calculated {len(movements)} movement windows")
        
        # Analyze movements
        stats_dict, filtered_movements = analyze_movements(movements)
        
        if stats_dict is None:
            print("No valid movements for analysis!")
            sys.exit(1)
        
        # Create visualization
        filter_suffix = ""
        if title_filter:
            filter_suffix += f"_filtered_{title_filter.replace(' ', '_')}"
        if time_filter:
            filter_suffix += f"_time_{time_filter[0]:02d}h-{time_filter[1]:02d}h"
        
        output_file = f"bitcoin_movements_{window_ms}ms{filter_suffix}.png"
        
        fig = create_movement_histogram(
            filtered_movements, window_ms, market_title, stats_dict, output_file=output_file
        )
        
        # Print detailed results
        print(f"\n" + "="*60)
        print(f"BITCOIN MOVEMENT ANALYSIS ({window_ms}ms windows)")
        print(f"="*60)
        print(f"Market: {market_title}")
        print(f"Analysis period: {price_series.index.min()} to {price_series.index.max()}")
        print(f"Total windows analyzed: {stats_dict['count']:,}")
        print(f"After outlier filtering: {stats_dict['count_filtered']:,}")
        
        print(f"\nMovement Statistics:")
        print(f"  Mean movement: {stats_dict['mean']*100:.4f}%")
        print(f"  Median movement: {stats_dict['median']*100:.4f}%")
        print(f"  Standard deviation: {stats_dict['std']*100:.4f}%")
        print(f"  Range: {stats_dict['min']*100:.3f}% to {stats_dict['max']*100:.3f}%")
        
        print(f"\nDistribution Properties:")
        print(f"  Skewness: {stats_dict['skewness']:.3f} {'(right-skewed)' if stats_dict['skewness'] > 0 else '(left-skewed)' if stats_dict['skewness'] < 0 else '(symmetric)'}")
        print(f"  Kurtosis: {stats_dict['kurtosis']:.3f} {'(heavy tails)' if stats_dict['kurtosis'] > 0 else '(light tails)'}")
        
        print(f"\nMovement Direction:")
        print(f"  No change: {stats_dict['zero_movements']:,} ({stats_dict['zero_movements']/stats_dict['count_filtered']*100:.1f}%)")
        print(f"  Positive moves: {stats_dict['positive_movements']:,} ({stats_dict['positive_movements']/stats_dict['count_filtered']*100:.1f}%)")
        print(f"  Negative moves: {stats_dict['negative_movements']:,} ({stats_dict['negative_movements']/stats_dict['count_filtered']*100:.1f}%)")
        
        print(f"\nMovement Magnitude Distribution:")
        print(f"  Tiny (<0.1%): {stats_dict['pct_tiny']:.1f}%")
        print(f"  Small (0.1-1%): {stats_dict['pct_small']:.1f}%")
        print(f"  Medium (1-5%): {stats_dict['pct_medium']:.1f}%")
        print(f"  Large (>5%): {stats_dict['pct_large']:.1f}%")
        
        # Trading insights
        print(f"\nTrading Insights:")
        significant_moves = stats_dict['pct_medium'] + stats_dict['pct_large']
        print(f"  Significant moves (>1%): {significant_moves:.1f}% of windows")
        print(f"  Average time between significant moves: {window_ms * 100 / significant_moves:.0f}ms")
        
        if stats_dict['mean'] > 0:
            print(f"  Slight upward bias: +{stats_dict['mean']*100:.4f}% per {window_ms}ms window")
        elif stats_dict['mean'] < 0:
            print(f"  Slight downward bias: {stats_dict['mean']*100:.4f}% per {window_ms}ms window")
        else:
            print(f"  No directional bias")
        
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