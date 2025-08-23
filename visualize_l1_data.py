#!/usr/bin/env python3
"""
Visualize L1 order book data for the top 5 most active assets.
Plots bids (solid lines) and asks (dashed lines) over time with normalized prices (0-100 scale).
"""

import json
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from collections import Counter
import numpy as np

def load_and_analyze_l1_data(filename):
    """Load L1 data and identify top 5 most active assets."""
    print(f"Loading L1 data from {filename}...")
    
    data = []
    asset_counts = Counter()
    
    with open(filename, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                record = json.loads(line)
                data.append(record)
                asset_counts[record['asset_id']] += 1
                
                if line_num % 1000 == 0:
                    print(f"Loaded {line_num} records...")
                    
            except json.JSONDecodeError as e:
                print(f"JSON decode error on line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    print(f"Loaded {len(data)} total records")
    
    # Get top 5 most active assets
    top_5_assets = [asset_id for asset_id, count in asset_counts.most_common(5)]
    print(f"\nTop 5 most active assets:")
    for i, (asset_id, count) in enumerate(asset_counts.most_common(5), 1):
        print(f"{i}. {asset_id}: {count} updates")
    
    return data, top_5_assets

def prepare_data_for_plotting(data, top_5_assets):
    """Prepare data for plotting with normalized prices."""
    print("\nPreparing data for plotting...")
    
    # Filter data for top 5 assets and convert to DataFrame
    filtered_data = [record for record in data if record['asset_id'] in top_5_assets]
    df = pd.DataFrame(filtered_data)
    
    # Convert timestamp to datetime
    df['datetime'] = pd.to_datetime(df['ts_ms'], unit='ms')
    
    # Convert price columns to numeric, handling None values
    df['best_bid_price'] = pd.to_numeric(df['best_bid_price'], errors='coerce')
    df['best_ask_price'] = pd.to_numeric(df['best_ask_price'], errors='coerce')
    
    # Group by asset and normalize prices
    asset_data = {}
    
    for asset_id in top_5_assets:
        asset_df = df[df['asset_id'] == asset_id].copy()
        
        # Get market title for this asset
        market_title = asset_df['market_title'].iloc[0] if not asset_df.empty else f"Asset {asset_id[:8]}..."
        
        # Remove rows where both bid and ask are null
        asset_df = asset_df.dropna(subset=['best_bid_price', 'best_ask_price'], how='all')
        
        if asset_df.empty:
            continue
            
        # Get all non-null prices for normalization
        all_prices = []
        if not asset_df['best_bid_price'].isna().all():
            all_prices.extend(asset_df['best_bid_price'].dropna().tolist())
        if not asset_df['best_ask_price'].isna().all():
            all_prices.extend(asset_df['best_ask_price'].dropna().tolist())
        
        if not all_prices:
            continue
            
        min_price = min(all_prices)
        max_price = max(all_prices)
        price_range = max_price - min_price
        
        # Normalize prices to 0-100 scale
        if price_range > 0:
            asset_df['normalized_bid'] = ((asset_df['best_bid_price'] - min_price) / price_range) * 100
            asset_df['normalized_ask'] = ((asset_df['best_ask_price'] - min_price) / price_range) * 100
        else:
            # If no price variation, set to 50
            asset_df['normalized_bid'] = 50
            asset_df['normalized_ask'] = 50
        
        asset_data[asset_id] = {
            'df': asset_df,
            'market_title': market_title,
            'min_price': min_price,
            'max_price': max_price,
            'original_range': f"{min_price:.4f} - {max_price:.4f}"
        }
        
        print(f"Asset {asset_id[:8]}... ({market_title[:30]}...): {len(asset_df)} records, price range: {min_price:.4f} - {max_price:.4f}")
    
    return asset_data

def create_visualization(asset_data, output_filename='l1_visualization.png'):
    """Create the visualization plot."""
    print(f"\nCreating visualization...")
    
    # Set up the plot
    plt.figure(figsize=(15, 10))
    
    # Define colors for each asset
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    # Plot data for each asset
    for i, (asset_id, data) in enumerate(asset_data.items()):
        df = data['df']
        color = colors[i % len(colors)]
        label = data['market_title'][:40] + "..." if len(data['market_title']) > 40 else data['market_title']
        
        # Plot bids (solid lines)
        bid_mask = ~df['normalized_bid'].isna()
        if bid_mask.any():
            plt.plot(df[bid_mask]['datetime'], df[bid_mask]['normalized_bid'], 
                    color=color, linestyle='-', linewidth=1.5, 
                    label=f'{label} (Bid)', alpha=0.8)
        
        # Plot asks (dashed lines)
        ask_mask = ~df['normalized_ask'].isna()
        if ask_mask.any():
            plt.plot(df[ask_mask]['datetime'], df[ask_mask]['normalized_ask'], 
                    color=color, linestyle='--', linewidth=1.5, 
                    label=f'{label} (Ask)', alpha=0.8)
    
    # Customize the plot
    plt.title('Top 5 Assets - Normalized L1 Order Book Data\n(Bids: Solid Lines, Asks: Dashed Lines)', 
              fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Normalized Price (0-100 Scale)', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Format x-axis
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    plt.xticks(rotation=45)
    
    # Set y-axis limits
    plt.ylim(-5, 105)
    
    # Add legend
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout()
    
    # Add text box with normalization info
    info_text = "Note: Prices normalized to 0-100 scale per asset\n(0 = min price, 100 = max price for each asset)"
    plt.figtext(0.02, 0.02, info_text, fontsize=9, style='italic', 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray", alpha=0.7))
    
    # Save the plot
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    print(f"Visualization saved as: {output_filename}")
    
    # Show summary statistics
    print(f"\nSummary:")
    for asset_id, data in asset_data.items():
        print(f"Asset {asset_id[:8]}...: {data['market_title'][:50]}...")
        print(f"  Original price range: {data['original_range']}")
        print(f"  Data points: {len(data['df'])}")
        print()
    
    return output_filename

def main():
    if len(sys.argv) != 2:
        print("Usage: python visualize_l1_data.py <l1_data_file>")
        print("Example: python visualize_l1_data.py test_l1_output.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        # Load and analyze data
        data, top_5_assets = load_and_analyze_l1_data(input_file)
        
        if not top_5_assets:
            print("No assets found in data!")
            sys.exit(1)
        
        # Prepare data for plotting
        asset_data = prepare_data_for_plotting(data, top_5_assets)
        
        if not asset_data:
            print("No valid data found for plotting!")
            sys.exit(1)
        
        # Create visualization
        output_file = create_visualization(asset_data)
        
        print(f"\nVisualization complete! Open {output_file} to view the graph.")
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()