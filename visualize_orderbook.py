#!/usr/bin/env python3
"""
Visualize order book data for the top market (most events).
Shows highest bid (green) and lowest ask (red) over time.
"""

import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from collections import defaultdict

def load_and_process_data(filename, target_market):
    """
    Load orderbook data and filter for target market.
    Returns list of (timestamp, highest_bid, lowest_ask) tuples.
    """
    data_points = []
    line_count = 0
    target_events = 0
    
    print(f"Processing all data for market: {target_market}")
    
    with open(filename, 'r') as f:
        for line in f:
            line_count += 1
            
            # Progress indicator
            if line_count % 1000 == 0:
                print(f"Processed {line_count} lines, found {len(data_points)} data points so far...")
                
            try:
                event = json.loads(line)
                
                # Filter for target market and book events only
                if (event.get('market_title') == target_market and
                    event.get('event_type') == 'book'):
                    
                    target_events += 1
                    bids = event.get('bids', [])
                    asks = event.get('asks', [])
                    
                    if bids and asks:
                        # Find highest bid (max price among bids)
                        highest_bid = max(float(bid['price']) for bid in bids)
                        
                        # Find lowest ask (min price among asks)
                        lowest_ask = min(float(ask['price']) for ask in asks)
                        
                        # Use timestamp from the event
                        ts_ms = event.get('ts_ms', 0)
                        timestamp = datetime.fromtimestamp(ts_ms / 1000)
                        
                        data_points.append((timestamp, highest_bid, lowest_ask))
                        
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                # Skip malformed lines silently
                continue
    
    print(f"Finished! Processed {line_count} total lines, found {target_events} target book events, extracted {len(data_points)} data points")
    return sorted(data_points)  # Sort by timestamp

def create_visualization(data_points, market_title):
    """
    Create a line plot showing highest bid (green) and lowest ask (red) over time.
    """
    if not data_points:
        print("No data points to visualize!")
        return
        
    # Separate the data
    timestamps = [point[0] for point in data_points]
    highest_bids = [point[1] for point in data_points]
    lowest_asks = [point[2] for point in data_points]
    
    # Create the plot
    plt.figure(figsize=(14, 8))
    
    # Plot the lines
    plt.plot(timestamps, highest_bids, color='green', linewidth=1.5, 
             label='Highest Bid', alpha=0.8)
    plt.plot(timestamps, lowest_asks, color='red', linewidth=1.5, 
             label='Lowest Ask', alpha=0.8)
    
    # Customize the plot
    plt.title(f'Order Book Spread Over Time\n{market_title}', fontsize=14, pad=20)
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Price', fontsize=12)
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    
    # Format x-axis to show time nicely
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.gca().xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    plt.xticks(rotation=45)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Add some stats
    bid_range = f"Bid range: ${min(highest_bids):.3f} - ${max(highest_bids):.3f}"
    ask_range = f"Ask range: ${min(lowest_asks):.3f} - ${max(lowest_asks):.3f}"
    spread_avg = f"Avg spread: ${sum(ask - bid for bid, ask in zip(highest_bids, lowest_asks)) / len(data_points):.3f}"
    
    plt.figtext(0.02, 0.02, f"{bid_range} | {ask_range} | {spread_avg}", 
                fontsize=9, style='italic')
    
    # Save and show
    output_file = 'orderbook_visualization.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Visualization saved as {output_file}")
    plt.show()

def main():
    filename = 'orderbook_clip.jsonl'
    target_market = "Ethereum Up or Down - August 17, 3PM ET"
    
    print(f"Loading data from {filename}")
    print(f"Target market: {target_market}")
    print()
    
    # Load and process the data
    data_points = load_and_process_data(filename, target_market)
    
    if data_points:
        print(f"Time range: {data_points[0][0]} to {data_points[-1][0]}")
        print(f"Data points: {len(data_points)}")
        print()
        
        # Create visualization
        create_visualization(data_points, target_market)
    else:
        print("No data found for the specified market!")

if __name__ == "__main__":
    main()