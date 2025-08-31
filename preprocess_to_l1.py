#!/usr/bin/env python3
"""
Preprocess L2 order book data from capture_real_data.py to L1 format.
L1 format contains only the best bid/ask (top of book) data.

Now supports cloud architecture:
- Reads from structured partitioned format (year/month/day/hour)
- Supports gzip compressed files
- Batch processes multiple files
- Integrates with writer.py for consistent output
"""

import json
import sys
import os
import gzip
import glob
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Iterator
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timezone
from writer import RotatingGzipWriter

@dataclass
class L1Quote:
    """Represents L1 (top-of-book) data for a market."""
    ts_ms: int
    asset_id: str
    market: str
    market_title: str
    outcome: str
    best_bid_price: Optional[str] = None
    best_bid_size: Optional[str] = None
    best_ask_price: Optional[str] = None
    best_ask_size: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON output."""
        # Normalize "Down" and "No" outcomes to show probability of "Up"/"Yes"
        should_flip = self.outcome.lower() in ['down', 'no']
        
        if should_flip and self.best_bid_price and self.best_ask_price:
            try:
                # Flip prices: bid becomes (1 - ask), ask becomes (1 - bid)
                original_bid = Decimal(self.best_bid_price)
                original_ask = Decimal(self.best_ask_price)
                
                flipped_bid_price = str(1 - original_ask)
                flipped_ask_price = str(1 - original_bid)
                flipped_bid_size = self.best_ask_size  # Size follows the flipped price
                flipped_ask_size = self.best_bid_size  # Size follows the flipped price
                
            except Exception:
                # Fall back to original if flipping fails
                flipped_bid_price = self.best_bid_price
                flipped_ask_price = self.best_ask_price
                flipped_bid_size = self.best_bid_size
                flipped_ask_size = self.best_ask_size
        else:
            # No flipping needed or not possible
            flipped_bid_price = self.best_bid_price
            flipped_ask_price = self.best_ask_price
            flipped_bid_size = self.best_bid_size
            flipped_ask_size = self.best_ask_size
        
        return {
            "ts_ms": self.ts_ms,
            "asset_id": self.asset_id,
            "market": self.market,
            "market_title": self.market_title,
            "outcome": self.outcome,
            "best_bid_price": flipped_bid_price,
            "best_bid_size": flipped_bid_size,
            "best_ask_price": flipped_ask_price,
            "best_ask_size": flipped_ask_size,
            "spread": self.calculate_spread_flipped(flipped_bid_price, flipped_ask_price),
            "mid_price": self.calculate_mid_price_flipped(flipped_bid_price, flipped_ask_price)
        }
    
    def calculate_spread(self) -> Optional[str]:
        """Calculate bid-ask spread."""
        if self.best_bid_price and self.best_ask_price:
            try:
                bid = Decimal(self.best_bid_price)
                ask = Decimal(self.best_ask_price)
                return str(ask - bid)
            except:
                return None
        return None
    
    def calculate_mid_price(self) -> Optional[str]:
        """Calculate mid price."""
        if self.best_bid_price and self.best_ask_price:
            try:
                bid = Decimal(self.best_bid_price)
                ask = Decimal(self.best_ask_price)
                return str((bid + ask) / 2)
            except:
                return None
        return None
    
    def calculate_spread_flipped(self, bid_price: Optional[str], ask_price: Optional[str]) -> Optional[str]:
        """Calculate bid-ask spread for flipped prices."""
        if bid_price and ask_price:
            try:
                bid = Decimal(bid_price)
                ask = Decimal(ask_price)
                return str(ask - bid)
            except:
                return None
        return None
    
    def calculate_mid_price_flipped(self, bid_price: Optional[str], ask_price: Optional[str]) -> Optional[str]:
        """Calculate mid price for flipped prices."""
        if bid_price and ask_price:
            try:
                bid = Decimal(bid_price)
                ask = Decimal(ask_price)
                return str((bid + ask) / 2)
            except:
                return None
        return None

class L1Processor:
    """Processes L2 order book data and maintains L1 state."""
    
    def __init__(self):
        # Track current L1 state for each asset
        self.l1_state: Dict[str, L1Quote] = {}
        
    def extract_best_levels(self, bids: List[Dict], asks: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Extract best bid and ask from order book levels."""
        best_bid = None
        best_ask = None
        
        # Find best bid (highest price)
        if bids:
            try:
                best_bid = max(bids, key=lambda x: Decimal(x.get("price", "0")))
            except:
                best_bid = bids[0] if bids else None
        
        # Find best ask (lowest price)  
        if asks:
            try:
                best_ask = min(asks, key=lambda x: Decimal(x.get("price", "999999")))
            except:
                best_ask = asks[0] if asks else None
                
        return best_bid, best_ask
    
    def process_book_event(self, event: Dict) -> Optional[L1Quote]:
        """Process a 'book' event (full order book snapshot)."""
        asset_id = event.get("asset_id")
        if not asset_id:
            return None
            
        bids = event.get("bids", [])
        asks = event.get("asks", [])
        
        best_bid, best_ask = self.extract_best_levels(bids, asks)
        
        # Create or update L1 quote
        timestamp = event.get("timestamp") or event.get("ts_ms")
        if timestamp is not None:
            timestamp = int(timestamp)
        l1_quote = L1Quote(
            ts_ms=timestamp,
            asset_id=asset_id,
            market=event.get("market", ""),
            market_title=event.get("market_title", ""),
            outcome=event.get("outcome", "")
        )
        
        if best_bid:
            l1_quote.best_bid_price = best_bid.get("price")
            l1_quote.best_bid_size = best_bid.get("size")
            
        if best_ask:
            l1_quote.best_ask_price = best_ask.get("price")
            l1_quote.best_ask_size = best_ask.get("size")
        
        # Update state
        self.l1_state[asset_id] = l1_quote
        return l1_quote
    
    def process_price_change_event(self, event: Dict) -> Optional[L1Quote]:
        """Process a 'price_change' event (delta updates)."""
        asset_id = event.get("asset_id")
        if not asset_id or asset_id not in self.l1_state:
            return None
            
        changes = event.get("changes", [])
        current_l1 = self.l1_state[asset_id]
        updated = False
        
        for change in changes:
            price = change.get("price")
            side = change.get("side")  # "buy" or "sell"
            size = change.get("size")
            
            if not price or not side:
                continue
                
            try:
                price_decimal = Decimal(price)
                size_decimal = Decimal(size) if size else Decimal("0")
                
                # Update best bid if this is a buy-side change affecting the top
                if side == "buy":
                    current_bid_price = Decimal(current_l1.best_bid_price) if current_l1.best_bid_price else Decimal("0")
                    if price_decimal >= current_bid_price:
                        if size_decimal > 0:
                            current_l1.best_bid_price = price
                            current_l1.best_bid_size = size
                            updated = True
                        elif price_decimal == current_bid_price:
                            # Best bid was removed, we'd need full book to know new best
                            # For now, just mark as None
                            current_l1.best_bid_price = None
                            current_l1.best_bid_size = None
                            updated = True
                
                # Update best ask if this is a sell-side change affecting the top
                elif side == "sell":
                    current_ask_price = Decimal(current_l1.best_ask_price) if current_l1.best_ask_price else Decimal("999999")
                    if price_decimal <= current_ask_price:
                        if size_decimal > 0:
                            current_l1.best_ask_price = price
                            current_l1.best_ask_size = size
                            updated = True
                        elif price_decimal == current_ask_price:
                            # Best ask was removed, we'd need full book to know new best
                            # For now, just mark as None
                            current_l1.best_ask_price = None
                            current_l1.best_ask_size = None
                            updated = True
                            
            except Exception as e:
                print(f"Error processing price change: {e}", file=sys.stderr)
                continue
        
        if updated:
            timestamp = event.get("timestamp") or event.get("ts_ms")
            if timestamp is not None:
                timestamp = int(timestamp)
            current_l1.ts_ms = timestamp
            return current_l1
            
        return None
    
    def process_event(self, event: Dict) -> Optional[L1Quote]:
        """Process any event and return L1 quote if updated."""
        event_type = event.get("event_type")
        
        if event_type == "book":
            return self.process_book_event(event)
        elif event_type == "price_change":
            return self.process_price_change_event(event)
        elif event_type == "last_trade_price":
            # Trade events don't directly update the book, but we could track last trade price
            # For now, just return None as L1 book data doesn't change
            return None
        else:
            return None

def discover_input_files(input_path: str, start_date: str = None, end_date: str = None) -> List[str]:
    """
    Discover input files from VM's local structured storage.
    
    Args:
        input_path: Local path on VM (typically /var/data/polymarket)
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
    
    Returns:
        List of file paths to process in chronological order
    """
    files = []
    
    # Handle single file case
    if input_path.endswith('.jsonl') or input_path.endswith('.jsonl.gz'):
        if os.path.exists(input_path):
            return [input_path]
        else:
            raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Handle directory with structured format (VM local storage)
    if os.path.isdir(input_path):
        # Look for year=*/month=*/day=*/hour=*/events-*.jsonl.gz pattern
        pattern = os.path.join(input_path, "year=*", "month=*", "day=*", "hour=*", "events-*.jsonl.gz")
        discovered = glob.glob(pattern)
        files.extend(discovered)
        
        # Also look for flat .jsonl and .jsonl.gz files (legacy format)
        for ext in ["*.jsonl", "*.jsonl.gz"]:
            pattern = os.path.join(input_path, ext)
            files.extend(glob.glob(pattern))
        
        print(f"Discovered {len(discovered)} partitioned files, {len(files) - len(discovered)} legacy files")
    
    files.sort()  # Process in chronological order
    
    if not files:
        raise FileNotFoundError(f"No input files found in: {input_path}")
    
    print(f"Total files to process: {len(files)}")
    if len(files) <= 10:
        for f in files:
            print(f"  - {f}")
    else:
        print(f"  - {files[0]} ... {files[-1]} (showing first and last)")
    
    return files

def open_file_smart(filepath: str):
    """Open file with automatic gzip detection."""
    if filepath.endswith('.gz'):
        return gzip.open(filepath, 'rt', encoding='utf-8')
    else:
        return open(filepath, 'r', encoding='utf-8')

def preprocess_l2_to_l1_batch(input_files: List[str], output_writer: Union[str, RotatingGzipWriter]):
    """
    Process multiple L2 files to L1 format using cloud-native writer.
    
    Args:
        input_files: List of input file paths
        output_writer: Either output filename string or RotatingGzipWriter instance
    """
    processor = L1Processor()
    events_processed = 0
    l1_updates = 0
    no_network_events = 0
    files_processed = 0
    
    # Track timestamps and network outage state
    last_event_timestamp = None
    in_network_outage = False
    NETWORK_TIMEOUT_MS = 1000  # 1 second in milliseconds
    
    # Initialize writer
    if isinstance(output_writer, str):
        # Legacy mode: single file output
        writer = None
        output_file = output_writer
        outfile = open(output_file, 'w')
        print(f"Using legacy single-file output: {output_file}")
    else:
        # Cloud mode: structured writer
        writer = output_writer
        outfile = None
        print(f"Using cloud-native structured writer")
    
    try:
        for input_file in input_files:
            print(f"Processing: {input_file}")
            files_processed += 1
            
            try:
                with open_file_smart(input_file) as infile:
                    for line_num, line in enumerate(infile, 1):
                        line = line.strip()
                        if not line:
                            continue
                            
                        try:
                            event = json.loads(line)
                            events_processed += 1
                            
                            current_timestamp = event.get("timestamp")
                            current_timestamp = int(current_timestamp)
                            
                            event_type = event.get("event_type")
                            
                            # Skip "book" events for outage detection timing
                            if event_type != "book":
                                # Check for network outage (gap > 1 second since last event)
                                if (last_event_timestamp is not None and
                                    current_timestamp is not None and
                                    current_timestamp - last_event_timestamp > NETWORK_TIMEOUT_MS):
                                    
                                    gap_duration = current_timestamp - last_event_timestamp
                                    
                                    # Only write marker if we weren't already in an outage
                                    if not in_network_outage:
                                        # Write "no network event" marker
                                        no_network_marker = {
                                            "ts_ms": last_event_timestamp + NETWORK_TIMEOUT_MS,
                                            "event_type": "no_network_event",
                                            "gap_duration_ms": gap_duration,
                                            "message": "Network outage detected - no events received"
                                        }
                                        
                                        if writer:
                                            writer.write(no_network_marker)
                                        else:
                                            outfile.write(json.dumps(no_network_marker) + '\n')
                                        
                                        no_network_events += 1
                                    
                                    # Stay in outage state until we see normal activity
                                    in_network_outage = True
                                else:
                                    # Normal gap - we're out of outage state
                                    in_network_outage = False
                                
                                # Update last event timestamp for next iteration (exclude book events)
                                if current_timestamp is not None:
                                    last_event_timestamp = current_timestamp
                            
                            # Process event and get L1 update if any
                            l1_quote = processor.process_event(event)
                            
                            if l1_quote:
                                # Write L1 update to output
                                l1_data = l1_quote.to_dict()
                                
                                if writer:
                                    writer.write(l1_data)
                                else:
                                    outfile.write(json.dumps(l1_data) + '\n')
                                
                                l1_updates += 1
                                
                                if l1_updates % 1000 == 0:
                                    print(f"Processed {events_processed} events, generated {l1_updates} L1 updates, {no_network_events} network outage markers...")
                                    
                        except json.JSONDecodeError as e:
                            print(f"JSON decode error in {input_file} line {line_num}: {e}", file=sys.stderr)
                            continue
                        except Exception as e:
                            print(f"Error processing {input_file} line {line_num}: {e}", file=sys.stderr)
                            continue
                            
            except Exception as e:
                print(f"Error processing file {input_file}: {e}", file=sys.stderr)
                continue
            
            print(f"Completed: {input_file}")
                    
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if outfile:
            outfile.close()
    
    print(f"Batch preprocessing complete!")
    print(f"Files processed: {files_processed}")
    print(f"Total events processed: {events_processed}")
    print(f"L1 updates generated: {l1_updates}")
    print(f"Network outage markers added: {no_network_events}")

def preprocess_l2_to_l1(input_file: str, output_file: str):
    """Legacy single-file preprocessing function."""
    files = discover_input_files(input_file)
    preprocess_l2_to_l1_batch(files, output_file)

def main():
    """Enhanced main function with cloud VM architecture support."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Preprocess L2 order book data to L1 format on cloud VM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Legacy mode - single file to single file
  python preprocess_to_l1.py orderbook_clip.jsonl l1_data.jsonl
  
  # Cloud mode - process all VM data with structured output
  python preprocess_to_l1.py /var/data/polymarket --cloud-output /var/data/polymarket/l1
  
  # Batch process specific directory to single file
  python preprocess_to_l1.py /var/data/polymarket/year=2024/month=08 l1_august.jsonl
  
  # Process yesterday's data
  python preprocess_to_l1.py /var/data/polymarket/year=2024/month=08/day=30 l1_aug30.jsonl
        """
    )
    
    parser.add_argument('input', help='Input file or directory path (VM local paths)')
    parser.add_argument('output', nargs='?', help='Output file path (optional if using --cloud-output)')
    parser.add_argument('--cloud-output', metavar='DIR', help='Use cloud-native structured output to directory')
    parser.add_argument('--start-date', help='Start date filter (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date filter (YYYY-MM-DD)')
    parser.add_argument('--data-root', default='/var/data/polymarket',
                       help='VM data root directory (default: /var/data/polymarket)')
    
    args = parser.parse_args()
    
    # Validation
    if not args.cloud_output and not args.output:
        parser.error("Must specify either 'output' file or --cloud-output directory")
    
    if args.cloud_output and args.output:
        parser.error("Cannot specify both 'output' file and --cloud-output directory")
    
    try:
        # Discover input files
        input_files = discover_input_files(args.input, args.start_date, args.end_date)
        
        if args.cloud_output:
            # Use cloud-native structured writer
            print(f"Using structured cloud output: {args.cloud_output}")
            os.makedirs(args.cloud_output, exist_ok=True)
            
            # Create writer with custom data root
            writer = RotatingGzipWriter(args.cloud_output)
            preprocess_l2_to_l1_batch(input_files, writer)
        else:
            # Use legacy single-file output
            print(f"Using single-file output: {args.output}")
            preprocess_l2_to_l1_batch(input_files, args.output)
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    # Support legacy command line for backwards compatibility
    if len(sys.argv) == 3 and not any(arg.startswith('-') for arg in sys.argv[1:]):
        # Legacy mode: python preprocess_to_l1.py input.jsonl output.jsonl
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        preprocess_l2_to_l1(input_file, output_file)
    else:
        # New enhanced mode
        main()