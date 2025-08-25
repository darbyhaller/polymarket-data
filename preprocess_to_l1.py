#!/usr/bin/env python3
"""
Preprocess L2 order book data from capture_real_data.py to L1 format.
L1 format contains only the best bid/ask (top of book) data.
"""

import json
import sys
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal

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

def preprocess_l2_to_l1(input_file: str, output_file: str):
    """Main preprocessing function."""
    processor = L1Processor()
    events_processed = 0
    l1_updates = 0
    no_network_events = 0
    
    # Track timestamps and network outage state
    last_event_timestamp = None
    in_network_outage = False
    NETWORK_TIMEOUT_MS = 1000  # 1 second in milliseconds
    
    print(f"Processing L2 data from {input_file}...")
    
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    event = json.loads(line)
                    events_processed += 1
                    current_timestamp = event.get("timestamp") or event.get("ts_ms")
                    if current_timestamp is not None:
                        current_timestamp = int(current_timestamp)
                    
                    event_type = event.get("event_type")
                    
                    # Skip "book" events for outage detection timing
                    if event_type != "book":
                        # Check for network outage (gap > 1 second since last event)
                        if (last_event_timestamp is not None and
                            current_timestamp is not None and
                            current_timestamp - last_event_timestamp > NETWORK_TIMEOUT_MS):
                            
                            gap_duration = current_timestamp - last_event_timestamp
                            print(f"DEBUG: Gap detected: {gap_duration}ms, in_outage: {in_network_outage}, last_ts: {last_event_timestamp}, curr_ts: {current_timestamp}")
                            
                            # Only write marker if we weren't already in an outage
                            if not in_network_outage:
                                print(f"DEBUG: Writing network outage marker for gap of {gap_duration}ms")
                                # Write "no network event" marker
                                no_network_marker = {
                                    "ts_ms": last_event_timestamp + NETWORK_TIMEOUT_MS,
                                    "event_type": "no_network_event",
                                    "gap_duration_ms": gap_duration,
                                    "message": "Network outage detected - no events received"
                                }
                                outfile.write(json.dumps(no_network_marker) + '\n')
                                no_network_events += 1
                            else:
                                print(f"DEBUG: Skipping marker - already in outage state")
                            # Stay in outage state until we see normal activity
                            in_network_outage = True
                        else:
                            # Normal gap - we're out of outage state
                            if in_network_outage:
                                print(f"DEBUG: Exiting outage state - normal gap")
                            in_network_outage = False
                        
                        # Update last event timestamp for next iteration (exclude book events)
                        if current_timestamp is not None:
                            last_event_timestamp = current_timestamp
                    
                    # Process event and get L1 update if any
                    l1_quote = processor.process_event(event)
                    
                    if l1_quote:
                        # Write L1 update to output
                        outfile.write(json.dumps(l1_quote.to_dict()) + '\n')
                        l1_updates += 1
                        
                        if l1_updates % 1000 == 0:
                            print(f"Processed {events_processed} events, generated {l1_updates} L1 updates, {no_network_events} network outage markers...")
                            
                except json.JSONDecodeError as e:
                    print(f"JSON decode error on line {line_num}: {e}", file=sys.stderr)
                    continue
                except Exception as e:
                    print(f"Error processing line {line_num}: {e}", file=sys.stderr)
                    continue
                    
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    print(f"Preprocessing complete!")
    print(f"Total events processed: {events_processed}")
    print(f"L1 updates generated: {l1_updates}")
    print(f"Network outage markers added: {no_network_events}")
    print(f"Output written to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python preprocess_to_l1.py <input_file> <output_file>")
        print("Example: python preprocess_to_l1.py orderbook_clip.jsonl l1_data.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    preprocess_l2_to_l1(input_file, output_file)