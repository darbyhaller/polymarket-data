"""
Polymarket API Client for REST endpoints and WebSocket streaming
"""

import json
import time
import logging
import requests
import websocket
import threading
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import pandas as pd


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class OrderBook:
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    timestamp: datetime


@dataclass
class Trade:
    price: float
    size: float
    side: str  # 'buy' or 'sell'
    timestamp: datetime
    market_id: str


class PolymarketAPI:
    """Client for Polymarket CLOB API"""
    
    def __init__(self, base_url: str = "https://clob.polymarket.com"):
        self.base_url = base_url
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        
    def get_markets(self) -> List[Dict]:
        """Get all available markets"""
        try:
            response = self.session.get(f"{self.base_url}/markets")
            response.raise_for_status()
            data = response.json()
            # API returns {'data': [...], 'next_cursor': ..., etc}
            if isinstance(data, dict) and 'data' in data:
                return data['data']
            return data if isinstance(data, list) else []
        except Exception as e:
            self.logger.error(f"Error fetching markets: {e}")
            return []
    
    def get_market_by_slug(self, slug: str) -> Optional[Dict]:
        """Get market by slug (e.g., 'trump-president-2024')"""
        markets = self.get_markets()
        for market in markets:
            if market.get('slug') == slug:
                return market
        return None
    
    def get_orderbook(self, market_id: str) -> Optional[OrderBook]:
        """Get current orderbook for a market"""
        try:
            response = self.session.get(f"{self.base_url}/book/{market_id}")
            response.raise_for_status()
            data = response.json()
            
            bids = [OrderBookLevel(float(level['price']), float(level['size'])) 
                   for level in data.get('bids', [])]
            asks = [OrderBookLevel(float(level['price']), float(level['size'])) 
                   for level in data.get('asks', [])]
            
            return OrderBook(
                bids=bids,
                asks=asks,
                timestamp=datetime.now(timezone.utc)
            )
        except Exception as e:
            self.logger.error(f"Error fetching orderbook for {market_id}: {e}")
            return None
    
    def get_trades(self, market_id: str, limit: int = 100) -> List[Trade]:
        """Get recent trades for a market"""
        try:
            response = self.session.get(
                f"{self.base_url}/trades/{market_id}",
                params={'limit': limit}
            )
            response.raise_for_status()
            data = response.json()
            
            trades = []
            for trade in data:
                trades.append(Trade(
                    price=float(trade['price']),
                    size=float(trade['size']),
                    side=trade['side'],
                    timestamp=datetime.fromtimestamp(
                        float(trade['timestamp']) / 1000, 
                        tz=timezone.utc
                    ),
                    market_id=market_id
                ))
            return trades
        except Exception as e:
            self.logger.error(f"Error fetching trades for {market_id}: {e}")
            return []


class PolymarketWebSocket:
    """WebSocket client for real-time market data"""
    
    def __init__(self, url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"):
        self.url = url
        self.ws = None
        self.callbacks = {}
        self.logger = logging.getLogger(__name__)
        self.running = False
        
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            msg_type = data.get('type')
            market_id = data.get('market_id')
            
            if msg_type == 'book_update' and market_id:
                if market_id in self.callbacks:
                    # Convert to OrderBook format
                    bids = [OrderBookLevel(float(level['price']), float(level['size'])) 
                           for level in data.get('bids', [])]
                    asks = [OrderBookLevel(float(level['price']), float(level['size'])) 
                           for level in data.get('asks', [])]
                    
                    orderbook = OrderBook(
                        bids=bids,
                        asks=asks,
                        timestamp=datetime.now(timezone.utc)
                    )
                    self.callbacks[market_id](orderbook)
                    
            elif msg_type == 'trade' and market_id:
                if market_id in self.callbacks:
                    trade = Trade(
                        price=float(data['price']),
                        size=float(data['size']),
                        side=data['side'],
                        timestamp=datetime.fromtimestamp(
                            float(data['timestamp']) / 1000,
                            tz=timezone.utc
                        ),
                        market_id=market_id
                    )
                    # Call trade callback if exists
                    trade_callback = self.callbacks.get(f"{market_id}_trades")
                    if trade_callback:
                        trade_callback(trade)
                        
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")
    
    def on_error(self, ws, error):
        self.logger.error(f"WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        self.logger.info("WebSocket connection closed")
        self.running = False
    
    def on_open(self, ws):
        self.logger.info("WebSocket connection opened")
        self.running = True
    
    def subscribe_to_market(self, market_id: str, orderbook_callback: Callable[[OrderBook], None], 
                          trade_callback: Optional[Callable[[Trade], None]] = None):
        """Subscribe to orderbook updates for a market"""
        self.callbacks[market_id] = orderbook_callback
        if trade_callback:
            self.callbacks[f"{market_id}_trades"] = trade_callback
            
        if self.ws and self.running:
            subscribe_msg = {
                "type": "subscribe",
                "market_id": market_id,
                "channels": ["book", "trades"]
            }
            self.ws.send(json.dumps(subscribe_msg))
    
    def connect(self):
        """Connect to WebSocket"""
        websocket.enableTrace(False)  # Set to True for debugging
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        # Run in separate thread
        def run_ws():
            self.ws.run_forever()
            
        ws_thread = threading.Thread(target=run_ws)
        ws_thread.daemon = True
        ws_thread.start()
        
        # Wait for connection
        timeout = 10
        start_time = time.time()
        while not self.running and time.time() - start_time < timeout:
            time.sleep(0.1)
            
        if not self.running:
            raise ConnectionError("Failed to connect to WebSocket")
    
    def disconnect(self):
        """Disconnect from WebSocket"""
        if self.ws:
            self.ws.close()
        self.running = False


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Initialize API client
    api = PolymarketAPI()
    
    # Get markets
    print("Fetching markets...")
    markets = api.get_markets()
    print(f"Found {len(markets)} markets")
    
    if markets:
        # Use first market for testing
        test_market = markets[0]
        market_id = test_market['id']
        
        print(f"\nTesting with market: {test_market.get('question', 'Unknown')}")
        
        # Get orderbook
        orderbook = api.get_orderbook(market_id)
        if orderbook:
            print(f"Best bid: ${orderbook.bids[0].price:.4f} (size: {orderbook.bids[0].size})")
            print(f"Best ask: ${orderbook.asks[0].price:.4f} (size: {orderbook.asks[0].size})")
        
        # Get recent trades
        trades = api.get_trades(market_id, limit=5)
        print(f"\nRecent {len(trades)} trades:")
        for trade in trades:
            print(f"  {trade.side} {trade.size} @ ${trade.price:.4f} at {trade.timestamp}")