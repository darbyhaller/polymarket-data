"""
Feature Engineering Pipeline for Polymarket Trading
Generates market microstructure features from orderbook and trade data
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from collections import deque
from dataclasses import dataclass
import logging
from datetime import datetime, timedelta

from polymarket_api import OrderBook, Trade, OrderBookLevel


@dataclass
class MarketSnapshot:
    """Single point-in-time market snapshot with features"""
    timestamp: datetime
    market_id: str
    mid_price: float
    spread: float
    spread_bps: float
    depth_imbalance: float
    signed_volume: float
    order_flow_imbalance: float
    recent_return_1m: float
    recent_return_5m: float
    volatility_1m: float
    volatility_5m: float
    bid_depth: float
    ask_depth: float
    bid_ask_ratio: float
    price_impact: float
    volume_weighted_price: float


class FeatureEngine:
    """Generate trading features from market data"""
    
    def __init__(self, lookback_window: int = 100, depth_levels: int = 5):
        self.lookback_window = lookback_window
        self.depth_levels = depth_levels
        self.logger = logging.getLogger(__name__)
        
        # Data storage
        self.orderbooks: Dict[str, deque] = {}
        self.trades: Dict[str, deque] = {}
        self.snapshots: Dict[str, deque] = {}
        
    def add_orderbook(self, market_id: str, orderbook: OrderBook):
        """Add new orderbook update"""
        if market_id not in self.orderbooks:
            self.orderbooks[market_id] = deque(maxlen=self.lookback_window)
        
        self.orderbooks[market_id].append(orderbook)
        
        # Generate features for this snapshot
        features = self._compute_features(market_id, orderbook)
        if features:
            if market_id not in self.snapshots:
                self.snapshots[market_id] = deque(maxlen=self.lookback_window)
            self.snapshots[market_id].append(features)
    
    def add_trade(self, market_id: str, trade: Trade):
        """Add new trade"""
        if market_id not in self.trades:
            self.trades[market_id] = deque(maxlen=self.lookback_window)
        
        self.trades[market_id].append(trade)
    
    def _compute_features(self, market_id: str, current_orderbook: OrderBook) -> Optional[MarketSnapshot]:
        """Compute all features for current market state"""
        try:
            # Basic price features
            mid_price = self._calculate_mid_price(current_orderbook)
            if mid_price is None:
                return None
            
            spread = self._calculate_spread(current_orderbook)
            spread_bps = (spread / mid_price) * 10000 if mid_price > 0 else 0
            
            # Depth features
            depth_imbalance = self._calculate_depth_imbalance(current_orderbook)
            bid_depth = self._calculate_bid_depth(current_orderbook)
            ask_depth = self._calculate_ask_depth(current_orderbook)
            bid_ask_ratio = bid_depth / max(ask_depth, 1e-8)
            
            # Volume and flow features
            signed_volume = self._calculate_signed_volume(market_id)
            order_flow_imbalance = self._calculate_order_flow_imbalance(market_id)
            volume_weighted_price = self._calculate_volume_weighted_price(market_id)
            
            # Price momentum features
            recent_return_1m = self._calculate_recent_return(market_id, window_seconds=60)
            recent_return_5m = self._calculate_recent_return(market_id, window_seconds=300)
            
            # Volatility features
            volatility_1m = self._calculate_volatility(market_id, window_seconds=60)
            volatility_5m = self._calculate_volatility(market_id, window_seconds=300)
            
            # Market impact
            price_impact = self._calculate_price_impact(current_orderbook)
            
            return MarketSnapshot(
                timestamp=current_orderbook.timestamp,
                market_id=market_id,
                mid_price=mid_price,
                spread=spread,
                spread_bps=spread_bps,
                depth_imbalance=depth_imbalance,
                signed_volume=signed_volume,
                order_flow_imbalance=order_flow_imbalance,
                recent_return_1m=recent_return_1m,
                recent_return_5m=recent_return_5m,
                volatility_1m=volatility_1m,
                volatility_5m=volatility_5m,
                bid_depth=bid_depth,
                ask_depth=ask_depth,
                bid_ask_ratio=bid_ask_ratio,
                price_impact=price_impact,
                volume_weighted_price=volume_weighted_price
            )
        except Exception as e:
            self.logger.error(f"Error computing features for {market_id}: {e}")
            return None
    
    def _calculate_mid_price(self, orderbook: OrderBook) -> Optional[float]:
        """Calculate mid price"""
        if not orderbook.bids or not orderbook.asks:
            return None
        
        best_bid = max(orderbook.bids, key=lambda x: x.price).price
        best_ask = min(orderbook.asks, key=lambda x: x.price).price
        
        return (best_bid + best_ask) / 2
    
    def _calculate_spread(self, orderbook: OrderBook) -> float:
        """Calculate bid-ask spread"""
        if not orderbook.bids or not orderbook.asks:
            return 0.0
        
        best_bid = max(orderbook.bids, key=lambda x: x.price).price
        best_ask = min(orderbook.asks, key=lambda x: x.price).price
        
        return best_ask - best_bid
    
    def _calculate_depth_imbalance(self, orderbook: OrderBook) -> float:
        """Calculate order book depth imbalance"""
        if not orderbook.bids or not orderbook.asks:
            return 0.0
        
        # Sum depth for top N levels
        bid_depth = sum([level.size for level in orderbook.bids[:self.depth_levels]])
        ask_depth = sum([level.size for level in orderbook.asks[:self.depth_levels]])
        
        total_depth = bid_depth + ask_depth
        if total_depth == 0:
            return 0.0
        
        return (bid_depth - ask_depth) / total_depth
    
    def _calculate_bid_depth(self, orderbook: OrderBook) -> float:
        """Calculate total bid depth for top levels"""
        if not orderbook.bids:
            return 0.0
        return sum([level.size for level in orderbook.bids[:self.depth_levels]])
    
    def _calculate_ask_depth(self, orderbook: OrderBook) -> float:
        """Calculate total ask depth for top levels"""
        if not orderbook.asks:
            return 0.0
        return sum([level.size for level in orderbook.asks[:self.depth_levels]])
    
    def _calculate_signed_volume(self, market_id: str, window_seconds: int = 60) -> float:
        """Calculate signed volume (buy volume - sell volume) in recent window"""
        if market_id not in self.trades:
            return 0.0
        
        cutoff_time = datetime.now().timestamp() - window_seconds
        recent_trades = [t for t in self.trades[market_id] 
                        if t.timestamp.timestamp() > cutoff_time]
        
        signed_vol = 0.0
        for trade in recent_trades:
            multiplier = 1.0 if trade.side == 'buy' else -1.0
            signed_vol += trade.size * multiplier
            
        return signed_vol
    
    def _calculate_order_flow_imbalance(self, market_id: str, window_seconds: int = 60) -> float:
        """Calculate order flow imbalance"""
        if market_id not in self.trades:
            return 0.0
        
        cutoff_time = datetime.now().timestamp() - window_seconds
        recent_trades = [t for t in self.trades[market_id] 
                        if t.timestamp.timestamp() > cutoff_time]
        
        buy_volume = sum([t.size for t in recent_trades if t.side == 'buy'])
        sell_volume = sum([t.size for t in recent_trades if t.side == 'sell'])
        total_volume = buy_volume + sell_volume
        
        if total_volume == 0:
            return 0.0
        
        return (buy_volume - sell_volume) / total_volume
    
    def _calculate_volume_weighted_price(self, market_id: str, window_seconds: int = 60) -> float:
        """Calculate volume weighted average price"""
        if market_id not in self.trades:
            return 0.0
        
        cutoff_time = datetime.now().timestamp() - window_seconds
        recent_trades = [t for t in self.trades[market_id] 
                        if t.timestamp.timestamp() > cutoff_time]
        
        if not recent_trades:
            return 0.0
        
        total_value = sum([t.price * t.size for t in recent_trades])
        total_volume = sum([t.size for t in recent_trades])
        
        return total_value / max(total_volume, 1e-8)
    
    def _calculate_recent_return(self, market_id: str, window_seconds: int = 60) -> float:
        """Calculate recent price return"""
        if market_id not in self.snapshots or len(self.snapshots[market_id]) < 2:
            return 0.0
        
        cutoff_time = datetime.now().timestamp() - window_seconds
        recent_snapshots = [s for s in self.snapshots[market_id] 
                           if s.timestamp.timestamp() > cutoff_time]
        
        if len(recent_snapshots) < 2:
            return 0.0
        
        oldest_price = recent_snapshots[0].mid_price
        latest_price = recent_snapshots[-1].mid_price
        
        if oldest_price == 0:
            return 0.0
        
        return (latest_price - oldest_price) / oldest_price
    
    def _calculate_volatility(self, market_id: str, window_seconds: int = 60) -> float:
        """Calculate price volatility (standard deviation of returns)"""
        if market_id not in self.snapshots or len(self.snapshots[market_id]) < 3:
            return 0.0
        
        cutoff_time = datetime.now().timestamp() - window_seconds
        recent_snapshots = [s for s in self.snapshots[market_id] 
                           if s.timestamp.timestamp() > cutoff_time]
        
        if len(recent_snapshots) < 3:
            return 0.0
        
        # Calculate returns
        returns = []
        for i in range(1, len(recent_snapshots)):
            prev_price = recent_snapshots[i-1].mid_price
            curr_price = recent_snapshots[i].mid_price
            if prev_price > 0:
                returns.append((curr_price - prev_price) / prev_price)
        
        if len(returns) < 2:
            return 0.0
        
        return np.std(returns) * np.sqrt(len(returns))  # Annualized
    
    def _calculate_price_impact(self, orderbook: OrderBook, trade_size: float = 10.0) -> float:
        """Calculate estimated price impact for a given trade size"""
        if not orderbook.asks:
            return 0.0
        
        # Calculate impact of buying trade_size
        remaining_size = trade_size
        total_cost = 0.0
        
        for ask_level in sorted(orderbook.asks, key=lambda x: x.price):
            if remaining_size <= 0:
                break
            
            size_to_take = min(remaining_size, ask_level.size)
            total_cost += size_to_take * ask_level.price
            remaining_size -= size_to_take
        
        if trade_size == 0:
            return 0.0
        
        avg_price = total_cost / trade_size
        mid_price = self._calculate_mid_price(orderbook)
        
        if mid_price is None or mid_price == 0:
            return 0.0
        
        return (avg_price - mid_price) / mid_price
    
    def get_features_dataframe(self, market_id: str) -> pd.DataFrame:
        """Get features as DataFrame for training"""
        if market_id not in self.snapshots:
            return pd.DataFrame()
        
        snapshots = list(self.snapshots[market_id])
        
        data = []
        for snapshot in snapshots:
            data.append({
                'timestamp': snapshot.timestamp,
                'market_id': snapshot.market_id,
                'mid_price': snapshot.mid_price,
                'spread': snapshot.spread,
                'spread_bps': snapshot.spread_bps,
                'depth_imbalance': snapshot.depth_imbalance,
                'signed_volume': snapshot.signed_volume,
                'order_flow_imbalance': snapshot.order_flow_imbalance,
                'recent_return_1m': snapshot.recent_return_1m,
                'recent_return_5m': snapshot.recent_return_5m,
                'volatility_1m': snapshot.volatility_1m,
                'volatility_5m': snapshot.volatility_5m,
                'bid_depth': snapshot.bid_depth,
                'ask_depth': snapshot.ask_depth,
                'bid_ask_ratio': snapshot.bid_ask_ratio,
                'price_impact': snapshot.price_impact,
                'volume_weighted_price': snapshot.volume_weighted_price
            })
        
        return pd.DataFrame(data).set_index('timestamp')
    
    def get_latest_features(self, market_id: str) -> Optional[MarketSnapshot]:
        """Get most recent feature snapshot"""
        if market_id not in self.snapshots or not self.snapshots[market_id]:
            return None
        
        return self.snapshots[market_id][-1]


# Example usage
if __name__ == "__main__":
    import time
    from polymarket_api import PolymarketAPI
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize feature engine
    feature_engine = FeatureEngine(lookback_window=50)
    
    # Test with real data
    api = PolymarketAPI()
    markets = api.get_markets()
    
    if markets:
        test_market = markets[0]
        market_id = test_market['id']
        
        print(f"Testing feature engineering with market: {test_market.get('question', 'Unknown')}")
        
        # Simulate some orderbook updates
        for i in range(10):
            orderbook = api.get_orderbook(market_id)
            if orderbook:
                feature_engine.add_orderbook(market_id, orderbook)
                
                # Get recent trades
                trades = api.get_trades(market_id, limit=10)
                for trade in trades:
                    feature_engine.add_trade(market_id, trade)
                
                # Get latest features
                latest_features = feature_engine.get_latest_features(market_id)
                if latest_features:
                    print(f"Features at {latest_features.timestamp}:")
                    print(f"  Mid price: ${latest_features.mid_price:.4f}")
                    print(f"  Spread: {latest_features.spread_bps:.2f} bps")
                    print(f"  Depth imbalance: {latest_features.depth_imbalance:.4f}")
                    print(f"  Order flow imbalance: {latest_features.order_flow_imbalance:.4f}")
                    print()
            
            time.sleep(2)