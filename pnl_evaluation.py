"""
PnL Evaluation System with Hit Rate and Slippage Metrics
Simulates trading performance with realistic transaction costs
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import logging

from polymarket_api import OrderBook, Trade
from feature_engineering import MarketSnapshot
from trading_model import TradingModel


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class Position:
    """Represents a trading position"""
    market_id: str
    side: OrderSide
    size: float
    entry_price: float
    entry_time: datetime
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class Trade:
    """Represents a completed trade"""
    market_id: str
    side: OrderSide
    size: float
    price: float
    timestamp: datetime
    commission: float
    slippage: float
    pnl: float = 0.0


@dataclass
class PerformanceMetrics:
    """Trading performance metrics"""
    total_pnl: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    hit_rate: float
    average_win: float
    average_loss: float
    profit_factor: float
    max_drawdown: float
    sharpe_ratio: float
    sortino_ratio: float
    total_commission: float
    total_slippage: float
    average_holding_period: timedelta


class SlippageModel:
    """Models market impact and slippage"""
    
    def __init__(self, model_type: str = "linear", base_cost: float = 0.001):
        """
        Args:
            model_type: 'linear', 'sqrt', or 'fixed'
            base_cost: base slippage cost (fraction of trade value)
        """
        self.model_type = model_type
        self.base_cost = base_cost
    
    def calculate_slippage(self, trade_size: float, orderbook: OrderBook, 
                          side: OrderSide) -> float:
        """Calculate slippage for a trade"""
        if self.model_type == "fixed":
            return self.base_cost
        
        if not orderbook.bids or not orderbook.asks:
            return self.base_cost
        
        # Calculate market impact based on orderbook depth
        if side == OrderSide.BUY:
            levels = sorted(orderbook.asks, key=lambda x: x.price)
        else:
            levels = sorted(orderbook.bids, key=lambda x: x.price, reverse=True)
        
        if not levels:
            return self.base_cost
        
        # Calculate depth within first few levels
        total_depth = sum([level.size for level in levels[:5]])
        impact_ratio = min(trade_size / max(total_depth, 1.0), 1.0)
        
        if self.model_type == "linear":
            slippage = self.base_cost * (1 + impact_ratio)
        elif self.model_type == "sqrt":
            slippage = self.base_cost * (1 + np.sqrt(impact_ratio))
        else:
            slippage = self.base_cost
        
        return min(slippage, 0.1)  # Cap at 10%


class TradingSimulator:
    """Simulates trading with realistic costs and constraints"""
    
    def __init__(self, initial_capital: float = 1000.0, position_size: float = 10.0,
                 max_positions: int = 5, stop_loss: float = -50.0, 
                 take_profit: float = 100.0, min_spread: float = 0.01,
                 commission_rate: float = 0.001, slippage_model: SlippageModel = None):
        """
        Args:
            initial_capital: starting capital
            position_size: default position size per trade
            max_positions: maximum concurrent positions
            stop_loss: stop loss threshold (negative)
            take_profit: take profit threshold (positive)
            min_spread: minimum spread to trade
            commission_rate: commission rate (fraction)
            slippage_model: slippage model instance
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.position_size = position_size
        self.max_positions = max_positions
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.min_spread = min_spread
        self.commission_rate = commission_rate
        self.slippage_model = slippage_model or SlippageModel()
        
        self.logger = logging.getLogger(__name__)
        
        # Trading state
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.equity_curve: List[Tuple[datetime, float]] = []
        self.daily_returns: List[float] = []
        
        # Performance tracking
        self.total_commission = 0.0
        self.total_slippage = 0.0
        
    def can_trade(self, market_id: str, orderbook: OrderBook) -> bool:
        """Check if we can trade this market"""
        if not orderbook.bids or not orderbook.asks:
            return False
        
        # Check minimum spread
        best_bid = max(orderbook.bids, key=lambda x: x.price).price
        best_ask = min(orderbook.asks, key=lambda x: x.price).price
        spread = best_ask - best_bid
        
        if spread < self.min_spread:
            return False
        
        # Check if we have too many positions
        if len(self.positions) >= self.max_positions:
            return False
        
        # Check if we already have a position in this market
        if market_id in self.positions:
            return False
        
        return True
    
    def execute_trade(self, market_id: str, side: OrderSide, size: float,
                     orderbook: OrderBook, timestamp: datetime) -> Optional[Trade]:
        """Execute a trade with slippage and commission"""
        if not self.can_trade(market_id, orderbook) and market_id not in self.positions:
            return None
        
        try:
            # Calculate execution price with slippage
            if side == OrderSide.BUY:
                base_price = min(orderbook.asks, key=lambda x: x.price).price
            else:
                base_price = max(orderbook.bids, key=lambda x: x.price).price
            
            # Calculate slippage
            slippage_rate = self.slippage_model.calculate_slippage(size, orderbook, side)
            slippage_amount = base_price * slippage_rate
            
            if side == OrderSide.BUY:
                execution_price = base_price + slippage_amount
            else:
                execution_price = base_price - slippage_amount
            
            # Calculate commission
            trade_value = size * execution_price
            commission = trade_value * self.commission_rate
            
            # Check if we have enough capital
            if side == OrderSide.BUY and (trade_value + commission) > self.current_capital:
                return None
            
            # Create trade record
            trade = Trade(
                market_id=market_id,
                side=side,
                size=size,
                price=execution_price,
                timestamp=timestamp,
                commission=commission,
                slippage=slippage_amount * size
            )
            
            # Update capital and positions
            if market_id in self.positions:
                # Closing position
                position = self.positions[market_id]
                pnl = self._calculate_position_pnl(position, execution_price)
                trade.pnl = pnl - commission
                
                self.current_capital += trade.pnl
                del self.positions[market_id]
                
            else:
                # Opening position
                self.current_capital -= (trade_value + commission)
                self.positions[market_id] = Position(
                    market_id=market_id,
                    side=side,
                    size=size,
                    entry_price=execution_price,
                    entry_time=timestamp
                )
            
            # Update tracking
            self.total_commission += commission
            self.total_slippage += trade.slippage
            self.trades.append(trade)
            
            # Update equity curve
            total_equity = self._calculate_total_equity(orderbook, timestamp)
            self.equity_curve.append((timestamp, total_equity))
            
            return trade
            
        except Exception as e:
            self.logger.error(f"Error executing trade: {e}")
            return None
    
    def _calculate_position_pnl(self, position: Position, current_price: float) -> float:
        """Calculate PnL for a position"""
        if position.side == OrderSide.BUY:
            return (current_price - position.entry_price) * position.size
        else:
            return (position.entry_price - current_price) * position.size
    
    def _calculate_total_equity(self, orderbook: OrderBook, timestamp: datetime) -> float:
        """Calculate total equity including unrealized PnL"""
        total_equity = self.current_capital
        
        # Add unrealized PnL from open positions
        if orderbook.bids and orderbook.asks:
            mid_price = (max(orderbook.bids, key=lambda x: x.price).price + 
                        min(orderbook.asks, key=lambda x: x.price).price) / 2
            
            for position in self.positions.values():
                unrealized_pnl = self._calculate_position_pnl(position, mid_price)
                total_equity += unrealized_pnl
        
        return total_equity
    
    def update_positions(self, market_data: Dict[str, OrderBook], timestamp: datetime):
        """Update positions and check for stop loss/take profit"""
        positions_to_close = []
        
        for market_id, position in self.positions.items():
            if market_id not in market_data:
                continue
            
            orderbook = market_data[market_id]
            if not orderbook.bids or not orderbook.asks:
                continue
            
            mid_price = (max(orderbook.bids, key=lambda x: x.price).price + 
                        min(orderbook.asks, key=lambda x: x.price).price) / 2
            
            unrealized_pnl = self._calculate_position_pnl(position, mid_price)
            position.unrealized_pnl = unrealized_pnl
            
            # Check stop loss and take profit
            if unrealized_pnl <= self.stop_loss or unrealized_pnl >= self.take_profit:
                positions_to_close.append(market_id)
        
        # Close positions that hit stop loss/take profit
        for market_id in positions_to_close:
            position = self.positions[market_id]
            opposite_side = OrderSide.SELL if position.side == OrderSide.BUY else OrderSide.BUY
            
            self.execute_trade(
                market_id=market_id,
                side=opposite_side,
                size=position.size,
                orderbook=market_data[market_id],
                timestamp=timestamp
            )
    
    def generate_signals(self, predictions: Dict[str, Dict], market_data: Dict[str, OrderBook],
                        timestamp: datetime) -> List[Tuple[str, OrderSide, float]]:
        """Generate trading signals based on model predictions"""
        signals = []
        
        for market_id, prediction in predictions.items():
            if market_id not in market_data:
                continue
            
            orderbook = market_data[market_id]
            confidence = prediction.get('confidence', 0.0)
            prob_up = prediction.get('probability_up', 0.5)
            
            # Simple strategy: trade if confidence is high enough
            if confidence > 0.3:  # Configurable threshold
                if prob_up > 0.6:  # Bullish
                    signals.append((market_id, OrderSide.BUY, self.position_size))
                elif prob_up < 0.4:  # Bearish
                    signals.append((market_id, OrderSide.SELL, self.position_size))
        
        return signals
    
    def run_simulation(self, predictions_df: pd.DataFrame, orderbook_data: Dict[str, pd.DataFrame],
                      start_time: datetime, end_time: datetime) -> PerformanceMetrics:
        """Run complete trading simulation"""
        self.logger.info(f"Running simulation from {start_time} to {end_time}")
        
        # Reset state
        self.current_capital = self.initial_capital
        self.positions = {}
        self.trades = []
        self.equity_curve = [(start_time, self.initial_capital)]
        self.total_commission = 0.0
        self.total_slippage = 0.0
        
        # Simulate trading
        current_time = start_time
        time_step = timedelta(seconds=30)  # 30 second steps
        
        while current_time <= end_time:
            try:
                # Get predictions for current time
                current_predictions = {}
                if current_time in predictions_df.index:
                    pred_row = predictions_df.loc[current_time]
                    for market_id in pred_row.index.levels[1] if hasattr(pred_row.index, 'levels') else [pred_row.name]:
                        if market_id in pred_row:
                            current_predictions[market_id] = pred_row[market_id]
                
                # Get market data for current time
                current_market_data = {}
                for market_id, orderbook_df in orderbook_data.items():
                    if current_time in orderbook_df.index:
                        # Convert DataFrame row back to OrderBook (simplified)
                        # In practice, you'd need to properly reconstruct the OrderBook
                        current_market_data[market_id] = orderbook_df.loc[current_time]
                
                # Update existing positions
                self.update_positions(current_market_data, current_time)
                
                # Generate new signals
                signals = self.generate_signals(current_predictions, current_market_data, current_time)
                
                # Execute trades
                for market_id, side, size in signals:
                    if market_id in current_market_data:
                        self.execute_trade(market_id, side, size, 
                                         current_market_data[market_id], current_time)
                
                current_time += time_step
                
            except Exception as e:
                self.logger.error(f"Error in simulation at {current_time}: {e}")
                current_time += time_step
                continue
        
        # Calculate final metrics
        return self._calculate_performance_metrics()
    
    def _calculate_performance_metrics(self) -> PerformanceMetrics:
        """Calculate comprehensive performance metrics"""
        if not self.trades:
            return PerformanceMetrics(
                total_pnl=0, total_trades=0, winning_trades=0, losing_trades=0,
                hit_rate=0, average_win=0, average_loss=0, profit_factor=0,
                max_drawdown=0, sharpe_ratio=0, sortino_ratio=0,
                total_commission=self.total_commission, total_slippage=self.total_slippage,
                average_holding_period=timedelta(0)
            )
        
        # Basic metrics
        total_pnl = sum([trade.pnl for trade in self.trades])
        total_trades = len(self.trades)
        winning_trades = len([t for t in self.trades if t.pnl > 0])
        losing_trades = len([t for t in self.trades if t.pnl < 0])
        
        hit_rate = winning_trades / max(total_trades, 1)
        
        wins = [t.pnl for t in self.trades if t.pnl > 0]
        losses = [t.pnl for t in self.trades if t.pnl < 0]
        
        average_win = np.mean(wins) if wins else 0
        average_loss = np.mean(losses) if losses else 0
        
        profit_factor = abs(sum(wins) / sum(losses)) if losses else float('inf')
        
        # Calculate returns and drawdown
        if len(self.equity_curve) > 1:
            equity_values = [eq[1] for eq in self.equity_curve]
            returns = np.diff(equity_values) / equity_values[:-1]
            
            # Sharpe ratio (assuming risk-free rate = 0)
            sharpe_ratio = np.mean(returns) / max(np.std(returns), 1e-8) * np.sqrt(252 * 24 * 60 * 2)  # Annualized
            
            # Sortino ratio (downside deviation)
            downside_returns = returns[returns < 0]
            sortino_ratio = (np.mean(returns) / max(np.std(downside_returns), 1e-8) * 
                           np.sqrt(252 * 24 * 60 * 2) if len(downside_returns) > 0 else 0)
            
            # Maximum drawdown
            peak = equity_values[0]
            max_drawdown = 0
            for value in equity_values:
                if value > peak:
                    peak = value
                drawdown = (peak - value) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
        else:
            sharpe_ratio = 0
            sortino_ratio = 0
            max_drawdown = 0
        
        # Average holding period
        holding_periods = []
        position_times = {}
        
        for trade in self.trades:
            if trade.market_id in position_times:
                holding_period = trade.timestamp - position_times[trade.market_id]
                holding_periods.append(holding_period)
                del position_times[trade.market_id]
            else:
                position_times[trade.market_id] = trade.timestamp
        
        average_holding_period = (np.mean([hp.total_seconds() for hp in holding_periods]) 
                                if holding_periods else 0)
        average_holding_period = timedelta(seconds=average_holding_period)
        
        return PerformanceMetrics(
            total_pnl=total_pnl,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            hit_rate=hit_rate,
            average_win=average_win,
            average_loss=average_loss,
            profit_factor=profit_factor,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            total_commission=self.total_commission,
            total_slippage=self.total_slippage,
            average_holding_period=average_holding_period
        )
    
    def get_trade_summary(self) -> pd.DataFrame:
        """Get summary of all trades"""
        if not self.trades:
            return pd.DataFrame()
        
        trade_data = []
        for trade in self.trades:
            trade_data.append({
                'timestamp': trade.timestamp,
                'market_id': trade.market_id,
                'side': trade.side.value,
                'size': trade.size,
                'price': trade.price,
                'commission': trade.commission,
                'slippage': trade.slippage,
                'pnl': trade.pnl
            })
        
        return pd.DataFrame(trade_data)
    
    def get_equity_curve(self) -> pd.DataFrame:
        """Get equity curve as DataFrame"""
        if not self.equity_curve:
            return pd.DataFrame()
        
        return pd.DataFrame(self.equity_curve, columns=['timestamp', 'equity']).set_index('timestamp')


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test slippage model
    slippage_model = SlippageModel("linear", 0.001)
    
    # Test trading simulator
    simulator = TradingSimulator(
        initial_capital=1000.0,
        position_size=10.0,
        slippage_model=slippage_model
    )
    
    print("Trading simulator initialized successfully!")
    print(f"Initial capital: ${simulator.initial_capital}")
    print(f"Position size: ${simulator.position_size}")
    print(f"Commission rate: {simulator.commission_rate:.3%}")
    print(f"Slippage model: {simulator.slippage_model.model_type}")