"""
Backtesting Framework for Polymarket Trading System
Combines data collection, feature engineering, model training, and PnL evaluation
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
import yaml
import json
import os
from pathlib import Path

from polymarket_api import PolymarketAPI, PolymarketWebSocket, OrderBook, Trade
from feature_engineering import FeatureEngine, MarketSnapshot
from trading_model import TradingModel
from pnl_evaluation import TradingSimulator, SlippageModel, PerformanceMetrics


class BacktestConfig:
    """Configuration for backtesting"""
    
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.api_config = self.config['api']
        self.trading_config = self.config['trading']
        self.features_config = self.config['features']
        self.model_config = self.config['model']
        self.evaluation_config = self.config['evaluation']
        self.logging_config = self.config['logging']


class DataCollector:
    """Collects and stores historical market data"""
    
    def __init__(self, api: PolymarketAPI, data_dir: str = "data"):
        self.api = api
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def collect_historical_data(self, market_id: str, hours: int = 24) -> Tuple[List[OrderBook], List[Trade]]:
        """Collect historical orderbook and trade data"""
        self.logger.info(f"Collecting {hours} hours of data for market {market_id}")
        
        orderbooks = []
        all_trades = []
        
        try:
            # Get recent trades (Polymarket API limitation - only recent data available)
            trades = self.api.get_trades(market_id, limit=1000)
            all_trades.extend(trades)
            
            # Get current orderbook snapshots over time
            # In a real implementation, you'd want to collect data over time or use WebSocket
            for i in range(min(hours * 60, 100)):  # Limit to avoid rate limiting
                orderbook = self.api.get_orderbook(market_id)
                if orderbook:
                    orderbooks.append(orderbook)
                    
                if i % 10 == 0:
                    self.logger.info(f"Collected {i+1} orderbook snapshots")
                
                # Small delay to avoid rate limiting
                import time
                time.sleep(1)
            
            self.logger.info(f"Collected {len(orderbooks)} orderbooks and {len(all_trades)} trades")
            
        except Exception as e:
            self.logger.error(f"Error collecting data: {e}")
        
        return orderbooks, all_trades
    
    def save_data(self, market_id: str, orderbooks: List[OrderBook], trades: List[Trade]):
        """Save collected data to disk"""
        market_dir = self.data_dir / market_id
        market_dir.mkdir(exist_ok=True)
        
        # Save orderbooks
        orderbook_data = []
        for ob in orderbooks:
            orderbook_data.append({
                'timestamp': ob.timestamp.isoformat(),
                'bids': [[level.price, level.size] for level in ob.bids],
                'asks': [[level.price, level.size] for level in ob.asks]
            })
        
        with open(market_dir / 'orderbooks.json', 'w') as f:
            json.dump(orderbook_data, f, indent=2)
        
        # Save trades
        trade_data = []
        for trade in trades:
            trade_data.append({
                'timestamp': trade.timestamp.isoformat(),
                'price': trade.price,
                'size': trade.size,
                'side': trade.side,
                'market_id': trade.market_id
            })
        
        with open(market_dir / 'trades.json', 'w') as f:
            json.dump(trade_data, f, indent=2)
        
        self.logger.info(f"Saved data for market {market_id}")
    
    def load_data(self, market_id: str) -> Tuple[List[OrderBook], List[Trade]]:
        """Load previously saved data"""
        market_dir = self.data_dir / market_id
        
        if not market_dir.exists():
            self.logger.warning(f"No saved data found for market {market_id}")
            return [], []
        
        orderbooks = []
        trades = []
        
        try:
            # Load orderbooks
            with open(market_dir / 'orderbooks.json', 'r') as f:
                orderbook_data = json.load(f)
            
            for ob_data in orderbook_data:
                from polymarket_api import OrderBookLevel
                bids = [OrderBookLevel(price=level[0], size=level[1]) for level in ob_data['bids']]
                asks = [OrderBookLevel(price=level[0], size=level[1]) for level in ob_data['asks']]
                
                orderbooks.append(OrderBook(
                    bids=bids,
                    asks=asks,
                    timestamp=datetime.fromisoformat(ob_data['timestamp'])
                ))
            
            # Load trades
            with open(market_dir / 'trades.json', 'r') as f:
                trade_data = json.load(f)
            
            for trade_data_item in trade_data:
                from polymarket_api import Trade
                trades.append(Trade(
                    price=trade_data_item['price'],
                    size=trade_data_item['size'],
                    side=trade_data_item['side'],
                    timestamp=datetime.fromisoformat(trade_data_item['timestamp']),
                    market_id=trade_data_item['market_id']
                ))
            
            self.logger.info(f"Loaded {len(orderbooks)} orderbooks and {len(trades)} trades for market {market_id}")
            
        except Exception as e:
            self.logger.error(f"Error loading data for market {market_id}: {e}")
        
        return orderbooks, trades


class Backtester:
    """Main backtesting engine"""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.api = PolymarketAPI(self.config.api_config['base_url'])
        self.data_collector = DataCollector(self.api)
        self.feature_engine = FeatureEngine(
            lookback_window=self.config.features_config['lookback_window'],
            depth_levels=5
        )
        self.model = TradingModel(
            prediction_horizon=self.config.model_config['prediction_horizon'],
            regularization=self.config.model_config['regularization'],
            max_features=self.config.model_config['max_features']
        )
        self.simulator = TradingSimulator(
            initial_capital=1000.0,
            position_size=self.config.trading_config['position_size'],
            max_positions=5,
            stop_loss=self.config.trading_config['stop_loss'],
            take_profit=self.config.trading_config['take_profit'],
            commission_rate=self.config.evaluation_config['transaction_cost'],
            slippage_model=SlippageModel(
                self.config.evaluation_config['slippage_model'],
                self.config.evaluation_config['transaction_cost']
            )
        )
        
        # Results storage
        self.results = {}
        self.models = {}
    
    def run_single_market_backtest(self, market_id: str, days: int = 7) -> Dict:
        """Run backtest on a single market"""
        self.logger.info(f"Starting backtest for market {market_id} over {days} days")
        
        try:
            # Collect or load data
            orderbooks, trades = self.data_collector.load_data(market_id)
            
            if not orderbooks:
                self.logger.info("No cached data found, collecting fresh data...")
                orderbooks, trades = self.data_collector.collect_historical_data(
                    market_id, hours=days * 24
                )
                self.data_collector.save_data(market_id, orderbooks, trades)
            
            if not orderbooks or len(orderbooks) < 50:
                self.logger.warning(f"Insufficient data for market {market_id}")
                return {"error": "insufficient_data"}
            
            # Process data through feature engine
            self.logger.info("Processing data through feature engine...")
            for orderbook in orderbooks:
                self.feature_engine.add_orderbook(market_id, orderbook)
            
            for trade in trades:
                self.feature_engine.add_trade(market_id, trade)
            
            # Get features DataFrame
            features_df = self.feature_engine.get_features_dataframe(market_id)
            
            if features_df.empty or len(features_df) < 100:
                self.logger.warning(f"Insufficient features for market {market_id}")
                return {"error": "insufficient_features"}
            
            self.logger.info(f"Generated {len(features_df)} feature vectors")
            
            # Walk-forward validation
            return self._walk_forward_backtest(market_id, features_df, orderbooks)
            
        except Exception as e:
            self.logger.error(f"Error in backtest for market {market_id}: {e}")
            return {"error": str(e)}
    
    def _walk_forward_backtest(self, market_id: str, features_df: pd.DataFrame, 
                              orderbooks: List[OrderBook]) -> Dict:
        """Perform walk-forward validation backtest"""
        self.logger.info("Starting walk-forward backtest...")
        
        # Sort by timestamp
        features_df = features_df.sort_index()
        
        # Define training and testing windows
        total_samples = len(features_df)
        train_size = int(total_samples * 0.7)  # 70% for training
        
        if train_size < 50:
            return {"error": "insufficient_data_for_walk_forward"}
        
        all_predictions = []
        model_performance = []
        
        # Walk-forward windows
        window_size = max(50, train_size // 5)  # 5 windows
        
        for i in range(train_size, total_samples, window_size):
            try:
                # Define train and test periods
                train_start = max(0, i - train_size)
                train_end = i
                test_start = i
                test_end = min(total_samples, i + window_size)
                
                if test_start >= test_end:
                    break
                
                self.logger.info(f"Training window: {train_start}:{train_end}, Test window: {test_start}:{test_end}")
                
                # Train model
                train_data = features_df.iloc[train_start:train_end]
                metrics = self.model.train(train_data)
                
                if 'error' in metrics:
                    self.logger.warning(f"Training failed: {metrics['error']}")
                    continue
                
                model_performance.append(metrics)
                
                # Make predictions on test set
                test_data = features_df.iloc[test_start:test_end]
                predictions = self.model.predict_batch(test_data)
                
                if not predictions.empty:
                    predictions['market_id'] = market_id
                    all_predictions.append(predictions)
                
            except Exception as e:
                self.logger.error(f"Error in walk-forward window {i}: {e}")
                continue
        
        if not all_predictions:
            return {"error": "no_predictions_generated"}
        
        # Combine all predictions
        combined_predictions = pd.concat(all_predictions)
        
        # Simulate trading
        self.logger.info("Simulating trading performance...")
        performance = self._simulate_trading(market_id, combined_predictions, features_df, orderbooks)
        
        # Combine results
        results = {
            "market_id": market_id,
            "model_performance": model_performance,
            "trading_performance": performance,
            "predictions_summary": {
                "total_predictions": len(combined_predictions),
                "average_confidence": combined_predictions['confidence'].mean(),
                "prediction_distribution": combined_predictions['prediction'].value_counts().to_dict()
            }
        }
        
        return results
    
    def _simulate_trading(self, market_id: str, predictions_df: pd.DataFrame, 
                         features_df: pd.DataFrame, orderbooks: List[OrderBook]) -> Dict:
        """Simulate trading based on predictions"""
        try:
            # Create simplified market data for simulation
            # In practice, you'd want more sophisticated orderbook reconstruction
            market_data = {}
            
            # Simple simulation using mid prices from features
            for timestamp, row in features_df.iterrows():
                # Create mock orderbook from features
                mid_price = row['mid_price']
                spread = row['spread']
                
                from polymarket_api import OrderBookLevel, OrderBook
                mock_orderbook = OrderBook(
                    bids=[OrderBookLevel(price=mid_price - spread/2, size=100.0)],
                    asks=[OrderBookLevel(price=mid_price + spread/2, size=100.0)],
                    timestamp=timestamp
                )
                market_data[timestamp] = {market_id: mock_orderbook}
            
            # Run simulation
            start_time = features_df.index[0]
            end_time = features_df.index[-1]
            
            # Reset simulator
            self.simulator.current_capital = 1000.0
            self.simulator.positions = {}
            self.simulator.trades = []
            self.simulator.equity_curve = [(start_time, 1000.0)]
            
            # Simple simulation loop
            for timestamp, pred_row in predictions_df.iterrows():
                if timestamp in market_data:
                    current_market_data = market_data[timestamp]
                    
                    # Generate signals based on predictions
                    confidence = pred_row['confidence']
                    prob_up = pred_row['probability_up']
                    
                    # Simple trading strategy
                    if confidence > 0.3:  # High confidence threshold
                        from pnl_evaluation import OrderSide
                        if prob_up > 0.6 and market_id not in self.simulator.positions:
                            # Open long position
                            self.simulator.execute_trade(
                                market_id=market_id,
                                side=OrderSide.BUY,
                                size=self.simulator.position_size,
                                orderbook=current_market_data[market_id],
                                timestamp=timestamp
                            )
                        elif prob_up < 0.4 and market_id not in self.simulator.positions:
                            # Open short position
                            self.simulator.execute_trade(
                                market_id=market_id,
                                side=OrderSide.SELL,
                                size=self.simulator.position_size,
                                orderbook=current_market_data[market_id],
                                timestamp=timestamp
                            )
                    
                    # Update positions
                    self.simulator.update_positions(current_market_data, timestamp)
            
            # Calculate final performance
            performance_metrics = self.simulator._calculate_performance_metrics()
            
            return {
                "total_pnl": performance_metrics.total_pnl,
                "total_trades": performance_metrics.total_trades,
                "hit_rate": performance_metrics.hit_rate,
                "average_win": performance_metrics.average_win,
                "average_loss": performance_metrics.average_loss,
                "profit_factor": performance_metrics.profit_factor,
                "max_drawdown": performance_metrics.max_drawdown,
                "sharpe_ratio": performance_metrics.sharpe_ratio,
                "sortino_ratio": performance_metrics.sortino_ratio,
                "total_commission": performance_metrics.total_commission,
                "total_slippage": performance_metrics.total_slippage,
                "final_equity": self.simulator.current_capital,
                "return_pct": (self.simulator.current_capital / 1000.0 - 1.0) * 100
            }
            
        except Exception as e:
            self.logger.error(f"Error in trading simulation: {e}")
            return {"error": str(e)}
    
    def run_multi_market_backtest(self, market_slugs: List[str], days: int = 7) -> Dict:
        """Run backtest across multiple markets"""
        self.logger.info(f"Starting multi-market backtest for {len(market_slugs)} markets")
        
        results = {}
        
        # Get market IDs from slugs
        markets = self.api.get_markets()
        market_map = {market.get('slug'): market['id'] for market in markets if market.get('slug')}
        
        for slug in market_slugs:
            if slug not in market_map:
                self.logger.warning(f"Market slug not found: {slug}")
                continue
            
            market_id = market_map[slug]
            self.logger.info(f"Running backtest for market: {slug} ({market_id})")
            
            result = self.run_single_market_backtest(market_id, days)
            results[slug] = result
        
        # Aggregate results
        aggregate_results = self._aggregate_multi_market_results(results)
        
        return {
            "individual_results": results,
            "aggregate_results": aggregate_results,
            "summary": {
                "total_markets": len(market_slugs),
                "successful_backtests": len([r for r in results.values() if 'error' not in r]),
                "failed_backtests": len([r for r in results.values() if 'error' in r])
            }
        }
    
    def _aggregate_multi_market_results(self, results: Dict) -> Dict:
        """Aggregate results across multiple markets"""
        successful_results = [r for r in results.values() if 'error' not in r and 'trading_performance' in r]
        
        if not successful_results:
            return {"error": "no_successful_results"}
        
        # Aggregate trading performance
        total_pnl = sum([r['trading_performance']['total_pnl'] for r in successful_results])
        total_trades = sum([r['trading_performance']['total_trades'] for r in successful_results])
        
        # Average metrics
        avg_hit_rate = np.mean([r['trading_performance']['hit_rate'] for r in successful_results])
        avg_sharpe = np.mean([r['trading_performance']['sharpe_ratio'] for r in successful_results])
        max_drawdown = max([r['trading_performance']['max_drawdown'] for r in successful_results])
        
        return {
            "total_pnl": total_pnl,
            "total_trades": total_trades,
            "average_hit_rate": avg_hit_rate,
            "average_sharpe_ratio": avg_sharpe,
            "max_drawdown": max_drawdown,
            "total_markets": len(successful_results),
            "average_return_pct": np.mean([r['trading_performance']['return_pct'] for r in successful_results])
        }
    
    def save_results(self, results: Dict, filename: str = None):
        """Save backtest results to disk"""
        if filename is None:
            filename = f"backtest_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        # Convert datetime objects to strings for JSON serialization
        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {k: convert_datetime(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime(item) for item in obj]
            else:
                return obj
        
        serializable_results = convert_datetime(results)
        
        with open(results_dir / filename, 'w') as f:
            json.dump(serializable_results, f, indent=2)
        
        self.logger.info(f"Results saved to {results_dir / filename}")
    
    def generate_report(self, results: Dict) -> str:
        """Generate a human-readable report from results"""
        report = []
        report.append("=" * 80)
        report.append("POLYMARKET TRADING BACKTEST REPORT")
        report.append("=" * 80)
        report.append("")
        
        if 'individual_results' in results:
            # Multi-market results
            report.append(f"MULTI-MARKET BACKTEST")
            report.append(f"Total Markets: {results['summary']['total_markets']}")
            report.append(f"Successful: {results['summary']['successful_backtests']}")
            report.append(f"Failed: {results['summary']['failed_backtests']}")
            report.append("")
            
            if 'aggregate_results' in results and 'error' not in results['aggregate_results']:
                agg = results['aggregate_results']
                report.append("AGGREGATE PERFORMANCE:")
                report.append(f"  Total PnL: ${agg['total_pnl']:.2f}")
                report.append(f"  Total Trades: {agg['total_trades']}")
                report.append(f"  Average Hit Rate: {agg['average_hit_rate']:.2%}")
                report.append(f"  Average Return: {agg['average_return_pct']:.2%}")
                report.append(f"  Average Sharpe Ratio: {agg['average_sharpe_ratio']:.4f}")
                report.append(f"  Max Drawdown: {agg['max_drawdown']:.2%}")
                report.append("")
        
        else:
            # Single market results
            if 'error' in results:
                report.append(f"BACKTEST FAILED: {results['error']}")
            else:
                report.append(f"SINGLE MARKET BACKTEST")
                report.append(f"Market ID: {results['market_id']}")
                report.append("")
                
                if 'trading_performance' in results:
                    perf = results['trading_performance']
                    report.append("TRADING PERFORMANCE:")
                    report.append(f"  Total PnL: ${perf['total_pnl']:.2f}")
                    report.append(f"  Return: {perf['return_pct']:.2%}")
                    report.append(f"  Total Trades: {perf['total_trades']}")
                    report.append(f"  Hit Rate: {perf['hit_rate']:.2%}")
                    report.append(f"  Average Win: ${perf['average_win']:.2f}")
                    report.append(f"  Average Loss: ${perf['average_loss']:.2f}")
                    report.append(f"  Profit Factor: {perf['profit_factor']:.2f}")
                    report.append(f"  Sharpe Ratio: {perf['sharpe_ratio']:.4f}")
                    report.append(f"  Max Drawdown: {perf['max_drawdown']:.2%}")
                    report.append(f"  Total Commission: ${perf['total_commission']:.2f}")
                    report.append(f"  Total Slippage: ${perf['total_slippage']:.2f}")
                    report.append("")
        
        report.append("=" * 80)
        return "\n".join(report)


# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize backtester
    config = BacktestConfig()
    backtester = Backtester(config)
    
    # Example: Single market backtest
    print("Running single market backtest...")
    
    # Get available markets
    markets = backtester.api.get_markets()
    if markets:
        test_market = markets[0]
        market_id = test_market['id']
        
        print(f"Testing market: {test_market.get('question', 'Unknown')}")
        
        # Run backtest
        results = backtester.run_single_market_backtest(market_id, days=1)  # Short test
        
        # Generate report
        report = backtester.generate_report(results)
        print(report)
        
        # Save results
        backtester.save_results(results)
        
    else:
        print("No markets available for testing")