"""
Main execution script for Polymarket Trading System
Provides CLI interface for backtesting, live trading, and data collection
"""

import argparse
import logging
import sys
import yaml
import json
from pathlib import Path
from datetime import datetime, timedelta
import asyncio
import signal
import time

from polymarket_api import PolymarketAPI, PolymarketWebSocket
from feature_engineering import FeatureEngine
from trading_model import TradingModel
from backtesting import Backtester, BacktestConfig, DataCollector
from pnl_evaluation import TradingSimulator, SlippageModel


def setup_logging(config_path: str = "config.yaml"):
    """Setup logging configuration"""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        logging_config = config.get('logging', {})
        level = getattr(logging, logging_config.get('level', 'INFO'))
        log_format = logging_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Configure root logger
        logging.basicConfig(
            level=level,
            format=log_format,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(logging_config.get('file', 'polymarket_bot.log'))
            ]
        )
        
        # Reduce noise from external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('websocket').setLevel(logging.WARNING)
        
    except Exception as e:
        # Fallback logging configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        logging.warning(f"Could not load logging config: {e}")


class PolymarketBot:
    """Main bot orchestrator"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = BacktestConfig(config_path)
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.api = PolymarketAPI(self.config.api_config['base_url'])
        self.feature_engine = FeatureEngine(
            lookback_window=self.config.features_config['lookback_window']
        )
        self.model = TradingModel(
            prediction_horizon=self.config.model_config['prediction_horizon'],
            regularization=self.config.model_config['regularization'],
            max_features=self.config.model_config['max_features']
        )
        
        # Live trading components
        self.ws = None
        self.running = False
        self.positions = {}
        
    def run_backtest(self, market_slugs: list = None, days: int = 7, single_market: str = None):
        """Run backtesting mode"""
        self.logger.info("Starting backtest mode...")
        
        backtester = Backtester(self.config)
        
        if single_market:
            # Single market backtest
            self.logger.info(f"Running single market backtest: {single_market}")
            
            # Get market ID from slug
            markets = self.api.get_markets()
            market_map = {market.get('slug'): market['id'] for market in markets if market.get('slug')}
            
            if single_market not in market_map:
                self.logger.error(f"Market slug not found: {single_market}")
                return
            
            market_id = market_map[single_market]
            results = backtester.run_single_market_backtest(market_id, days)
            
        else:
            # Multi-market backtest
            if not market_slugs:
                # Get top N active markets if none specified
                markets = self.api.get_markets()
                market_slugs = [market.get('slug') for market in markets[:5] if market.get('slug')]
            
            self.logger.info(f"Running multi-market backtest: {market_slugs}")
            results = backtester.run_multi_market_backtest(market_slugs, days)
        
        # Generate and display report
        report = backtester.generate_report(results)
        print("\n" + report)
        
        # Save results
        backtester.save_results(results)
        
        return results
    
    def collect_data(self, market_slug: str, hours: int = 24):
        """Data collection mode"""
        self.logger.info(f"Starting data collection for {market_slug} ({hours} hours)")
        
        # Get market ID
        markets = self.api.get_markets()
        market_map = {market.get('slug'): market['id'] for market in markets if market.get('slug')}
        
        if market_slug not in market_map:
            self.logger.error(f"Market slug not found: {market_slug}")
            return
        
        market_id = market_map[market_slug]
        
        # Collect data
        collector = DataCollector(self.api)
        orderbooks, trades = collector.collect_historical_data(market_id, hours)
        
        # Save data
        collector.save_data(market_id, orderbooks, trades)
        
        self.logger.info(f"Data collection complete: {len(orderbooks)} orderbooks, {len(trades)} trades")
    
    def train_model(self, market_slug: str, days: int = 7, save_model: bool = True):
        """Model training mode"""
        self.logger.info(f"Training model for {market_slug}")
        
        # Get market ID
        markets = self.api.get_markets()
        market_map = {market.get('slug'): market['id'] for market in markets if market.get('slug')}
        
        if market_slug not in market_map:
            self.logger.error(f"Market slug not found: {market_slug}")
            return
        
        market_id = market_map[market_slug]
        
        # Load or collect data
        collector = DataCollector(self.api)
        orderbooks, trades = collector.load_data(market_id)
        
        if not orderbooks:
            self.logger.info("No cached data found, collecting fresh data...")
            orderbooks, trades = collector.collect_historical_data(market_id, hours=days * 24)
            collector.save_data(market_id, orderbooks, trades)
        
        if len(orderbooks) < 50:
            self.logger.error("Insufficient data for training")
            return
        
        # Process through feature engine
        for orderbook in orderbooks:
            self.feature_engine.add_orderbook(market_id, orderbook)
        
        for trade in trades:
            self.feature_engine.add_trade(market_id, trade)
        
        # Get features
        features_df = self.feature_engine.get_features_dataframe(market_id)
        
        if features_df.empty or len(features_df) < 100:
            self.logger.error("Insufficient features for training")
            return
        
        # Train model
        metrics = self.model.train(features_df)
        
        print("\nTraining Results:")
        print(f"  Training Accuracy: {metrics.get('train_accuracy', 0):.4f}")
        print(f"  Validation Accuracy: {metrics.get('val_accuracy', 0):.4f}")
        print(f"  Training AUC: {metrics.get('train_auc', 0):.4f}")
        print(f"  Validation AUC: {metrics.get('val_auc', 0):.4f}")
        print(f"  Number of Features: {metrics.get('n_features', 0)}")
        print(f"  Number of Samples: {metrics.get('n_samples', 0)}")
        
        # Feature importance
        importance = self.model.get_feature_importance()
        print(f"\nTop 5 Most Important Features:")
        for feature, score in list(importance.items())[:5]:
            print(f"  {feature}: {score:.4f}")
        
        # Save model if requested
        if save_model:
            model_dir = Path("models")
            model_dir.mkdir(exist_ok=True)
            model_path = model_dir / f"model_{market_slug}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
            self.model.save_model(str(model_path))
            self.logger.info(f"Model saved to {model_path}")
    
    def run_live_trading(self, market_slug: str, model_path: str = None):
        """Live trading mode with WebSocket streaming"""
        self.logger.info(f"Starting live trading for {market_slug}")
        
        # Get market ID
        markets = self.api.get_markets()
        market_map = {market.get('slug'): market['id'] for market in markets if market.get('slug')}
        
        if market_slug not in market_map:
            self.logger.error(f"Market slug not found: {market_slug}")
            return
        
        market_id = market_map[market_slug]
        
        # Load model if specified
        if model_path:
            self.logger.info(f"Loading model from {model_path}")
            self.model.load_model(model_path)
        
        if not self.model.is_trained:
            self.logger.error("No trained model available. Please train a model first.")
            return
        
        # Initialize WebSocket
        self.ws = PolymarketWebSocket(self.config.api_config['websocket_url'])
        
        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            self.logger.info("Received shutdown signal")
            self.stop_live_trading()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start live trading
        self._start_live_trading(market_id, market_slug)
    
    def _start_live_trading(self, market_id: str, market_slug: str):
        """Internal method to start live trading"""
        self.running = True
        
        # Initialize trading simulator for position tracking
        simulator = TradingSimulator(
            initial_capital=1000.0,
            position_size=self.config.trading_config['position_size'],
            stop_loss=self.config.trading_config['stop_loss'],
            take_profit=self.config.trading_config['take_profit']
        )
        
        def on_orderbook_update(orderbook):
            """Handle orderbook updates"""
            if not self.running:
                return
            
            try:
                # Add to feature engine
                self.feature_engine.add_orderbook(market_id, orderbook)
                
                # Get latest features
                latest_features = self.feature_engine.get_latest_features(market_id)
                
                if latest_features:
                    # Make prediction
                    prediction = self.model.predict(latest_features)
                    
                    if prediction:
                        confidence = prediction['confidence']
                        prob_up = prediction['probability_up']
                        
                        self.logger.info(f"Prediction - Prob Up: {prob_up:.4f}, Confidence: {confidence:.4f}")
                        
                        # Simple trading logic (demo only - NOT for real money!)
                        if confidence > 0.4:  # High confidence threshold
                            if prob_up > 0.7 and market_id not in self.positions:
                                self.logger.info(f"SIGNAL: BUY {market_slug} (confidence: {confidence:.4f})")
                                # In real implementation, you would execute trade here
                                
                            elif prob_up < 0.3 and market_id not in self.positions:
                                self.logger.info(f"SIGNAL: SELL {market_slug} (confidence: {confidence:.4f})")
                                # In real implementation, you would execute trade here
                
            except Exception as e:
                self.logger.error(f"Error processing orderbook update: {e}")
        
        def on_trade(trade):
            """Handle trade updates"""
            if not self.running:
                return
            
            try:
                self.feature_engine.add_trade(market_id, trade)
                self.logger.debug(f"Trade: {trade.side} {trade.size} @ ${trade.price:.4f}")
                
            except Exception as e:
                self.logger.error(f"Error processing trade: {e}")
        
        try:
            # Connect WebSocket
            self.ws.connect()
            
            # Subscribe to market
            self.ws.subscribe_to_market(market_id, on_orderbook_update, on_trade)
            
            self.logger.info("Live trading started. Press Ctrl+C to stop.")
            
            # Keep running until stopped
            while self.running:
                time.sleep(1)
                
        except Exception as e:
            self.logger.error(f"Error in live trading: {e}")
        finally:
            self.stop_live_trading()
    
    def stop_live_trading(self):
        """Stop live trading"""
        self.running = False
        if self.ws:
            self.ws.disconnect()
        self.logger.info("Live trading stopped")
    
    def list_markets(self, limit: int = 10):
        """List available markets"""
        self.logger.info("Fetching available markets...")
        
        markets = self.api.get_markets()
        
        if not markets:
            print("No markets found")
            return
        
        print(f"\nTop {min(limit, len(markets))} Markets:")
        print("-" * 80)
        
        for i, market in enumerate(markets[:limit]):
            slug = market.get('slug', 'N/A')
            question = market.get('question', 'N/A')
            volume = market.get('volume', 0)
            
            print(f"{i+1:2d}. {slug}")
            print(f"    Question: {question}")
            print(f"    Volume: ${volume:,.2f}")
            print()


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Polymarket Trading Bot")
    parser.add_argument('--config', '-c', default='config.yaml', help='Config file path')
    
    subparsers = parser.add_subparsers(dest='mode', help='Execution mode')
    
    # Backtest mode
    backtest_parser = subparsers.add_parser('backtest', help='Run backtesting')
    backtest_parser.add_argument('--markets', '-m', nargs='+', help='Market slugs to test')
    backtest_parser.add_argument('--single', '-s', help='Single market slug to test')
    backtest_parser.add_argument('--days', '-d', type=int, default=7, help='Days of data to use')
    
    # Data collection mode
    data_parser = subparsers.add_parser('collect', help='Collect market data')
    data_parser.add_argument('market', help='Market slug')
    data_parser.add_argument('--hours', type=int, default=24, help='Hours of data to collect')
    
    # Model training mode
    train_parser = subparsers.add_parser('train', help='Train model')
    train_parser.add_argument('market', help='Market slug')
    train_parser.add_argument('--days', '-d', type=int, default=7, help='Days of training data')
    train_parser.add_argument('--no-save', action='store_true', help='Do not save trained model')
    
    # Live trading mode
    live_parser = subparsers.add_parser('live', help='Live trading')
    live_parser.add_argument('market', help='Market slug')
    live_parser.add_argument('--model', '-m', help='Path to trained model file')
    
    # List markets mode
    list_parser = subparsers.add_parser('list', help='List available markets')
    list_parser.add_argument('--limit', '-l', type=int, default=10, help='Number of markets to show')
    
    args = parser.parse_args()
    
    if args.mode is None:
        parser.print_help()
        return
    
    # Setup logging
    setup_logging(args.config)
    
    # Initialize bot
    bot = PolymarketBot(args.config)
    
    try:
        if args.mode == 'backtest':
            bot.run_backtest(
                market_slugs=args.markets,
                single_market=args.single,
                days=args.days
            )
        
        elif args.mode == 'collect':
            bot.collect_data(args.market, args.hours)
        
        elif args.mode == 'train':
            bot.train_model(args.market, args.days, not args.no_save)
        
        elif args.mode == 'live':
            bot.run_live_trading(args.market, args.model)
        
        elif args.mode == 'list':
            bot.list_markets(args.limit)
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()