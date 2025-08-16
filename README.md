# Polymarket Trading System

A comprehensive baseline trading system for Polymarket that collects data from official endpoints, trains ML models, and evaluates performance with realistic PnL metrics.

## Features

- **Real-time Data**: REST API and WebSocket streaming from official Polymarket endpoints
- **Feature Engineering**: Market microstructure features (spread, depth imbalance, order flow, volatility)
- **Baseline Model**: Calibrated logistic regression with walk-forward validation
- **PnL Evaluation**: Realistic transaction costs, slippage modeling, and comprehensive metrics
- **Backtesting Framework**: Walk-forward validation with multiple markets
- **Live Trading**: Real-time prediction and signal generation (demo mode)

## Quick Start

### Installation

```bash
# Clone or download the project files
# Install dependencies
pip install -r requirements.txt
```

### Usage Examples

```bash
# List available markets
python main.py list --limit 10

# Collect data for a market
python main.py collect trump-president-2024 --hours 48

# Train a model
python main.py train trump-president-2024 --days 7

# Run backtest on single market
python main.py backtest --single trump-president-2024 --days 3

# Run backtest on multiple markets
python main.py backtest --markets trump-president-2024 biden-president-2024 --days 7

# Live trading (demo mode - no real orders)
python main.py live trump-president-2024 --model models/model_trump-president-2024_20240816_120000.pkl
```

## System Architecture

### Core Components

1. **`polymarket_api.py`** - REST API and WebSocket client
2. **`feature_engineering.py`** - Market microstructure feature extraction
3. **`trading_model.py`** - Baseline logistic regression model
4. **`pnl_evaluation.py`** - Trading simulation with realistic costs
5. **`backtesting.py`** - Walk-forward validation framework
6. **`main.py`** - CLI interface and orchestration

### Data Flow

```
Polymarket API → Feature Engineering → Model Prediction → Trading Signals → PnL Evaluation
     ↓                    ↓                    ↓              ↓               ↓
Order Books         Spread, Depth        Probability      Buy/Sell        Hit Rate
   Trades           Order Flow           Confidence       Position        Sharpe
                   Volatility                            Management       Drawdown
```

## Features Generated

The system generates 14+ microstructure features:

- **Price Features**: Mid price, spread, spread basis points
- **Depth Features**: Bid/ask depth, depth imbalance, bid/ask ratio
- **Flow Features**: Signed volume, order flow imbalance, VWAP
- **Momentum Features**: Recent returns (1min, 5min)
- **Volatility Features**: Rolling volatility (1min, 5min)
- **Impact Features**: Price impact estimation

## Model Details

### Baseline Model
- **Algorithm**: Logistic Regression with L2 regularization
- **Target**: Binary classification (price up/down in next 30 seconds)
- **Features**: Top 10 most predictive microstructure features
- **Calibration**: Platt scaling for probability calibration
- **Validation**: Walk-forward with 70/30 train/test split

### Performance Metrics
- **Trading**: Total PnL, hit rate, profit factor, Sharpe ratio
- **Risk**: Maximum drawdown, Sortino ratio
- **Costs**: Transaction costs, slippage, commission tracking
- **Timing**: Average holding period, trade frequency

## Configuration

Edit [`config.yaml`](config.yaml) to customize:

```yaml
trading:
  position_size: 10.0        # USDC per trade
  max_inventory: 100.0       # Maximum exposure
  stop_loss: -50.0          # Stop loss threshold
  take_profit: 100.0        # Take profit threshold

model:
  prediction_horizon: 30     # Seconds ahead to predict
  regularization: 0.01      # L2 penalty
  max_features: 10          # Feature limit

evaluation:
  transaction_cost: 0.001   # 0.1% per trade
  slippage_model: "linear"  # linear, sqrt, or fixed
```

## Results Interpretation

### Sample Backtest Output

```
POLYMARKET TRADING BACKTEST REPORT
================================================================================

SINGLE MARKET BACKTEST
Market ID: 0x1234...

TRADING PERFORMANCE:
  Total PnL: $45.30
  Return: 4.53%
  Total Trades: 23
  Hit Rate: 56.52%
  Average Win: $4.20
  Average Loss: $-3.10
  Profit Factor: 1.43
  Sharpe Ratio: 0.8245
  Max Drawdown: 3.20%
  Total Commission: $2.30
  Total Slippage: $1.50
```

### Key Metrics Explained

- **Hit Rate**: Percentage of profitable trades (>50% is good)
- **Profit Factor**: Total wins / Total losses (>1.0 is profitable)
- **Sharpe Ratio**: Risk-adjusted returns (>0.5 is decent, >1.0 is good)
- **Max Drawdown**: Largest peak-to-trough decline (lower is better)

## Important Notes

⚠️ **This is for research and educational purposes only**

- **Paper Trading**: The live mode generates signals but doesn't execute real trades
- **Historical Limitations**: Polymarket API provides limited historical data
- **Market Impact**: Real trading may have higher slippage than modeled
- **Regime Changes**: Models may need retraining as market conditions evolve

## Common Use Cases

### Research & Analysis
```bash
# Analyze multiple markets
python main.py backtest --markets market1 market2 market3 --days 7

# Feature importance analysis
python main.py train your-market --days 14
```

### Strategy Development
```bash
# Test on recent data
python main.py collect your-market --hours 72
python main.py backtest --single your-market --days 3
```

### Model Validation
```bash
# Walk-forward validation
python main.py backtest --single your-market --days 14
```

## Extending the System

### Adding New Features
1. Modify [`feature_engineering.py`](feature_engineering.py)
2. Add feature calculation in `_compute_features()`
3. Update `MarketSnapshot` dataclass
4. Retrain models

### Alternative Models
1. Replace logistic regression in [`trading_model.py`](trading_model.py)
2. Options: Random Forest, XGBoost, Neural Networks
3. Keep the same interface (`train()`, `predict()` methods)

### Custom Strategies
1. Modify signal generation in [`main.py`](main.py)
2. Update trading logic in `_start_live_trading()`
3. Add new parameters to [`config.yaml`](config.yaml)

## Troubleshooting

### Common Issues

**Rate Limiting**: Add delays between API calls
```python
import time
time.sleep(1)  # 1 second delay
```

**Insufficient Data**: Increase collection period
```bash
python main.py collect your-market --hours 96  # 4 days
```

**Model Training Fails**: Check data quality
- Ensure sufficient price movement
- Verify feature calculations
- Check for data gaps

**Poor Performance**: 
- Try different regularization values
- Increase training data
- Check feature importance
- Validate market liquidity

## File Structure

```
polymarket-trading/
├── requirements.txt          # Dependencies
├── config.yaml              # Configuration
├── main.py                  # CLI interface
├── polymarket_api.py        # API client
├── feature_engineering.py   # Feature extraction
├── trading_model.py         # ML model
├── pnl_evaluation.py        # Performance evaluation
├── backtesting.py           # Backtesting framework
├── data/                    # Cached market data
├── models/                  # Saved models
├── results/                 # Backtest results
└── README.md               # This file
```

## License

This project is for educational and research purposes. Please ensure compliance with Polymarket's Terms of Service and applicable regulations in your jurisdiction.