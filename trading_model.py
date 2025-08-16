"""
Baseline Trading Model using Logistic Regression
Predicts up-tick probability in next 30 seconds
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score
import logging
import pickle
import time
from datetime import datetime, timedelta

from feature_engineering import MarketSnapshot, FeatureEngine


class TradingModel:
    """Baseline logistic regression model for price prediction"""
    
    def __init__(self, prediction_horizon: int = 30, regularization: float = 0.01, 
                 calibrate: bool = True, max_features: int = 10):
        """
        Args:
            prediction_horizon: seconds to predict ahead
            regularization: L2 regularization parameter
            calibrate: whether to calibrate probabilities
            max_features: maximum number of features to use
        """
        self.prediction_horizon = prediction_horizon
        self.regularization = regularization
        self.calibrate = calibrate
        self.max_features = max_features
        self.logger = logging.getLogger(__name__)
        
        # Model components
        self.scaler = StandardScaler()
        self.model = LogisticRegression(
            C=1.0/regularization,
            random_state=42,
            solver='liblinear',
            max_iter=1000
        )
        self.calibrated_model = None
        self.feature_names = []
        self.is_trained = False
        
        # Training data buffer for online learning
        self.training_buffer = []
        self.buffer_size = 1000
        
    def _create_labels(self, df: pd.DataFrame) -> pd.Series:
        """Create binary labels: 1 if price goes up in next N seconds, 0 otherwise"""
        if df.empty:
            return pd.Series(dtype=int)
        
        # Shift mid_price by prediction_horizon to get future price
        df_sorted = df.sort_index()
        future_price = df_sorted['mid_price'].shift(-1)  # Next observation
        current_price = df_sorted['mid_price']
        
        # Binary label: 1 if price goes up, 0 if down
        labels = (future_price > current_price).astype(int)
        
        # Remove last observation (no future price available)
        return labels[:-1]
    
    def _prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare and select features for training"""
        if df.empty:
            return df
        
        # Select feature columns (exclude timestamp, market_id, mid_price)
        feature_cols = [col for col in df.columns 
                       if col not in ['timestamp', 'market_id', 'mid_price']]
        
        features_df = df[feature_cols].copy()
        
        # Handle missing values
        features_df = features_df.fillna(0.0)
        
        # Handle infinite values
        features_df = features_df.replace([np.inf, -np.inf], 0.0)
        
        # Limit number of features if specified
        if self.max_features and len(feature_cols) > self.max_features:
            # Use correlation with future returns to select features
            # For now, just take first max_features
            features_df = features_df.iloc[:, :self.max_features]
        
        return features_df
    
    def train(self, df: pd.DataFrame) -> Dict[str, float]:
        """Train the model on historical data"""
        try:
            if df.empty or len(df) < 10:
                self.logger.warning("Insufficient data for training")
                return {"error": "insufficient_data"}
            
            # Sort by timestamp
            df_sorted = df.sort_index()
            
            # Create labels
            labels = self._create_labels(df_sorted)
            if labels.empty or len(labels) < 5:
                self.logger.warning("Insufficient labels for training")
                return {"error": "insufficient_labels"}
            
            # Prepare features (excluding last row since it has no label)
            features_df = self._prepare_features(df_sorted.iloc[:-1])
            
            if features_df.empty:
                self.logger.warning("No features available for training")
                return {"error": "no_features"}
            
            # Ensure we have matching samples
            min_len = min(len(features_df), len(labels))
            X = features_df.iloc[:min_len]
            y = labels.iloc[:min_len]
            
            # Check for class imbalance
            if y.sum() == 0 or y.sum() == len(y):
                self.logger.warning("All labels are the same class")
                return {"error": "no_class_variation"}
            
            # Store feature names
            self.feature_names = X.columns.tolist()
            
            # Split into train/validation
            X_train, X_val, y_train, y_val = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_val_scaled = self.scaler.transform(X_val)
            
            # Train model
            self.model.fit(X_train_scaled, y_train)
            
            # Calibrate if requested
            if self.calibrate:
                self.calibrated_model = CalibratedClassifierCV(
                    self.model, method='platt', cv=3
                )
                # Fit calibrated model on validation set
                self.calibrated_model.fit(X_val_scaled, y_val)
            
            self.is_trained = True
            
            # Compute metrics
            active_model = self.calibrated_model if self.calibrate else self.model
            train_pred = active_model.predict(X_train_scaled)
            train_proba = active_model.predict_proba(X_train_scaled)[:, 1]
            val_pred = active_model.predict(X_val_scaled)
            val_proba = active_model.predict_proba(X_val_scaled)[:, 1]
            
            metrics = {
                "train_accuracy": accuracy_score(y_train, train_pred),
                "train_precision": precision_score(y_train, train_pred, zero_division=0),
                "train_recall": recall_score(y_train, train_pred, zero_division=0),
                "train_auc": roc_auc_score(y_train, train_proba),
                "val_accuracy": accuracy_score(y_val, val_pred),
                "val_precision": precision_score(y_val, val_pred, zero_division=0),
                "val_recall": recall_score(y_val, val_pred, zero_division=0),
                "val_auc": roc_auc_score(y_val, val_proba),
                "n_samples": len(X),
                "n_features": len(self.feature_names),
                "class_balance": y.mean()
            }
            
            self.logger.info(f"Model trained successfully: {metrics}")
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error training model: {e}")
            return {"error": str(e)}
    
    def predict(self, features: MarketSnapshot) -> Optional[Dict[str, float]]:
        """Make prediction on single market snapshot"""
        if not self.is_trained:
            return None
        
        try:
            # Convert snapshot to feature vector
            feature_dict = {
                'spread': features.spread,
                'spread_bps': features.spread_bps,
                'depth_imbalance': features.depth_imbalance,
                'signed_volume': features.signed_volume,
                'order_flow_imbalance': features.order_flow_imbalance,
                'recent_return_1m': features.recent_return_1m,
                'recent_return_5m': features.recent_return_5m,
                'volatility_1m': features.volatility_1m,
                'volatility_5m': features.volatility_5m,
                'bid_depth': features.bid_depth,
                'ask_depth': features.ask_depth,
                'bid_ask_ratio': features.bid_ask_ratio,
                'price_impact': features.price_impact,
                'volume_weighted_price': features.volume_weighted_price
            }
            
            # Filter to only use trained features
            X = np.array([[feature_dict.get(name, 0.0) for name in self.feature_names]])
            
            # Handle missing values and infinite values
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
            
            # Scale features
            X_scaled = self.scaler.transform(X)
            
            # Make prediction
            active_model = self.calibrated_model if self.calibrate else self.model
            prob_up = active_model.predict_proba(X_scaled)[0, 1]
            prediction = active_model.predict(X_scaled)[0]
            
            return {
                "probability_up": float(prob_up),
                "probability_down": float(1 - prob_up),
                "prediction": int(prediction),
                "confidence": float(abs(prob_up - 0.5) * 2),  # 0 to 1
                "timestamp": features.timestamp
            }
            
        except Exception as e:
            self.logger.error(f"Error making prediction: {e}")
            return None
    
    def predict_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Make predictions on batch of data"""
        if not self.is_trained or df.empty:
            return pd.DataFrame()
        
        try:
            features_df = self._prepare_features(df)
            if features_df.empty:
                return pd.DataFrame()
            
            # Filter to only use trained features
            X = features_df[[name for name in self.feature_names 
                           if name in features_df.columns]].values
            
            # Handle missing values and infinite values
            X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
            
            # Scale features
            X_scaled = self.scaler.transform(X)
            
            # Make predictions
            active_model = self.calibrated_model if self.calibrate else self.model
            probabilities = active_model.predict_proba(X_scaled)[:, 1]
            predictions = active_model.predict(X_scaled)
            
            # Create results DataFrame
            results = pd.DataFrame({
                "probability_up": probabilities,
                "probability_down": 1 - probabilities,
                "prediction": predictions,
                "confidence": np.abs(probabilities - 0.5) * 2
            }, index=df.index)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error making batch predictions: {e}")
            return pd.DataFrame()
    
    def update_online(self, new_data: pd.DataFrame):
        """Update model with new data (online learning)"""
        if not self.is_trained or new_data.empty:
            return
        
        try:
            # Add to training buffer
            self.training_buffer.append(new_data)
            
            # Retrain if buffer is full
            if len(self.training_buffer) >= self.buffer_size // 10:  # Retrain every 10% of buffer
                combined_data = pd.concat(self.training_buffer[-self.buffer_size:])
                self.train(combined_data)
                
        except Exception as e:
            self.logger.error(f"Error updating model online: {e}")
    
    def save_model(self, filepath: str):
        """Save model to disk"""
        try:
            model_data = {
                'scaler': self.scaler,
                'model': self.model,
                'calibrated_model': self.calibrated_model,
                'feature_names': self.feature_names,
                'is_trained': self.is_trained,
                'prediction_horizon': self.prediction_horizon,
                'regularization': self.regularization,
                'calibrate': self.calibrate,
                'max_features': self.max_features
            }
            
            with open(filepath, 'wb') as f:
                pickle.dump(model_data, f)
                
            self.logger.info(f"Model saved to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving model: {e}")
    
    def load_model(self, filepath: str):
        """Load model from disk"""
        try:
            with open(filepath, 'rb') as f:
                model_data = pickle.load(f)
            
            self.scaler = model_data['scaler']
            self.model = model_data['model']
            self.calibrated_model = model_data['calibrated_model']
            self.feature_names = model_data['feature_names']
            self.is_trained = model_data['is_trained']
            self.prediction_horizon = model_data['prediction_horizon']
            self.regularization = model_data['regularization']
            self.calibrate = model_data['calibrate']
            self.max_features = model_data['max_features']
            
            self.logger.info(f"Model loaded from {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance from trained model"""
        if not self.is_trained or not self.feature_names:
            return {}
        
        try:
            # Get coefficients from logistic regression
            coefficients = self.model.coef_[0]
            importance = dict(zip(self.feature_names, np.abs(coefficients)))
            
            # Sort by importance
            sorted_importance = dict(sorted(importance.items(), 
                                          key=lambda x: x[1], reverse=True))
            
            return sorted_importance
            
        except Exception as e:
            self.logger.error(f"Error getting feature importance: {e}")
            return {}


# Example usage and testing
if __name__ == "__main__":
    import yaml
    from polymarket_api import PolymarketAPI
    from feature_engineering import FeatureEngine
    
    logging.basicConfig(level=logging.INFO)
    
    # Load configuration
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize components
    api = PolymarketAPI()
    feature_engine = FeatureEngine()
    model = TradingModel(
        prediction_horizon=config['model']['prediction_horizon'],
        regularization=config['model']['regularization'],
        max_features=config['model']['max_features']
    )
    
    # Test with real data
    markets = api.get_markets()
    if markets:
        test_market = markets[0]
        market_id = test_market['id']
        
        print(f"Testing model with market: {test_market.get('question', 'Unknown')}")
        
        # Collect some data
        print("Collecting training data...")
        for i in range(20):
            orderbook = api.get_orderbook(market_id)
            if orderbook:
                feature_engine.add_orderbook(market_id, orderbook)
                
                trades = api.get_trades(market_id, limit=10)
                for trade in trades:
                    feature_engine.add_trade(market_id, trade)
            
            time.sleep(1)
        
        # Get features DataFrame
        df = feature_engine.get_features_dataframe(market_id)
        
        if not df.empty and len(df) > 10:
            print(f"Training model on {len(df)} samples...")
            
            # Train model
            metrics = model.train(df)
            print(f"Training metrics: {metrics}")
            
            # Test prediction
            latest_features = feature_engine.get_latest_features(market_id)
            if latest_features:
                prediction = model.predict(latest_features)
                if prediction:
                    print(f"\nLatest prediction:")
                    print(f"  Probability up: {prediction['probability_up']:.4f}")
                    print(f"  Prediction: {'UP' if prediction['prediction'] else 'DOWN'}")
                    print(f"  Confidence: {prediction['confidence']:.4f}")
                
                # Feature importance
                importance = model.get_feature_importance()
                print(f"\nTop 5 most important features:")
                for feature, score in list(importance.items())[:5]:
                    print(f"  {feature}: {score:.4f}")
        else:
            print("Not enough data for training")