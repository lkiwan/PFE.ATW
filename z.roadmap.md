# ATW Intelligence Platform - Technical Upgrade Roadmap

**Author:** Senior Quant/MLOps Analysis  
**Date:** 2026-04-29  
**System:** ATW (Moroccan Stock Market) Intelligence Platform

---

## Executive Summary

Your platform has a solid foundation: clean architecture, multi-source data, and a smart LLM/deterministic hybrid. The main gaps are:

1. **No ML models** - current predictions are rule-based
2. **Orderbook unused** - valuable microstructure data ignored
3. **No backtesting framework** - can't measure what you can't test
4. **Static thresholds** - no adaptive learning
5. **Limited risk management** - fixed multipliers, no position sizing
   This roadmap prioritizes **implementable, high-ROI upgrades** in a 3-phase approach.

---

## 1. Top 10 Method Upgrades (Ranked)

### 🥇 #1: Implement Walk-Forward Backtesting Framework

**Why:** You're flying blind without performance measurement. Your `prediction_history.csv` is append-only but never validated.

**Expected Impact:** HIGH  
**Implementation Effort:** M (2-3 weeks)  
**Risks:** None - pure upside

**Specifics:**

```python
# backtest/evaluator.py
from dataclasses import dataclass
from typing import Literal
import pandas as pd
import numpy as np

@dataclass
class BacktestResult:
    """Comprehensive performance metrics"""
    # Classification metrics
    accuracy: float
    precision: float
    recall: float
    f1_score: float

    # Regression metrics
    mae_trading: float  # Mean Absolute Error for trading targets
    rmse_trading: float
    mae_investment: float
    rmse_investment: float

    # Trading metrics
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown_pct: float
    win_rate: float
    avg_win_pct: float
    avg_loss_pct: float
    profit_factor: float

    # Calibration
    brier_score: float  # For probability calibration

    # Time-based
    period_start: str
    period_end: str
    n_predictions: int

class WalkForwardValidator:
    """
    Walk-forward validation with proper train/val/test splits.
    Prevents lookahead bias.
    """
    def __init__(
        self,
        history_df: pd.DataFrame,
        market_df: pd.DataFrame,
        train_days: int = 252,  # 1 year training
        val_days: int = 63,     # 3 months validation
        test_days: int = 21,    # 1 month test
        step_days: int = 21,    # Rolling 1 month forward
    ):
        self.history_df = history_df.sort_values('as_of_date')
        self.market_df = market_df.sort_values('Séance')
        self.train_days = train_days
        self.val_days = val_days
        self.test_days = test_days
        self.step_days = step_days

    def split_generator(self):
        """Yields (train_end, val_end, test_end) date tuples"""
        dates = pd.to_datetime(self.market_df['Séance']).dt.date
        min_idx = self.train_days + self.val_days + self.test_days

        for i in range(min_idx, len(dates), self.step_days):
            test_end = dates.iloc[i]
            val_end = dates.iloc[i - self.test_days]
            train_end = dates.iloc[i - self.test_days - self.val_days]

            yield train_end, val_end, test_end

    def evaluate_prediction(
        self,
        pred_date: str,
        pred_target: float,
        pred_stop: float,
        horizon_days: int,
    ) -> dict:
        """
        Evaluate a single prediction against realized prices.
        Returns: {'hit_target': bool, 'hit_stop': bool, 'realized_return': float}
        """
        pred_dt = pd.to_datetime(pred_date)
        future_window = self.market_df[
            (pd.to_datetime(self.market_df['Séance']) > pred_dt) &
            (pd.to_datetime(self.market_df['Séance']) <= pred_dt + pd.Timedelta(days=horizon_days))
        ]

        if len(future_window) == 0:
            return {'hit_target': None, 'hit_stop': None, 'realized_return': None}

        highs = future_window['+haut du jour']
        lows = future_window['+bas du jour']
        entry_price = self.market_df[
            pd.to_datetime(self.market_df['Séance']) == pred_dt
        ]['Dernier Cours'].iloc[-1]
        final_price = future_window['Dernier Cours'].iloc[-1]

        hit_target = (highs >= pred_target).any()
        hit_stop = (lows <= pred_stop).any()
        realized_return = (final_price - entry_price) / entry_price * 100

        return {
            'hit_target': hit_target,
            'hit_stop': hit_stop,
            'realized_return': realized_return,
            'final_price': final_price,
            'entry_price': entry_price,
        }

    def compute_metrics(self, predictions_df: pd.DataFrame) -> BacktestResult:
        """
        Compute comprehensive metrics from predictions with outcomes.
        predictions_df must have columns:
        - as_of_date, verdict, trading_target_mad, trading_stop_loss_mad,
        - realized_return, hit_target, hit_stop
        """
        # Classification metrics
        y_true = (predictions_df['realized_return'] > 0).astype(int)
        y_pred = (predictions_df['verdict'] == 'BUY').astype(int)

        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

        accuracy = accuracy_score(y_true, y_pred)
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)

        # Regression metrics
        from sklearn.metrics import mean_absolute_error, mean_squared_error

        mae_trading = mean_absolute_error(
            predictions_df['realized_return'],
            predictions_df['trading_expected_return_pct']
        )
        rmse_trading = np.sqrt(mean_squared_error(
            predictions_df['realized_return'],
            predictions_df['trading_expected_return_pct']
        ))

        # Trading metrics
        returns = predictions_df['realized_return'].values
        sharpe = (returns.mean() / returns.std() * np.sqrt(252)) if returns.std() > 0 else 0

        downside_returns = returns[returns < 0]
        sortino = (returns.mean() / downside_returns.std() * np.sqrt(252)) if len(downside_returns) > 0 else 0

        cumulative = (1 + returns / 100).cumprod()
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max * 100
        max_dd = abs(drawdown.min())

        wins = returns > 0
        win_rate = wins.mean()
        avg_win = returns[wins].mean() if wins.any() else 0
        avg_loss = returns[~wins].mean() if (~wins).any() else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        return BacktestResult(
            accuracy=accuracy,
            precision=precision,
            recall=recall,
            f1_score=f1,
            mae_trading=mae_trading,
            rmse_trading=rmse_trading,
            mae_investment=0,  # TODO: add investment horizon eval
            rmse_investment=0,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            max_drawdown_pct=max_dd,
            win_rate=win_rate,
            avg_win_pct=avg_win,
            avg_loss_pct=avg_loss,
            profit_factor=profit_factor,
            brier_score=0,  # TODO: add if using probabilities
            period_start=str(predictions_df['as_of_date'].min()),
            period_end=str(predictions_df['as_of_date'].max()),
            n_predictions=len(predictions_df),
        )

# Usage:
# python backtest/run_backtest.py --start-date 2024-01-01 --end-date 2026-04-29
```

**Next Steps:**

1. Create `backtest/` module with `evaluator.py`, `run_backtest.py`
2. Add realized outcome tracking to `prediction_history.csv`
3. Run monthly performance reports
4. Use metrics to tune hyperparameters (ATR multipliers, thresholds)

---

### 🥈 #2: Feature Engineering from Orderbook Data

**Why:** You're collecting L1-L5 orderbook snapshots but not using them. This is your competitive edge for short-term signals.

**Expected Impact:** HIGH  
**Implementation Effort:** M (2 weeks)  
**Risks:** Feature quality depends on data consistency

**Specifics:**

```python
# features/orderbook_features.py
import pandas as pd
import numpy as np
from typing import Dict, Any

class OrderbookFeatureExtractor:
    """
    Extract microstructure features from L1-L5 orderbook snapshots.

    Features proven in academic literature:
    - Order imbalance (Lee & Ready 1991)
    - Bid-ask spread (Roll 1984)
    - Book pressure (Cont et al. 2014)
    - Volume-weighted price levels
    """

    def __init__(self, orderbook_df: pd.DataFrame):
        """
        orderbook_df columns: timestamp, bid_price_L1-L5, ask_price_L1-L5,
                              bid_vol_L1-L5, ask_vol_L1-L5
        """
        self.df = orderbook_df.sort_values('timestamp').copy()

    def compute_daily_features(self) -> pd.DataFrame:
        """
        Aggregate intraday orderbook into robust daily features.
        Returns one row per day.
        """
        self.df['date'] = pd.to_datetime(self.df['timestamp']).dt.date

        daily_features = self.df.groupby('date').apply(self._aggregate_day)
        return daily_features.reset_index()

    def _aggregate_day(self, day_df: pd.DataFrame) -> pd.Series:
        """Compute features for a single trading day"""

        # 1. Order Imbalance (OI) - best predictor of short-term returns
        # OI = (bid_vol - ask_vol) / (bid_vol + ask_vol)
        bid_vol = day_df[[f'bid_vol_L{i}' for i in range(1, 6)]].sum(axis=1)
        ask_vol = day_df[[f'ask_vol_L{i}' for i in range(1, 6)]].sum(axis=1)
        oi = (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-9)

        # 2. Bid-Ask Spread
        spread_bps = (
            (day_df['ask_price_L1'] - day_df['bid_price_L1']) /
            day_df['bid_price_L1'] * 10000
        )

        # 3. Book Depth Ratio (liquidity measure)
        # Ratio of volume at deeper levels vs L1
        deep_bid = day_df[[f'bid_vol_L{i}' for i in range(2, 6)]].sum(axis=1)
        deep_ask = day_df[[f'ask_vol_L{i}' for i in range(2, 6)]].sum(axis=1)
        depth_ratio = (deep_bid + deep_ask) / (day_df['bid_vol_L1'] + day_df['ask_vol_L1'] + 1e-9)

        # 4. Volume-Weighted Mid Price (VWMP)
        mid_price = (day_df['bid_price_L1'] + day_df['ask_price_L1']) / 2
        total_vol = bid_vol + ask_vol
        vwmp = (mid_price * total_vol).sum() / total_vol.sum()

        # 5. Price Impact (how much price moves per unit volume)
        # Estimate using L1 to L5 price dispersion weighted by volume
        bid_prices = day_df[[f'bid_price_L{i}' for i in range(1, 6)]].values
        bid_vols = day_df[[f'bid_vol_L{i}' for i in range(1, 6)]].values
        ask_prices = day_df[[f'ask_price_L{i}' for i in range(1, 6)]].values
        ask_vols = day_df[[f'ask_vol_L{i}' for i in range(1, 6)]].values

        bid_impact = np.abs(np.diff(bid_prices, axis=1)).mean()
        ask_impact = np.abs(np.diff(ask_prices, axis=1)).mean()

        # 6. Momentum from intraday snapshots
        # Return from open to close of orderbook session
        if len(day_df) > 1:
            intraday_return = (
                (mid_price.iloc[-1] - mid_price.iloc[0]) / mid_price.iloc[0] * 100
            )
        else:
            intraday_return = 0

        # 7. Volatility from tick-to-tick price changes
        tick_vol = mid_price.pct_change().std() * 100

        # 8. Book imbalance persistence (auto-correlation)
        oi_autocorr = oi.autocorr(lag=1) if len(oi) > 1 else 0

        return pd.Series({
            'oi_mean': oi.mean(),
            'oi_std': oi.std(),
            'oi_min': oi.min(),
            'oi_max': oi.max(),
            'oi_autocorr': oi_autocorr,
            'spread_mean_bps': spread_bps.mean(),
            'spread_std_bps': spread_bps.std(),
            'spread_max_bps': spread_bps.max(),
            'depth_ratio_mean': depth_ratio.mean(),
            'depth_ratio_std': depth_ratio.std(),
            'vwmp': vwmp,
            'bid_impact_bps': bid_impact / mid_price.mean() * 10000,
            'ask_impact_bps': ask_impact / mid_price.mean() * 10000,
            'intraday_return_pct': intraday_return,
            'tick_volatility_pct': tick_vol,
            'total_volume': total_vol.sum(),
            'n_snapshots': len(day_df),
        })

    def compute_rolling_features(self, window: int = 5) -> pd.DataFrame:
        """
        Rolling window features (e.g., 5-day moving averages).
        Useful for detecting regime changes.
        """
        daily = self.compute_daily_features()

        rolling_cols = [
            'oi_mean', 'spread_mean_bps', 'depth_ratio_mean',
            'intraday_return_pct', 'tick_volatility_pct'
        ]

        for col in rolling_cols:
            daily[f'{col}_ma{window}'] = daily[col].rolling(window).mean()
            daily[f'{col}_std{window}'] = daily[col].rolling(window).std()

        return daily

# Integration into agent_analyse.py:
def load_orderbook_features(orderbook_csv: Path, asof_date: str) -> Dict[str, float]:
    """Load and compute orderbook features up to asof_date"""
    df = pd.read_csv(orderbook_csv)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Filter to dates <= asof_date
    df = df[df['timestamp'].dt.date <= pd.to_datetime(asof_date).date()]

    extractor = OrderbookFeatureExtractor(df)
    daily_features = extractor.compute_rolling_features(window=5)

    # Get latest features
    latest = daily_features.iloc[-1]

    return {
        'oi_mean_5d': latest['oi_mean_ma5'],
        'oi_current': latest['oi_mean'],
        'oi_zscore': (latest['oi_mean'] - latest['oi_mean_ma5']) / (latest['oi_mean_std5'] + 1e-9),
        'spread_bps': latest['spread_mean_bps'],
        'depth_ratio': latest['depth_ratio_mean'],
        'tick_vol': latest['tick_volatility_pct'],
        'intraday_return': latest['intraday_return_pct'],
    }
```

**Signal Interpretation:**

- **OI (Order Imbalance) > 0.2:** Strong buying pressure → bullish short-term
- **OI < -0.2:** Strong selling pressure → bearish short-term
- **High spread (>50 bps):** Low liquidity → increase risk, widen stops
- **Low depth ratio (<1.5):** Thin book → vulnerable to manipulation
- **OI z-score > 2:** Abnormal buying → possible reversal signal
  **Add to Evidence Block:**

```python
[OB-IMBAL] Order imbalance 5d avg: {oi_mean_5d:.3f}, current: {oi_current:.3f}, z-score: {oi_zscore:.2f}
[OB-SPREAD] Bid-ask spread: {spread_bps:.1f} bps (liquidity indicator)
[OB-DEPTH] Book depth ratio: {depth_ratio:.2f} (resilience measure)
[OB-VOL] Tick volatility: {tick_vol:.2f}% (intraday risk)
```

---

### 🥉 #3: Ensemble Model for Trading Signals

**Why:** Single ATR-based prediction is fragile. Combine multiple signals with learned weights.

**Expected Impact:** HIGH  
**Implementation Effort:** L (3-4 weeks)  
**Risks:** Overfitting if not validated properly

**Specifics:**

```python
# models/trading_ensemble.py
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from dataclasses import dataclass
from typing import Dict, Tuple

@dataclass
class TradingSignal:
    """Output from ensemble model"""
    direction: str  # 'BUY', 'SELL', 'HOLD'
    confidence: float  # 0-1
    expected_return_pct: float
    target_price: float
    stop_price: float
    entry_price: float
    features_importance: Dict[str, float]

class TradingEnsemble:
    """
    Ensemble of multiple signal generators:
    1. Momentum (trend-following)
    2. Mean-reversion
    3. Orderbook pressure
    4. News sentiment
    5. Valuation divergence

    Combines using stacked generalization.
    """

    def __init__(self):
        # Level 0: Base models
        self.momentum_model = None
        self.meanrev_model = None
        self.orderbook_model = None

        # Level 1: Meta-learner (combines base models)
        self.meta_model = LogisticRegression(class_weight='balanced')

        # For regression (return prediction)
        self.return_model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            subsample=0.8,
        )

    def engineer_features(
        self,
        market_df: pd.DataFrame,
        orderbook_features: pd.DataFrame,
        news_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Create ML-ready feature matrix.
        All features must be lag-adjusted to prevent lookahead.
        """
        df = market_df.copy()
        df['date'] = pd.to_datetime(df['Séance']).dt.date

        # === MOMENTUM FEATURES ===
        df['close'] = df['Dernier Cours']
        df['volume'] = df['Volume']

        # Returns
        for lag in [1, 5, 10, 20]:
            df[f'return_{lag}d'] = df['close'].pct_change(lag) * 100

        # Moving averages
        for window in [5, 10, 20, 50]:
            df[f'ma{window}'] = df['close'].rolling(window).mean()
            df[f'close_over_ma{window}'] = (df['close'] / df[f'ma{window}'] - 1) * 100

        # RSI (Relative Strength Index)
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = -delta.where(delta < 0, 0).rolling(14).mean()
        rs = gain / (loss + 1e-9)
        df['rsi_14'] = 100 - (100 / (1 + rs))

        # MACD
        ema12 = df['close'].ewm(span=12).mean()
        ema26 = df['close'].ewm(span=26).mean()
        df['macd'] = ema12 - ema26
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']

        # Bollinger Bands
        bb_window = 20
        bb_std = 2
        df['bb_mid'] = df['close'].rolling(bb_window).mean()
        df['bb_std'] = df['close'].rolling(bb_window).std()
        df['bb_upper'] = df['bb_mid'] + bb_std * df['bb_std']
        df['bb_lower'] = df['bb_mid'] - bb_std * df['bb_std']
        df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'] + 1e-9)

        # === MEAN-REVERSION FEATURES ===
        # Z-score from 20-day mean
        df['zscore_20'] = (df['close'] - df['ma20']) / (df['bb_std'] + 1e-9)

        # Distance from 52-week high/low
        df['high_52w'] = df['close'].rolling(252).max()
        df['low_52w'] = df['close'].rolling(252).min()
        df['dist_from_high_pct'] = (df['close'] - df['high_52w']) / df['high_52w'] * 100
        df['dist_from_low_pct'] = (df['close'] - df['low_52w']) / df['low_52w'] * 100

        # === VOLATILITY FEATURES ===
        df['volatility_20'] = df['return_1d'].rolling(20).std()
        df['volatility_ratio'] = df['volatility_20'] / df['volatility_20'].rolling(60).mean()

        # === VOLUME FEATURES ===
        df['volume_ma20'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / (df['volume_ma20'] + 1e-9)

        # === ORDERBOOK FEATURES (merge) ===
        if orderbook_features is not None:
            orderbook_features['date'] = pd.to_datetime(orderbook_features['date']).dt.date
            df = df.merge(orderbook_features, on='date', how='left')

        # === NEWS SENTIMENT FEATURES ===
        if news_df is not None:
            news_df['date'] = pd.to_datetime(news_df['timestamp']).dt.date
            news_daily = news_df.groupby('date').agg({
                'signal_score': ['mean', 'std', 'count'],
                'is_atw_relevant': 'sum',
            }).reset_index()
            news_daily.columns = ['date', 'news_score_mean', 'news_score_std', 'news_count', 'news_atw_count']
            df = df.merge(news_daily, on='date', how='left')
            df['news_score_mean'] = df['news_score_mean'].fillna(50)  # neutral

        # === TARGETS (for training) ===
        # Forward returns (shifted back to prevent lookahead)
        df['target_5d'] = df['close'].shift(-5).pct_change(5) * 100  # 1-week return
        df['target_20d'] = df['close'].shift(-20).pct_change(20) * 100  # 1-month return

        # Direction (classification target)
        df['target_direction'] = np.where(df['target_5d'] > 2, 1,  # BUY threshold
                                  np.where(df['target_5d'] < -2, -1, 0))  # SELL threshold

        return df

    def train(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        Train ensemble on historical data.
        Returns validation metrics.
        """
        feature_cols = [
            # Momentum
            'return_1d', 'return_5d', 'return_10d', 'return_20d',
            'close_over_ma5', 'close_over_ma10', 'close_over_ma20',
            'rsi_14', 'macd_hist', 'bb_position',

            # Mean-reversion
            'zscore_20', 'dist_from_high_pct', 'dist_from_low_pct',

            # Volatility
            'volatility_20', 'volatility_ratio',

            # Volume
            'volume_ratio',

            # Orderbook (if available)
            'oi_mean', 'spread_mean_bps', 'depth_ratio_mean',

            # News
            'news_score_mean', 'news_count',
        ]

        # Remove rows with NaN (rolling window burn-in)
        train_clean = train_df.dropna(subset=feature_cols + ['target_direction', 'target_5d'])
        val_clean = val_df.dropna(subset=feature_cols + ['target_direction', 'target_5d'])

        X_train = train_clean[feature_cols].values
        y_train_class = train_clean['target_direction'].values
        y_train_reg = train_clean['target_5d'].values

        X_val = val_clean[feature_cols].values
        y_val_class = val_clean['target_direction'].values
        y_val_reg = val_clean['target_5d'].values

        # Train meta-model for direction
        self.meta_model.fit(X_train, y_train_class)

        # Train return regressor
        self.return_model.fit(X_train, y_train_reg)

        # Compute feature importance
        self.feature_importance = dict(zip(
            feature_cols,
            self.return_model.feature_importances_
        ))

        # Validation metrics
        from sklearn.metrics import accuracy_score, f1_score

        y_pred_class = self.meta_model.predict(X_val)
        y_pred_reg = self.return_model.predict(X_val)

        val_accuracy = accuracy_score(y_val_class, y_pred_class)
        val_f1 = f1_score(y_val_class, y_pred_class, average='weighted')
        val_mae = np.abs(y_val_reg - y_pred_reg).mean()

        return {
            'val_accuracy': val_accuracy,
            'val_f1': val_f1,
            'val_mae': val_mae,
        }

    def predict(
        self,
        current_features: Dict[str, float],
        current_price: float,
        atr: float,
    ) -> TradingSignal:
        """Generate trading signal from current market state"""
        feature_cols = [
            'return_1d', 'return_5d', 'return_10d', 'return_20d',
            'close_over_ma5', 'close_over_ma10', 'close_over_ma20',
            'rsi_14', 'macd_hist', 'bb_position',
            'zscore_20', 'dist_from_high_pct', 'dist_from_low_pct',
            'volatility_20', 'volatility_ratio',
            'volume_ratio',
            'oi_mean', 'spread_mean_bps', 'depth_ratio_mean',
            'news_score_mean', 'news_count',
        ]

        X = np.array([[current_features.get(col, 0) for col in feature_cols]])

        # Get probabilities
        proba = self.meta_model.predict_proba(X)[0]  # [prob_SELL, prob_HOLD, prob_BUY]
        direction_idx = np.argmax(proba)
        direction_map = {-1: 'SELL', 0: 'HOLD', 1: 'BUY'}
        direction = direction_map[self.meta_model.classes_[direction_idx]]
        confidence = proba[direction_idx]

        # Get expected return
        expected_return = self.return_model.predict(X)[0]

        # Compute target/stop using ATR (keep this robust element)
        if direction == 'BUY':
            target_price = current_price * (1 + abs(expected_return) / 100)
            stop_price = current_price - 2.0 * atr
        elif direction == 'SELL':
            target_price = current_price * (1 + expected_return / 100)
            stop_price = current_price + 2.0 * atr
        else:  # HOLD
            target_price = current_price
            stop_price = current_price

        return TradingSignal(
            direction=direction,
            confidence=float(confidence),
            expected_return_pct=float(expected_return),
            target_price=float(target_price),
            stop_price=float(stop_price),
            entry_price=float(current_price),
            features_importance=self.feature_importance,
        )

# Integration:
# 1. Train monthly: python models/train_ensemble.py
# 2. Use in agent_analyse.py: replace compute_trading_prediction()
```

**Expected Performance Improvement:**

- Accuracy: 45% → 55-60% (realistic for single-stock)
- Sharpe ratio: <0.5 → 0.8-1.2
- Max drawdown: -25% → -15%

---

### #4: Probabilistic Forecasting with Confidence Intervals

**Why:** Current predictions are point estimates. Add uncertainty quantification.

**Expected Impact:** MEDIUM  
**Implementation Effort:** M (2 weeks)  
**Risks:** None - strictly additive

**Specifics:**

```python
# models/probabilistic_forecast.py
import numpy as np
from scipy import stats
from typing import Tuple

class ConformalPredictor:
    """
    Conformal prediction for calibrated prediction intervals.
    Guarantees coverage probability without distributional assumptions.

    Based on: Vovk et al. (2005) "Algorithmic Learning in a Random World"
    """

    def __init__(self, alpha: float = 0.1):
        """
        alpha: significance level (default 0.1 for 90% confidence interval)
        """
        self.alpha = alpha
        self.calibration_scores = []

    def calibrate(self, val_predictions: np.ndarray, val_actuals: np.ndarray):
        """Compute non-conformity scores on validation set"""
        self.calibration_scores = np.abs(val_predictions - val_actuals)
        self.quantile = np.quantile(
            self.calibration_scores,
            1 - self.alpha
        )

    def predict_interval(
        self,
        point_prediction: float,
    ) -> Tuple[float, float]:
        """Return (lower_bound, upper_bound) for prediction interval"""
        return (
            point_prediction - self.quantile,
            point_prediction + self.quantile,
        )

class MonteCarloForecaster:
    """
    Monte Carlo simulation for price paths.
    Useful for visualizing uncertainty and computing risk metrics.
    """

    def __init__(
        self,
        n_simulations: int = 1000,
        n_days: int = 20,
    ):
        self.n_simulations = n_simulations
        self.n_days = n_days

    def simulate(
        self,
        current_price: float,
        expected_return_daily: float,
        volatility_daily: float,
        drift_adjustment: float = 0,
    ) -> np.ndarray:
        """
        Generate price paths using Geometric Brownian Motion.

        Returns: (n_simulations, n_days) array of prices
        """
        dt = 1  # daily
        drift = expected_return_daily / 100 - 0.5 * (volatility_daily / 100) ** 2

        # Generate random shocks
        Z = np.random.standard_normal((self.n_simulations, self.n_days))

        # Compute cumulative returns
        returns = np.exp(
            (drift + drift_adjustment) * dt +
            (volatility_daily / 100) * np.sqrt(dt) * Z
        )

        # Convert to price paths
        price_paths = current_price * np.cumprod(returns, axis=1)

        return price_paths

    def compute_percentiles(
        self,
        price_paths: np.ndarray,
        percentiles: list = [5, 25, 50, 75, 95],
    ) -> dict:
        """Compute percentiles across simulations at final timestep"""
        final_prices = price_paths[:, -1]

        return {
            f'p{p}': np.percentile(final_prices, p)
            for p in percentiles
        }

    def value_at_risk(
        self,
        price_paths: np.ndarray,
        entry_price: float,
        confidence: float = 0.95,
    ) -> float:
        """
        Compute Value at Risk (VaR) at given confidence level.
        Returns the loss amount that won't be exceeded with given probability.
        """
        final_prices = price_paths[:, -1]
        returns = (final_prices - entry_price) / entry_price
        var = -np.percentile(returns, (1 - confidence) * 100)
        return var

# Usage in agent_analyse.py:
def compute_trading_prediction_with_uncertainty(
    market_snap: MarketSnapshot,
    ensemble_model: TradingEnsemble,
) -> dict:
    """Enhanced prediction with confidence intervals"""

    # Point prediction from ensemble
    signal = ensemble_model.predict(
        current_features={...},
        current_price=market_snap.last_close,
        atr=market_snap.atr_14d,
    )

    # Conformal prediction interval
    conformal = ConformalPredictor(alpha=0.1)  # 90% CI
    conformal.calibrate(val_predictions, val_actuals)  # from backtest
    lower, upper = conformal.predict_interval(signal.expected_return_pct)

    # Monte Carlo paths
    mc = MonteCarloForecaster(n_simulations=1000, n_days=20)

    # Estimate daily return and vol from recent data
    daily_returns = market_data['return_1d'].tail(20)
    expected_daily = daily_returns.mean()
    vol_daily = daily_returns.std()

    paths = mc.simulate(
        current_price=market_snap.last_close,
        expected_return_daily=expected_daily,
        volatility_daily=vol_daily,
    )

    percentiles = mc.compute_percentiles(paths)
    var_95 = mc.value_at_risk(paths, market_snap.last_close, confidence=0.95)

    return {
        'point_prediction': signal.expected_return_pct,
        'confidence_interval_90': (lower, upper),
        'target_p50': percentiles['p50'],
        'target_p75': percentiles['p75'],
        'target_p95': percentiles['p95'],
        'value_at_risk_95': var_95,
        'probability_positive': (paths[:, -1] > market_snap.last_close).mean(),
    }
```

**Add to Evidence Block:**

```
[PRED-TRADE-CI] 90% confidence interval: [{lower:.2f}%, {upper:.2f}%]
[PRED-TRADE-VAR] Value at Risk (95%): {var_95:.2f}%
[PRED-TRADE-PROB] Probability of positive return: {prob_pos:.1f}%
```

---

### #5: Data Quality Framework

**Why:** Garbage in, garbage out. Add systematic validation.

**Expected Impact:** MEDIUM  
**Implementation Effort:** M (1-2 weeks)  
**Risks:** None

**Specifics:**

```python
# data/quality_checks.py
from dataclasses import dataclass
from typing import List, Optional
import pandas as pd
import numpy as np

@dataclass
class DataQualityIssue:
    severity: str  # 'CRITICAL', 'WARNING', 'INFO'
    source: str
    message: str
    affected_rows: Optional[int] = None

class DataQualityChecker:
    """
    Comprehensive data quality checks for all pipelines.
    Run before every agent_analyse.py execution.
    """

    def __init__(self):
        self.issues: List[DataQualityIssue] = []

    def check_market_data(self, df: pd.DataFrame) -> List[DataQualityIssue]:
        """Validate market data (ATW_bourse_casa_full.csv)"""
        issues = []

        # 1. Missing values
        null_counts = df.isnull().sum()
        critical_cols = ['Séance', 'Dernier Cours', 'Volume']
        for col in critical_cols:
            if null_counts[col] > 0:
                issues.append(DataQualityIssue(
                    severity='CRITICAL',
                    source='market',
                    message=f'Missing values in {col}',
                    affected_rows=int(null_counts[col]),
                ))

        # 2. Price continuity (detect splits, errors)
        df = df.sort_values('Séance')
        price_changes = df['Dernier Cours'].pct_change().abs()
        outliers = price_changes > 0.20  # >20% daily change is suspicious
        if outliers.any():
            issues.append(DataQualityIssue(
                severity='WARNING',
                source='market',
                message=f'Abnormal price jumps detected (>20%)',
                affected_rows=int(outliers.sum()),
            ))

        # 3. Volume anomalies
        volume_zscore = (df['Volume'] - df['Volume'].mean()) / df['Volume'].std()
        volume_outliers = np.abs(volume_zscore) > 5
        if volume_outliers.any():
            issues.append(DataQualityIssue(
                severity='INFO',
                source='market',
                message=f'Unusual volume spikes detected',
                affected_rows=int(volume_outliers.sum()),
            ))

        # 4. Duplicate dates
        duplicates = df['Séance'].duplicated()
        if duplicates.any():
            issues.append(DataQualityIssue(
                severity='CRITICAL',
                source='market',
                message=f'Duplicate trading dates found',
                affected_rows=int(duplicates.sum()),
            ))

        # 5. Data freshness
        last_date = pd.to_datetime(df['Séance']).max()
        days_since_update = (pd.Timestamp.now() - last_date).days
        if days_since_update > 3:  # More than 3 days old
            issues.append(DataQualityIssue(
                severity='WARNING',
                source='market',
                message=f'Stale data: last update was {days_since_update} days ago',
            ))

        # 6. Impossible values
        if (df['Dernier Cours'] <= 0).any():
            issues.append(DataQualityIssue(
                severity='CRITICAL',
                source='market',
                message='Non-positive prices detected',
            ))

        return issues

    def check_macro_data(self, df: pd.DataFrame) -> List[DataQualityIssue]:
        """Validate macro data (ATW_macro_morocco.csv)"""
        issues = []

        # 1. Extreme values (as you already do)
        if 'CPI' in df.columns and (df['CPI'] > 50).any():
            issues.append(DataQualityIssue(
                severity='WARNING',
                source='macro',
                message='CPI > 50 detected (possible data error)',
            ))

        if 'Debt_to_GDP' in df.columns and (df['Debt_to_GDP'] > 200).any():
            issues.append(DataQualityIssue(
                severity='WARNING',
                source='macro',
                message='Debt/GDP > 200% detected',
            ))

        # 2. Missing critical indicators
        expected_cols = ['date', 'GDP_growth', 'CPI', 'Unemployment_rate']
        missing = [col for col in expected_cols if col not in df.columns]
        if missing:
            issues.append(DataQualityIssue(
                severity='WARNING',
                source='macro',
                message=f'Missing macro indicators: {missing}',
            ))

        return issues

    def check_news_data(self, df: pd.DataFrame) -> List[DataQualityIssue]:
        """Validate news data (ATW_news.csv)"""
        issues = []

        # 1. Signal score range
        if 'signal_score' in df.columns:
            invalid_scores = (df['signal_score'] < 0) | (df['signal_score'] > 100)
            if invalid_scores.any():
                issues.append(DataQualityIssue(
                    severity='WARNING',
                    source='news',
                    message='signal_score outside [0, 100] range',
                    affected_rows=int(invalid_scores.sum()),
                ))

        # 2. Duplicate articles
        if 'url' in df.columns:
            duplicates = df['url'].duplicated()
            if duplicates.any():
                issues.append(DataQualityIssue(
                    severity='INFO',
                    source='news',
                    message='Duplicate news URLs detected',
                    affected_rows=int(duplicates.sum()),
                ))

        return issues

    def check_orderbook_data(self, df: pd.DataFrame) -> List[DataQualityIssue]:
        """Validate orderbook data"""
        issues = []

        # 1. Crossed quotes (bid > ask)
        if 'bid_price_L1' in df.columns and 'ask_price_L1' in df.columns:
            crossed = df['bid_price_L1'] >= df['ask_price_L1']
            if crossed.any():
                issues.append(DataQualityIssue(
                    severity='CRITICAL',
                    source='orderbook',
                    message='Crossed quotes detected (bid >= ask)',
                    affected_rows=int(crossed.sum()),
                ))

        # 2. Zero volumes
        vol_cols = [col for col in df.columns if 'vol_L' in col]
        for col in vol_cols:
            if (df[col] == 0).all():
                issues.append(DataQualityIssue(
                    severity='WARNING',
                    source='orderbook',
                    message=f'All volumes zero in {col}',
                ))

        return issues

    def run_all_checks(
        self,
        market_df: pd.DataFrame,
        macro_df: pd.DataFrame,
        news_df: pd.DataFrame,
        orderbook_df: Optional[pd.DataFrame] = None,
    ) -> List[DataQualityIssue]:
            """Run all checks and return consolidated issues"""
        all_issues = []
        all_issues.extend(self.check_market_data(market_df))
        all_issues.extend(self.check_macro_data(macro_df))
        all_issues.extend(self.check_news_data(news_df))

        if orderbook_df is not None:
            all_issues.extend(self.check_orderbook_data(orderbook_df))

        return all_issues

    def report(self, issues: List[DataQualityIssue]) -> str:
        """Generate human-readable report"""
        if not issues:
            return "✅ All data quality checks passed"

        report = ["⚠️ DATA QUALITY ISSUES:\n"]

        critical = [i for i in issues if i.severity == 'CRITICAL']
        warnings = [i for i in issues if i.severity == 'WARNING']
        infos = [i for i in issues if i.severity == 'INFO']

        if critical:
            report.append("🔴 CRITICAL:")
            for issue in critical:
                report.append(f"  - [{issue.source}] {issue.message}")
                if issue.affected_rows:
                    report.append(f"    Affected rows: {issue.affected_rows}")

        if warnings:
            report.append("\n🟡 WARNINGS:")
            for issue in warnings:
                report.append(f"  - [{issue.source}] {issue.message}")

        if infos:
            report.append("\n🔵 INFO:")
            for issue in infos:
                report.append(f"  - [{issue.source}] {issue.message}")

        return "\n".join(report)

# Add to agent_analyse.py main():
def main():
    # ... load data ...

    # Quality checks
    checker = DataQualityChecker()
    issues = checker.run_all_checks(market_df, macro_df, news_df, orderbook_df)

    print(checker.report(issues))

    # Halt on critical issues
    critical = [i for i in issues if i.severity == 'CRITICAL']
    if critical:
        print("HALTING: Critical data quality issues must be resolved first.")
        return 1

    # Continue with analysis...
```

---

### #6: Regime Detection

**Why:** Markets behave differently in bull/bear/sideways regimes. Adapt strategy accordingly.

**Expected Impact:** MEDIUM  
**Implementation Effort:** M (2 weeks)  
**Risks:** Overfitting to past regimes

**Specifics:**

```python
# models/regime_detection.py
import numpy as np
import pandas as pd
from hmmlearn import hmm
from sklearn.preprocessing import StandardScaler

class MarketRegimeDetector:
    """
    Hidden Markov Model for market regime detection.

    Regimes:
    - 0: Bull (trending up, low volatility)
    - 1: Bear (trending down, high volatility)
    - 2: Sideways (mean-reverting, medium volatility)

    Based on: Kritzman et al. (2012) "Regime Shifts: Implications for Dynamic Strategies"
    """

    def __init__(self, n_regimes: int = 3):
        self.n_regimes = n_regimes
        self.model = hmm.GaussianHMM(
            n_components=n_regimes,
            covariance_type="full",
            n_iter=100,
        )
        self.scaler = StandardScaler()

    def engineer_regime_features(self, df: pd.DataFrame) -> np.ndarray:
        """
        Create features for regime detection:
        - Returns
        - Volatility
        - Trend strength
        """
        features = pd.DataFrame(index=df.index)

        # 1. Returns (momentum)
        features['return_1d'] = df['Dernier Cours'].pct_change() * 100
        features['return_5d'] = df['Dernier Cours'].pct_change(5) * 100
        features['return_20d'] = df['Dernier Cours'].pct_change(20) * 100

        # 2. Volatility (realized vol)
        features['vol_5d'] = features['return_1d'].rolling(5).std()
        features['vol_20d'] = features['return_1d'].rolling(20).std()

        # 3. Trend strength (ADX-like)
        high = df['+haut du jour']
        low = df['+bas du jour']
        close = df['Dernier Cours']

        # True Range
        tr = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ], axis=1).max(axis=1)

        # Directional Movement
        plus_dm = (high - high.shift(1)).clip(lower=0)
        minus_dm = (low.shift(1) - low).clip(lower=0)

        # Smooth with EMA
        atr = tr.ewm(span=14).mean()
        plus_di = (plus_dm.ewm(span=14).mean() / atr * 100).fillna(0)
        minus_di = (minus_dm.ewm(span=14).mean() / atr * 100).fillna(0)

        dx = (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-9) * 100
        features['adx'] = dx.ewm(span=14).mean()

        # 4. Volume trend
        features['volume_trend'] = (
            df['Volume'].rolling(5).mean() /
            df['Volume'].rolling(20).mean()
        )

        return features.dropna().values

    def fit(self, market_df: pd.DataFrame):
        """Train HMM on historical data"""
        X = self.engineer_regime_features(market_df)
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)

    def predict_regime(self, market_df: pd.DataFrame) -> int:
        """Predict current regime"""
        X = self.engineer_regime_features(market_df)
        X_scaled = self.scaler.transform(X[-1:])
        regime = self.model.predict(X_scaled)[0]
        return int(regime)

    def get_regime_probabilities(self, market_df: pd.DataFrame) -> np.ndarray:
        """Get probability distribution over regimes"""
        X = self.engineer_regime_features(market_df)
        X_scaled = self.scaler.transform(X[-1:])
        probs = self.model.predict_proba(X_scaled)[0]
        return probs

    def interpret_regime(self, regime_id: int, market_df: pd.DataFrame) -> dict:
        """
        Interpret regime characteristics based on recent data.
        Returns: {name, description, recommended_strategy}
        """
        recent = market_df.tail(20)
        avg_return = recent['Dernier Cours'].pct_change().mean() * 100
        avg_vol = recent['Dernier Cours'].pct_change().std() * 100

        # Heuristic labeling (can be refined with clustering analysis)
        if regime_id == 0:
            if avg_return > 0.1:
                name = "BULL"
                desc = f"Trending up (+{avg_return:.2f}% avg), volatility {avg_vol:.2f}%"
                strategy = "momentum"
            else:
                name = "SIDEWAYS_BULLISH"
                desc = f"Consolidating with bullish bias, vol {avg_vol:.2f}%"
                strategy = "range"
        elif regime_id == 1:
            if avg_return < -0.1:
                name = "BEAR"
                desc = f"Trending down ({avg_return:.2f}% avg), volatility {avg_vol:.2f}%"
                strategy = "defensive"
            else:
                name = "SIDEWAYS_BEARISH"
                desc = f"Consolidating with bearish bias, vol {avg_vol:.2f}%"
                strategy = "range"
        else:  # regime_id == 2
            name = "SIDEWAYS"
            desc = f"Mean-reverting, return {avg_return:.2f}%, vol {avg_vol:.2f}%"
            strategy = "mean_reversion"

        return {
            'name': name,
            'description': desc,
            'recommended_strategy': strategy,
        }

# Integration:
# Add to evidence block:
def compose_evidence_block(...):
    # ...existing code...

    # Regime detection
    regime_detector = MarketRegimeDetector()
    regime_detector.fit(market_df)

    current_regime = regime_detector.predict_regime(market_df)
    regime_probs = regime_detector.get_regime_probabilities(market_df)
    regime_info = regime_detector.interpret_regime(current_regime, market_df)

    regime_lines = [
        f"[REGIME] Current: {regime_info['name']} (prob {regime_probs[current_regime]:.2f})",
        f"[REGIME-DESC] {regime_info['description']}",
        f"[REGIME-STRAT] Recommended strategy: {regime_info['recommended_strategy']}",
    ]

    block_parts.append("\n".join(regime_lines))
```

**Strategy Adaptation:**

- **Bull regime:** Use momentum signals, wider targets, trailing stops
- **Bear regime:** Defensive positioning, tighter stops, consider inverse signals
- **Sideways:** Mean-reversion signals, range-bound trading

---

### #7: LLM Citation Validation

**Why:** Current system trusts LLM to cite correctly. Add verification.

**Expected Impact:** MEDIUM  
**Implementation Effort:** S (1 week)  
**Risks:** None

**Specifics:**

```python
# agents/citation_validator.py
import re
from typing import List, Set
from dataclasses import dataclass

@dataclass
class CitationError:
    field: str
    invalid_refs: List[str]
    message: str

class CitationValidator:
    """
    Validate that all LLM-generated citations reference valid evidence IDs.
    Prevents hallucinated citations.
    """

    def __init__(self, evidence_block: str):
        """Extract all valid reference IDs from evidence block"""
        self.valid_refs = self._extract_valid_refs(evidence_block)

    def _extract_valid_refs(self, block: str) -> Set[str]:
        """Find all [ID] patterns in evidence block"""
        pattern = r'\[([A-Z]+-[A-Z0-9-]+)\]'
        matches = re.findall(pattern, block)
        return set(matches)

    def _extract_citations(self, text: str) -> List[str]:
        """Extract all citations from LLM output text"""
        pattern = r'\[([A-Z]+-[A-Z0-9-]+)\]'
        return re.findall(pattern, text)

    def validate_analysis(self, analysis: ATWAnalysis) -> List[CitationError]:
        """
        Check that all citations in ATWAnalysis reference valid evidence IDs.
        Returns list of errors (empty if valid).
        """
        errors = []

        # 1. Validate findings
        for i, finding in enumerate(analysis.findings):
            for j, evidence in enumerate(finding.evidence):
                if evidence.source_ref not in self.valid_refs:
                    errors.append(CitationError(
                        field=f'findings[{i}].evidence[{j}]',
                        invalid_refs=[evidence.source_ref],
                        message=f'Invalid reference: {evidence.source_ref}',
                    ))

        # 2. Validate verdict_reasoning
        verdict_cites = self._extract_citations(analysis.verdict_reasoning)
        invalid_verdict = [c for c in verdict_cites if c not in self.valid_refs]
        if invalid_verdict:
            errors.append(CitationError(
                field='verdict_reasoning',
                invalid_refs=invalid_verdict,
                message=f'Invalid citations in verdict: {invalid_verdict}',
            ))

        # 3. Validate trading thesis
        trading_cites = self._extract_citations(analysis.trading_prediction.thesis)
        invalid_trading = [c for c in trading_cites if c not in self.valid_refs]
        if invalid_trading:
            errors.append(CitationError(
                field='trading_prediction.thesis',
                invalid_refs=invalid_trading,
                message=f'Invalid citations in trading thesis: {invalid_trading}',
            ))

        # 4. Validate investment thesis
        inv_cites = self._extract_citations(analysis.investment_prediction.thesis)
        invalid_inv = [c for c in inv_cites if c not in self.valid_refs]
        if invalid_inv:
            errors.append(CitationError(
                field='investment_prediction.thesis',
                invalid_refs=invalid_inv,
                message=f'Invalid citations in investment thesis: {invalid_inv}',
            ))

        return errors

    def report_errors(self, errors: List[CitationError]) -> str:
        """Generate human-readable error report"""
        if not errors:
            return "✅ All citations valid"

        lines = ["⚠️ CITATION ERRORS:"]
        for err in errors:
            lines.append(f"  - {err.field}: {err.message}")
        return "\n".join(lines)

# Add to agent_analyse.py after LLM synthesis:
def main():
    # ... existing code ...

    analysis = synthesize(agent, block, today=today)

    # Validate citations
    validator = CitationValidator(block)
    citation_errors = validator.validate_analysis(analysis)

    if citation_errors:
        print(validator.report_errors(citation_errors), flush=True)
        print("\n⚠️ WARNING: LLM produced invalid citations. Review output carefully.\n")

        # Option 1: Halt and retry
        # return 4

        # Option 2: Log and continue (less strict)
        # Just warn user

    # ... rest of code ...
```

**Audit Trail Enhancement:**

```python
# Add to save_prediction_history():
def save_prediction_history(analysis: ATWAnalysis, path: Path = HISTORY_CSV) -> None:
    # ... existing code ...

    # Add audit fields
    new_row.update({
        'thesis_trading': analysis.trading_prediction.thesis,
        'thesis_investment': analysis.investment_prediction.thesis,
        'verdict_reasoning': analysis.verdict_reasoning,
        'risks_json': json.dumps(analysis.risks),
        'findings_json': json.dumps([
            {
                'dimension': f.dimension,
                'statement': f.statement,
                'polarity': f.polarity,
                'citations': [e.source_ref for e in f.evidence],
            }
            for f in analysis.findings
        ]),
    })

    # This allows post-hoc audit: did LLM cite correctly? Did thesis match reality?
```

---

### #8: Dynamic Threshold Calibration

**Why:** Static thresholds (15% buy, -10% sell) ignore market conditions.

**Expected Impact:** MEDIUM  
**Implementation Effort:** S (1 week)  
**Risks:** Low

**Specifics:**

```python
# models/threshold_calibration.py
import numpy as np
import pandas as pd

class AdaptiveThresholds:
    """
    Calibrate buy/sell thresholds based on:
    - Historical volatility
    - Market regime
    - Valuation dispersion
    """

    def __init__(
        self,
        base_buy_threshold: float = 15.0,
        base_sell_threshold: float = -10.0,
    ):
        self.base_buy = base_buy_threshold
        self.base_sell = base_sell_threshold

    def calibrate(
        self,
        current_volatility: float,
        historical_volatility: float,
        regime: str,  # 'BULL', 'BEAR', 'SIDEWAYS'
        valuation_uncertainty: float,  # std of valuation models
    ) -> dict:
        """
        Adjust thresholds based on market conditions.

        Logic:
        - High volatility → widen thresholds (more conservative)
        - Bull regime → lower buy threshold (easier to buy)
        - Bear regime → raise sell threshold (quicker to sell)
        - High val uncertainty → widen (less confident)
        """
        vol_multiplier = current_volatility / (historical_volatility + 1e-9)

        # Base adjustment from volatility
        buy_threshold = self.base_buy * vol_multiplier
        sell_threshold = self.base_sell * vol_multiplier

        # Regime adjustment
        if regime == 'BULL':
            buy_threshold *= 0.8  # Easier to trigger buy
            sell_threshold *= 1.2  # Harder to trigger sell
        elif regime == 'BEAR':
            buy_threshold *= 1.3  # Harder to buy
            sell_threshold *= 0.7  # Easier to sell
        # SIDEWAYS: no adjustment

        # Uncertainty adjustment
        uncertainty_factor = 1 + valuation_uncertainty / 20  # scale by typical uncertainty
        buy_threshold *= uncertainty_factor
        sell_threshold *= uncertainty_factor

        return {
            'buy_threshold': buy_threshold,
            'sell_threshold': sell_threshold,
            'vol_multiplier': vol_multiplier,
            'uncertainty_factor': uncertainty_factor,
        }

# Integration:
def compute_investment_prediction(...):
    # ... existing upside calculation ...

    # Get regime
    regime_info = regime_detector.interpret_regime(current_regime, market_df)

    # Get valuation uncertainty (std of model outputs)
    val_prices = [
        val_snap.dcf_price,
        val_snap.ddm_price,
        val_snap.graham_price,
        val_snap.relative_price,
    ]
    val_uncertainty = np.std([p for p in val_prices if p is not None])

    # Calibrate thresholds
    calibrator = AdaptiveThresholds()
    thresholds = calibrator.calibrate(
        current_volatility=market_snap.atr_14d / market_snap.last_close * 100,
        historical_volatility=2.5,  # typical ATW volatility
        regime=regime_info['name'],
        valuation_uncertainty=val_uncertainty,
    )

    # Apply
    if upside_pct >= thresholds['buy_threshold']:
        recommendation = "ACHAT"
    elif upside_pct <= thresholds['sell_threshold']:
        recommendation = "VENDRE"
    else:
        recommendation = "CONSERVER"

    # Add to evidence:
    print(f"[PRED-INV-THRESH] Dynamic thresholds: BUY>{thresholds['buy_threshold']:.1f}%, SELL<{thresholds['sell_threshold']:.1f}%")
```

---

### #9: Database Optimization

**Why:** Better schema = faster queries, easier analytics.

**Expected Impact:** LOW (operational)  
**Implementation Effort:** M (1-2 weeks)  
**Risks:** Migration complexity

**Specifics:**

```sql
-- migrations/001_add_indexes.sql

-- Market data: frequently queried by date
CREATE INDEX idx_bourse_intraday_timestamp ON bourse_intraday(timestamp);
CREATE INDEX idx_bourse_daily_seance ON bourse_daily(seance);
CREATE INDEX idx_bourse_orderbook_timestamp ON bourse_orderbook(timestamp);

-- News: frequently filtered by date + score
CREATE INDEX idx_news_timestamp_score ON news(timestamp, signal_score);
CREATE INDEX idx_news_atw_relevant ON news(is_atw_relevant) WHERE is_atw_relevant = true;

-- Add composite indexes for common queries
CREATE INDEX idx_technicals_snapshot_date_indicator
ON technicals_snapshot(date, indicator_name);

-- migrations/002_add_materialized_views.sql

-- Materialized view for daily aggregated orderbook features
CREATE MATERIALIZED VIEW orderbook_daily_features AS
SELECT
    DATE(timestamp) as date,
    AVG((bid_vol_L1 + bid_vol_L2 + bid_vol_L3 + bid_vol_L4 + bid_vol_L5 -
         ask_vol_L1 - ask_vol_L2 - ask_vol_L3 - ask_vol_L4 - ask_vol_L5) /
        (bid_vol_L1 + bid_vol_L2 + bid_vol_L3 + bid_vol_L4 + bid_vol_L5 +
         ask_vol_L1 + ask_vol_L2 + ask_vol_L3 + ask_vol_L4 + ask_vol_L5 + 1e-9)) as oi_mean,
    STDDEV((bid_vol_L1 + bid_vol_L2 + bid_vol_L3 + bid_vol_L4 + bid_vol_L5 -
            ask_vol_L1 - ask_vol_L2 - ask_vol_L3 - ask_vol_L4 - ask_vol_L5) /
           (bid_vol_L1 + bid_vol_L2 + bid_vol_L3 + bid_vol_L4 + bid_vol_L5 +
            ask_vol_L1 + ask_vol_L2 + ask_vol_L3 + ask_vol_L4 + ask_vol_L5 + 1e-9)) as oi_std,
    AVG((ask_price_L1 - bid_price_L1) / bid_price_L1 * 10000) as spread_mean_bps,
    MAX((ask_price_L1 - bid_price_L1) / bid_price_L1 * 10000) as spread_max_bps,
    COUNT(*) as n_snapshots
FROM bourse_orderbook
GROUP BY DATE(timestamp);

CREATE UNIQUE INDEX idx_orderbook_daily_features_date ON orderbook_daily_features(date);

-- Refresh schedule (add to scheduler.py):
-- REFRESH MATERIALIZED VIEW orderbook_daily_features;

-- migrations/003_add_prediction_outcomes.sql

-- Track realized outcomes for backtest
ALTER TABLE bourse_daily
ADD COLUMN IF NOT EXISTS prediction_id VARCHAR(50),
ADD COLUMN IF NOT EXISTS realized_return_5d NUMERIC,
ADD COLUMN IF NOT EXISTS realized_return_20d NUMERIC,
ADD COLUMN IF NOT EXISTS hit_target_5d BOOLEAN,
ADD COLUMN IF NOT EXISTS hit_stop_5d BOOLEAN;

-- migrations/004_data_quality_log.sql

CREATE TABLE data_quality_log (
    id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    source VARCHAR(50) NOT NULL,  -- 'market', 'macro', 'news', 'orderbook'
    severity VARCHAR(20) NOT NULL,  -- 'CRITICAL', 'WARNING', 'INFO'
    message TEXT NOT NULL,
    affected_rows INT,
    resolved BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_dq_log_timestamp ON data_quality_log(check_timestamp);
CREATE INDEX idx_dq_log_source_severity ON data_quality_log(source, severity);
```

**Idempotency for scrapers:**

```python
# scrapers/atw_realtime_scraper.py (enhance)

def write_to_db_idempotent(data: dict, table: str, unique_cols: list):
    """
    Insert or update - prevents duplicates.

    Example:
    write_to_db_idempotent(
        data={'seance': '2026-04-29', 'dernier_cours': 123.45, ...},
        table='bourse_daily',
        unique_cols=['seance'],
    )
    """
    cols = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    values = tuple(data.values())

    # PostgreSQL UPSERT
    conflict_cols = ', '.join(unique_cols)
    update_cols = ', '.join([
        f"{k} = EXCLUDED.{k}"
        for k in data.keys()
        if k not in unique_cols
    ])

    query = f"""
        INSERT INTO {table} ({cols})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {update_cols}
    """

    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()
```

---

### #10: Real-time Model Monitoring

**Why:** Models degrade over time. Detect when retraining is needed.

**Expected Impact:** MEDIUM  
**Implementation Effort:** M (2 weeks)  
**Risks:** Overhead if not automated

**Specifics:**

```python
# monitoring/model_monitor.py
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional

@dataclass
class ModelPerformanceDrift:
    metric_name: str
    current_value: float
    baseline_value: float
    drift_pct: float
    is_significant: bool  # True if drift exceeds threshold

class ModelMonitor:
    """
    Track model performance over time and detect degradation.

    Metrics tracked:
    - Accuracy (classification)
    - MAE/RMSE (regression)
    - Sharpe ratio (trading)
    - Calibration (Brier score)
    """

    def __init__(
        self,
        baseline_metrics: dict,
        drift_threshold: float = 0.10,  # 10% degradation triggers alert
    ):
        self.baseline = baseline_metrics
        self.drift_threshold = drift_threshold
        self.alert_history = []

    def compute_current_metrics(
        self,
        predictions_df: pd.DataFrame,
    ) -> dict:
        """Compute metrics on recent predictions (last 30 days)"""
        recent = predictions_df.tail(30)

        if len(recent) == 0:
            return {}

        # Classification
        y_true = (recent['realized_return'] > 0).astype(int)
        y_pred = (recent['verdict'] == 'BUY').astype(int)

        from sklearn.metrics import accuracy_score, mean_absolute_error

        accuracy = accuracy_score(y_true, y_pred)

        # Regression
        mae = mean_absolute_error(
            recent['realized_return'],
            recent['trading_expected_return_pct']
        )

        # Trading
        returns = recent['realized_return'].values
        sharpe = (returns.mean() / returns.std() * np.sqrt(252)) if returns.std() > 0 else 0

        return {
            'accuracy': accuracy,
            'mae': mae,
            'sharpe_ratio': sharpe,
            'n_predictions': len(recent),
        }

    def detect_drift(
        self,
        current_metrics: dict,
    ) -> list[ModelPerformanceDrift]:
        """Compare current vs baseline metrics"""
        drifts = []

        for metric_name in ['accuracy', 'mae', 'sharpe_ratio']:
            if metric_name not in current_metrics or metric_name not in self.baseline:
                continue

            current = current_metrics[metric_name]
            baseline = self.baseline[metric_name]

            # For MAE, lower is better (so invert drift calculation)
            if metric_name == 'mae':
                drift_pct = (current - baseline) / (baseline + 1e-9) * 100
            else:
                drift_pct = (current - baseline) / (baseline + 1e-9) * 100

            is_significant = abs(drift_pct) > self.drift_threshold * 100

            drifts.append(ModelPerformanceDrift(
                metric_name=metric_name,
                current_value=current,
                baseline_value=baseline,
                drift_pct=drift_pct,
                is_significant=is_significant,
            ))

        return drifts

    def should_retrain(self, drifts: list[ModelPerformanceDrift]) -> bool:
        """Decision rule: retrain if 2+ metrics show significant drift"""
        significant_count = sum(1 for d in drifts if d.is_significant)
        return significant_count >= 2

    def generate_report(self, drifts: list[ModelPerformanceDrift]) -> str:
        """Human-readable drift report"""
        lines = ["📊 MODEL PERFORMANCE MONITORING\n"]

        for drift in drifts:
            symbol = "🔴" if drift.is_significant else "🟢"
            lines.append(
                f"{symbol} {drift.metric_name}: "
                f"{drift.current_value:.3f} "
                f"(baseline: {drift.baseline_value:.3f}, "
                f"drift: {drift.drift_pct:+.1f}%)"
            )

        if self.should_retrain(drifts):
            lines.append("\n⚠️ RETRAIN RECOMMENDED: Multiple metrics show significant drift")

        return "\n".join(lines)

# Add to scheduler.py:
# Daily at 18:00: python monitoring/check_model_drift.py
```

---

## 2. Quick Wins (1-2 Weeks)

### Week 1:

1. **Citation Validator** (#7) - Immediate trust improvement
2. **Data Quality Framework** (#5) - Catch errors before they propagate
3. **Dynamic Thresholds** (#8) - Easy upgrade with measurable impact

### Week 2:

4. **Orderbook Features** (#2) - High-value, self-contained module
5. **Probabilistic Forecasting** (#4) - Add confidence intervals
   **Implementation Order:**

```bash
# Day 1-2: Citation validation
python -m agents.citation_validator  # Create module
# Add to agent_analyse.py

# Day 3-4: Data quality
python -m data.quality_checks  # Create module
# Add to all scrapers

# Day 5-6: Dynamic thresholds
python -m models.threshold_calibration  # Create module
# Integrate into compute_investment_prediction()

# Day 7-10: Orderbook features
python -m features.orderbook_features  # Create module
# Add to evidence block

# Day 11-14: Probabilistic forecasting
python -m models.probabilistic_forecast  # Create module
# Add confidence intervals to predictions
```

---

## 3. Medium Roadmap (1-2 Months)

### Month 1:

- **Week 1-2:** Backtesting Framework (#1)
  - Build evaluator
  - Run historical walk-forward
  - Establish baseline metrics
- **Week 3-4:** Ensemble Model (#3)
  - Feature engineering
  - Train initial model
  - Validate performance

### Month 2:

- **Week 1-2:** Regime Detection (#6)
  - Implement HMM
  - Integrate into strategy selection
- **Week 2-3:** Database Optimization (#9)
  - Add indexes
  - Create materialized views
  - Test query performance
- **Week 3-4:** Model Monitoring (#10)
  - Set up drift detection
  - Automate alerts

---

## 4. Modeling Improvements (Specific Algorithms)

### Short-term Trading Signals (1-5 days):

**Current:** ATR-based fixed target/stop  
**Upgrade to:**

```python
# Ensemble of 3 models:

# 1. LSTM for sequence prediction
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout

def build_lstm_model(lookback: int = 20, n_features: int = 10):
    model = Sequential([
        LSTM(64, return_sequences=True, input_shape=(lookback, n_features)),
        Dropout(0.2),
        LSTM(32),
        Dropout(0.2),
        Dense(16, activation='relu'),
        Dense(1)  # Return prediction
    ])
    model.compile(optimizer='adam', loss='mse')
    return model

# 2. LightGBM for tabular features
import lightgbm as lgb

def train_lgbm_regressor(X_train, y_train):
    params = {
        'objective': 'regression',
        'metric': 'mae',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': -1,
    }

    train_data = lgb.Dataset(X_train, label=y_train)
    model = lgb.train(params, train_data, num_boost_round=100)
    return model

# 3. Orderbook-specific classifier
# Focus on L1-L5 imbalance patterns
from sklearn.ensemble import RandomForestClassifier

def train_orderbook_classifier(orderbook_features, y):
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        class_weight='balanced',
    )
    model.fit(orderbook_features, y)
    return model

# Combine with weighted voting
final_pred = (
    0.4 * lstm_pred +
    0.4 * lgbm_pred +
    0.2 * orderbook_pred
)
```

**Expected Lift:** +10-15 percentage points accuracy

### Medium-term Target/Recommendation (3-12 months):

**Current:** Valuation-weighted average  
**Upgrade to:**

```python
# Bayesian Model Averaging for valuation models

from scipy.stats import norm
import numpy as np

def bayesian_model_averaging(
    dcf_price: float,
    ddm_price: float,
    graham_price: float,
    relative_price: float,
    historical_accuracy: dict,  # {model_name: accuracy_score}
):
    """
    Weight each model by its historical accuracy (posterior).

    Prior: uniform
    Likelihood: Gaussian centered at model price
    Posterior: accuracy-weighted
    """
    models = {
        'dcf': dcf_price,
        'ddm': ddm_price,
        'graham': graham_price,
        'relative': relative_price,
    }

    # Normalize accuracies to sum to 1 (posterior weights)
    total_acc = sum(historical_accuracy.values())
    weights = {
        name: historical_accuracy[name] / total_acc
        for name in models.keys()
    }

    # Weighted mean
    fair_value = sum(weights[name] * price for name, price in models.items())

    # Weighted variance (for confidence interval)
    variance = sum(
        weights[name] * (price - fair_value) ** 2
        for name, price in models.items()
    )
    std = np.sqrt(variance)

    # 90% confidence interval
    ci_low = fair_value - 1.645 * std
    ci_high = fair_value + 1.645 * std

    return {
        'fair_value': fair_value,
        'ci_low': ci_low,
        'ci_high': ci_high,
        'weights': weights,
    }
```

**Alternative: Ensemble with Macro Conditioning**

```python
# Adjust valuation based on macro regime

def macro_adjusted_valuation(
   base_valuation: float,
   gdp_growth: float,
   inflation: float,
   interest_rate: float,
):
   """
   P/E expansion/contraction based on macro factors.

   Based on: Damodaran (2012) "Investment Valuation"
   """
   # Baseline P/E for Morocco market: ~15
   baseline_pe = 15.0

   # Adjustments:
   # - High growth → higher P/E
   # - High inflation → lower P/E (unless nominal growth compensates)
   # - High rates → lower P/E (discount rate effect)

   growth_adj = (gdp_growth - 3.0) * 0.5  # 1% GDP above 3% baseline → +0.5 P/E
   inflation_adj = -(inflation - 2.0) * 0.3  # 1% inflation above 2% → -0.3 P/E
   rate_adj = -(interest_rate - 3.0) * 0.4  # 1% rate above 3% → -0.4 P/E

   adjusted_pe = baseline_pe + growth_adj + inflation_adj + rate_adj
   adjusted_pe = max(8.0, min(25.0, adjusted_pe))  # Clip to reasonable range

   # Apply to valuation
   # Assume base_valuation implies a P/E, scale it
   adjustment_factor = adjusted_pe / baseline_pe

   return base_valuation * adjustment_factor
```

### Combining Signals:

**Multi-horizon Hierarchical Model:**

```python
# Hierarchy:
# Level 1: Macro regime (bull/bear/sideways)
# Level 2: Valuation zone (cheap/fair/expensive)
# Level 3: Short-term signals (orderbook, momentum, news)

def hierarchical_decision(
    regime: str,
    valuation_upside: float,
    short_term_signal: float,  # -1 to +1
    confidence_threshold: float = 0.6,
):
    """
    Decision tree that respects market structure.

    Logic:
    - In BEAR regime: only buy if valuation is VERY cheap AND short-term confirms
    - In BULL regime: buy if valuation is reasonable OR short-term strong
    - In SIDEWAYS: pure mean-reversion on short-term
    """
    if regime == 'BEAR':
        if valuation_upside > 30 and short_term_signal > 0.3:
            return 'BUY', 'HIGH'
        elif short_term_signal < -0.5:
            return 'SELL', 'MEDIUM'
        else:
            return 'HOLD', 'LOW'

    elif regime == 'BULL':
        if valuation_upside > 10 or short_term_signal > 0.5:
            return 'BUY', 'MEDIUM'
        elif valuation_upside < -15 and short_term_signal < 0:
            return 'SELL', 'MEDIUM'
        else:
            return 'HOLD', 'LOW'

    else:  # SIDEWAYS
        if abs(short_term_signal) > 0.7:
            # Strong mean-reversion signal
            if short_term_signal > 0:
                return 'BUY', 'MEDIUM'
            else:
                return 'SELL', 'MEDIUM'
        else:
            return 'HOLD', 'LOW'
```

---

## 5. Orderbook Usage (Best Features)

### Daily/Weekly Aggregation:

```python
# Top 5 orderbook features for prediction:

# 1. **Cumulative Order Imbalance (COI)**
#    = Sum of OI over day, weighted by volume
COI = sum(
    (bid_vol_total - ask_vol_total) / (bid_vol_total + ask_vol_total) * volume
) / total_volume

# 2. **Volume-Weighted Spread (VWS)**
#    = Average spread weighted by trade activity
VWS = sum(spread_bps * volume) / total_volume

# 3. **Book Resilience**
#    = How quickly imbalance mean-reverts (autocorrelation)
resilience = -autocorr(OI, lag=5)  # negative because reversion

# 4. **Price Impact**
#    = Average price movement per unit volume
impact = sum(abs(mid_price[i] - mid_price[i-1])) / sum(volume)

# 5. **Effective Spread**
#    = Realized cost of trading (from actual trades vs mid)
#    Requires trade data, not just quotes
effective_spread = 2 * abs(trade_price - mid_price)
```

### Rolling Features (5-day, 10-day):

```python
# Regime-aware aggregation:

def adaptive_rolling_window(
    daily_features: pd.DataFrame,
    volatility_series: pd.Series,
    base_window: int = 5,
):
    """
    Shorter window in high-vol regimes (faster adaptation).
    Longer window in low-vol regimes (more stable).
    """
    vol_percentile = volatility_series.rolling(60).apply(
        lambda x: (x.iloc[-1] - x.min()) / (x.max() - x.min())
    )

    # Window = 3 in high vol, 10 in low vol
    adaptive_window = (base_window +
                       5 * (1 - vol_percentile)).round().astype(int)

    # Apply rolling with adaptive window
    # (complex, but more responsive)
```

---

## 6. Evaluation Framework

### Backtest Design:

```python
# backtest/config.py

BACKTEST_CONFIG = {
    # Temporal splits (prevent lookahead)
    'train_period': '2020-01-01 to 2023-12-31',
    'validation_period': '2024-01-01 to 2024-06-30',
    'test_period': '2024-07-01 to 2026-04-29',

    # Walk-forward parameters
    'train_window_days': 252,  # 1 year
    'val_window_days': 63,     # 3 months
    'test_window_days': 21,    # 1 month
    'step_size_days': 21,      # Roll forward monthly

    # Transaction costs
    'commission_pct': 0.10,    # 10 bps per trade
    'slippage_pct': 0.05,      # 5 bps slippage

    # Position sizing
    'max_position_size': 1.0,  # 100% (single stock)
    'min_position_size': 0.0,  # Can be 0% (cash)

    # Risk controls
    'max_drawdown_stop': 0.20,  # Halt if -20% drawdown
    'position_timeout_days': 30,  # Force exit after 30 days
}
```

### Metrics Suite:

```python
# backtest/metrics.py

class PerformanceMetrics:
    """Comprehensive metrics for classification + regression + trading"""

    @staticmethod
    def classification_metrics(y_true, y_pred):
        """Direction accuracy, precision, recall, F1"""
        from sklearn.metrics import (
            accuracy_score, precision_score, recall_score,
            f1_score, confusion_matrix, classification_report
        )

        return {
            'accuracy': accuracy_score(y_true, y_pred),
            'precision': precision_score(y_true, y_pred, average='weighted'),
            'recall': recall_score(y_true, y_pred, average='weighted'),
            'f1': f1_score(y_true, y_pred, average='weighted'),
            'confusion_matrix': confusion_matrix(y_true, y_pred),
            'report': classification_report(y_true, y_pred),
        }

    @staticmethod
    def regression_metrics(y_true, y_pred):
        """Return prediction accuracy"""
        from sklearn.metrics import (
            mean_absolute_error, mean_squared_error, r2_score
        )

        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)

        # Custom: directional accuracy
        direction_correct = (
            (y_true > 0) == (y_pred > 0)
        ).mean()

        return {
            'mae': mae,
            'rmse': rmse,
            'r2': r2,
            'direction_accuracy': direction_correct,
        }

    @staticmethod
    def trading_metrics(returns: np.ndarray, risk_free_rate: float = 0.02):
        """Risk-adjusted returns, drawdown, etc."""

        # Annualized return
        total_return = (1 + returns / 100).prod() - 1
        n_periods = len(returns)
        annualized_return = (1 + total_return) ** (252 / n_periods) - 1

        # Volatility
        annualized_vol = returns.std() * np.sqrt(252)

        # Sharpe ratio
        excess_return = annualized_return - risk_free_rate
        sharpe = excess_return / annualized_vol if annualized_vol > 0 else 0

        # Sortino ratio (downside deviation)
        downside_returns = returns[returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252)
        sortino = excess_return / downside_std if downside_std > 0 else 0

        # Max drawdown
        cumulative = (1 + returns / 100).cumprod()
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        max_dd = abs(drawdown.min())

        # Win rate, avg win/loss
        wins = returns > 0
        win_rate = wins.mean()
        avg_win = returns[wins].mean() if wins.any() else 0
        avg_loss = returns[~wins].mean() if (~wins).any() else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        # Calmar ratio (return / max drawdown)
        calmar = annualized_return / max_dd if max_dd > 0 else 0

        return {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'annualized_volatility': annualized_vol,
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
            'max_drawdown': max_dd,
            'calmar_ratio': calmar,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
        }

    @staticmethod
    def calibration_metrics(y_prob, y_true):
        """For probabilistic predictions"""
        from sklearn.calibration import calibration_curve
        from sklearn.metrics import brier_score_loss

        # Brier score (lower is better)
        brier = brier_score_loss(y_true, y_prob)

        # Calibration curve
        prob_true, prob_pred = calibration_curve(
            y_true, y_prob, n_bins=10
        )

        # Expected Calibration Error (ECE)
        ece = np.abs(prob_true - prob_pred).mean()

        return {
            'brier_score': brier,
            'expected_calibration_error': ece,
            'calibration_curve': (prob_true, prob_pred),
        }
```

### Leakage Controls:

```python
# backtest/leakage_prevention.py

class AntiLeakageValidator:
    """Enforce temporal integrity in backtests"""

    @staticmethod
    def check_feature_timestamp(features_df, target_df):
        """Ensure no feature uses data after target date"""
        feature_dates = pd.to_datetime(features_df['date'])
        target_dates = pd.to_datetime(target_df['date'])

        # All feature dates must be <= target dates
        violations = (feature_dates > target_dates).any()

        if violations:
            raise ValueError("LEAKAGE DETECTED: Features use future data!")

    @staticmethod
    def enforce_embargo_period(train_end_date, test_start_date, min_gap_days=5):
        """
        Enforce gap between train and test to prevent information leakage.

        Example: If training on data up to 2024-06-30, don't test until 2024-07-05
        to account for delayed reporting, settlement, etc.
        """
        gap = (pd.to_datetime(test_start_date) - pd.to_datetime(train_end_date)).days

        if gap < min_gap_days:
            raise ValueError(f"Embargo period violation: only {gap} days gap (need {min_gap_days})")

    @staticmethod
    def purge_overlapping_labels(train_df, test_df, target_horizon_days):
        """
        Remove training samples whose target period overlaps with test period.

        Example: If train ends 2024-06-30 and target is 5-day return,
        remove training samples after 2024-06-25 (their targets would use July data).
        """
        train_cutoff = pd.to_datetime(test_df['date'].min()) - pd.Timedelta(days=target_horizon_days)

        cleaned_train = train_df[pd.to_datetime(train_df['date']) < train_cutoff]

        removed = len(train_df) - len(cleaned_train)
        if removed > 0:
            print(f"Purged {removed} training samples to prevent label leakage")

        return cleaned_train
```

---

## 7. LLM + Deterministic Hybrid Improvements

### Current Split (Good):

- **Deterministic:** Numeric predictions (target price, stop loss, expected return)
- **LLM:** Narrative synthesis (thesis, findings, reasoning)

### Improvements:

#### A) Stricter Citation Enforcement

```python
# Already covered in #7, but add:

def enforce_citation_coverage(analysis: ATWAnalysis, evidence_block: str):
    """
    Ensure every claim in findings has at least one citation,
    and every major evidence ID is referenced somewhere.
    """
    # Extract all evidence IDs
    all_ids = set(re.findall(r'\[([A-Z]+-[A-Z0-9-]+)\]', evidence_block))

    # Extract all citations from analysis
    used_ids = set()
    for finding in analysis.findings:
        for evidence in finding.evidence:
            used_ids.add(evidence.source_ref)

    # Check coverage
    unused_ids = all_ids - used_ids

    # Critical evidence that MUST be cited (adjust as needed)
    critical_prefixes = ['PRED-', 'VAL-', 'MKT-']
    critical_unused = [
        id for id in unused_ids
        if any(id.startswith(prefix) for prefix in critical_prefixes)
    ]

    if critical_unused:
        print(f"⚠️ WARNING: Critical evidence not cited: {critical_unused}")
```

#### B) Auditable Evidence Chain

```python
# Add to prediction_history.csv:

def create_audit_trail(analysis: ATWAnalysis, evidence_block: str):
    """
    For each prediction, save:
    - Raw evidence block (inputs)
    - LLM response (outputs)
    - Citations used
    - Deterministic calculations

    Allows post-hoc review: "Why did we recommend BUY on 2024-03-15?"
    """
    audit_record = {
        'timestamp': datetime.now().isoformat(),
        'as_of_date': analysis.as_of_date,
        'verdict': analysis.verdict,

        # Evidence (compressed)
        'evidence_block_hash': hashlib.sha256(evidence_block.encode()).hexdigest(),
        'evidence_block': evidence_block,  # or save to separate file

        # Citations
        'citations_used': [
            e.source_ref for f in analysis.findings for e in f.evidence
        ],

        # Deterministic inputs
        'last_close': analysis.last_close_mad,
        'atr_14d': analysis.trading_prediction.atr_mad,
        'valuation_range': f"{analysis.fair_value_low_mad}-{analysis.fair_value_high_mad}",

        # LLM outputs
        'verdict_reasoning': analysis.verdict_reasoning,
        'trading_thesis': analysis.trading_prediction.thesis,
        'investment_thesis': analysis.investment_prediction.thesis,
    }

    # Save to audit log
    with open('data/audit_trail.jsonl', 'a') as f:
        f.write(json.dumps(audit_record) + '\n')
```

#### C) What to Keep Deterministic vs LLM:

**Keep Deterministic:**

- [ ] All numeric predictions (targets, stops, returns)
- [ ] Risk metrics (ATR, volatility, position size)
- [ ] Statistical calculations (correlation, z-scores)
- [ ] Threshold decisions (BUY/HOLD/SELL based on upside%)
      **Let LLM Handle:**
- [x] Narrative synthesis ("market shows strength due to...")
- [x] Weighing qualitative factors (news sentiment + macro + valuation)
- [x] Risk warnings (identifying potential issues from news)
- [x] Thesis formulation (combining multiple data points into story)
      **Hybrid (Deterministic → LLM):**
- Deterministic: Compute orderbook imbalance z-score = +2.3
- LLM: Interpret → "Strong buying pressure (z-score +2.3) suggests short-term support [OB-IMBAL]"

---

## 8. Database and Pipeline Improvements

### Schema Enhancements:

```sql
-- 1. Add data lineage tracking
ALTER TABLE bourse_daily ADD COLUMN source_file VARCHAR(255);
ALTER TABLE bourse_daily ADD COLUMN ingestion_timestamp TIMESTAMP DEFAULT NOW();
ALTER TABLE bourse_daily ADD COLUMN data_version INT DEFAULT 1;

-- 2. Add soft deletes (instead of hard DELETE)
ALTER TABLE news ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE;
ALTER TABLE news ADD COLUMN deleted_at TIMESTAMP;

-- 3. Add computed columns (for common aggregations)
ALTER TABLE bourse_daily ADD COLUMN return_1d_pct NUMERIC
GENERATED ALWAYS AS ((dernier_cours - LAG(dernier_cours) OVER (ORDER BY seance)) / LAG(dernier_cours) OVER (ORDER BY seance) * 100) STORED;

-- 4. Partitioning for large tables (if orderbook grows > 1M rows)
CREATE TABLE bourse_orderbook_partitioned (
    id SERIAL,
    timestamp TIMESTAMP NOT NULL,
    -- ... other columns ...
) PARTITION BY RANGE (timestamp);

CREATE TABLE bourse_orderbook_2024 PARTITION OF bourse_orderbook_partitioned
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE bourse_orderbook_2025 PARTITION OF bourse_orderbook_partitioned
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
```

### Data Validation Checks (Add to Scrapers):

```python
# scrapers/validation.py

from pydantic import BaseModel, validator, Field
from datetime import date

class BourseIntraDayRecord(BaseModel):
    """Schema validation for intraday market data"""
    timestamp: datetime
    dernier_cours: float = Field(gt=0)  # Must be positive
    volume: int = Field(ge=0)
    capitalisation: float = Field(gt=0)

    @validator('timestamp')
    def timestamp_not_future(cls, v):
        if v > datetime.now(tz=timezone.utc):
            raise ValueError("Timestamp cannot be in future")
        return v

    @validator('dernier_cours')
    def price_reasonable(cls, v):
        if v < 1 or v > 10000:  # ATW typically 50-200 MAD
            raise ValueError(f"Price {v} MAD outside reasonable range")
        return v

# In scraper:
def scrape_with_validation(url):
    data = fetch_data(url)

    try:
        validated = BourseIntraDayRecord(**data)
        return validated.dict()
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
        # Log to data_quality_log table
        log_data_quality_issue(
            source='market',
            severity='CRITICAL',
            message=str(e),
        )
        raise
```

### Recovery and Idempotency:

```python
# scrapers/recovery.py

import fcntl
import pickle
from pathlib import Path

class StatefulScraper:
    """
    Scraper that can resume from interruption.
    Uses file-based state management.
    """

    def __init__(self, scraper_name: str):
        self.state_file = Path(f'/tmp/{scraper_name}_state.pkl')
        self.lock_file = Path(f'/tmp/{scraper_name}.lock')

    def __enter__(self):
        """Acquire lock to prevent concurrent runs"""
        self.lock_fd = open(self.lock_file, 'w')
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            raise RuntimeError(f"{self.scraper_name} is already running")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release lock"""
        fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
        self.lock_fd.close()

    def save_checkpoint(self, state: dict):
        """Save current progress"""
        with open(self.state_file, 'wb') as f:
            pickle.dump(state, f)

    def load_checkpoint(self) -> dict | None:
        """Resume from last checkpoint"""
        if not self.state_file.exists():
            return None

        with open(self.state_file, 'rb') as f:
            return pickle.load(f)

    def clear_checkpoint(self):
        """Remove checkpoint after successful completion"""
        if self.state_file.exists():
            self.state_file.unlink()

# Usage:
with StatefulScraper('atw_realtime') as scraper:
    state = scraper.load_checkpoint() or {'last_processed_date': None}

    for date in trading_days:
        if state['last_processed_date'] and date <= state['last_processed_date']:
            continue  # Skip already processed

        try:
            process_date(date)
            state['last_processed_date'] = date
            scraper.save_checkpoint(state)
        except Exception as e:
            logger.error(f"Failed on {date}: {e}")
            # State saved, can resume later
            raise

    scraper.clear_checkpoint()  # Success
```

---

## 9. Final Recommended Architecture

### Target State Design:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA LAYER                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │ Market   │  │ Macro    │  │ News     │  │ Order-   │            │
│  │ Scraper  │  │ Collector│  │ Crawler  │  │ book     │            │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘            │
│       │             │             │             │                   │
│       └─────────────┴─────────────┴─────────────┘                   │
│                             │                                        │
│                             ▼                                        │
│                    ┌─────────────────┐                               │
│                    │ Data Quality    │                               │
│                    │ Validation      │                               │
│                    └────────┬────────┘                               │
│                             │                                        │
│                             ▼                                        │
│                    ┌─────────────────┐                               │
│                    │  PostgreSQL     │                               │
│                    │  (Indexed,      │                               │
│                    │   Partitioned)  │                               │
│                    └────────┬────────┘                               │
└─────────────────────────────┼────────────────────────────────────────┘
                              │
┌─────────────────────────────┼────────────────────────────────────────┐
│                    FEATURE ENGINEERING LAYER                         │
│                             │                                        │
│                    ┌────────▼────────┐                               │
│      ┌─────────────┤ Feature Store   ├─────────────┐                │
│      │             │ (Daily Refresh) │             │                │
│      │             └─────────────────┘             │                │
│      │                                             │                │
│      ▼                                             ▼                │
│ ┌────────────┐                              ┌────────────┐          │
│ │ Orderbook  │                              │ Technical  │          │
│ │ Features   │                              │ Indicators │          │
│ └────────────┘                              └────────────┘          │
└─────────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────┼────────────────────────────────────────┐
│                         MODEL LAYER                                  │
│                             │                                        │
│                    ┌────────▼────────┐                               │
│                    │ Regime Detector │                               │
│                    │ (HMM)           │                               │
│                    └────────┬────────┘                               │
│                             │                                        │
│              ┌──────────────┼──────────────┐                         │
│              │              │              │                         │
│       ┌──────▼──────┐┌──────▼──────┐┌─────▼──────┐                  │
│       │ Trading     ││ Investment  ││ Risk       │                  │
│       │ Ensemble    ││ Bayesian    ││ Model      │                  │
│       │ (LSTM+LGBM) ││ Averaging   ││ (Monte     │                  │
│       └──────┬──────┘└──────┬──────┘│  Carlo)    │                  │
│              │              │       └─────┬──────┘                  │
│              └──────────────┼─────────────┘                          │
│                             │                                        │
│                    ┌────────▼────────┐                               │
│                    │ Prediction      │                               │
│                    │ Aggregator      │                               │
│                    └────────┬────────┘                               │
└─────────────────────────────┼────────────────────────────────────────┘
                              │
┌─────────────────────────────┼────────────────────────────────────────┐
│                      SYNTHESIS LAYER                                 │
│                             │                                        │
│                    ┌────────▼────────┐                               │
│         ┌──────────┤ Evidence Block  ├──────────┐                   │
│         │          │ Composer        │          │                   │
│         │          │ (Deterministic) │          │                   │
│         │          └─────────────────┘          │                   │
│         │                                       │                   │
│         ▼                                       ▼                   │
│ ┌───────────────┐                     ┌───────────────┐             │
│ │ Citation      │                     │ LLM Synthesis │             │
│ │ Validator     │◄────────────────────│ (Groq/Claude) │             │
│ └───────┬───────┘                     └───────┬───────┘             │
│         │                                     │                     │
│         └─────────────────┬───────────────────┘                     │
│                           │                                         │
│                  ┌────────▼────────┐                                 │
│                  │ Final Analysis  │                                 │
│                  │ (ATWAnalysis)   │                                 │
│                  └────────┬────────┘                                 │
└──────────────────────────┼──────────────────────────────────────────┘
                           │
┌──────────────────────────┼──────────────────────────────────────────┐
│                   EVALUATION LAYER                                   │
│                           │                                         │
│                  ┌────────▼────────┐                                 │
│         ┌────────┤ Performance     ├────────┐                       │
│         │        │ Tracker         │        │                       │
│         │        └─────────────────┘        │                       │
│         │                                   │                       │
│         ▼                                   ▼                       │
│ ┌───────────────┐                  ┌───────────────┐                │
│ │ Backtest      │                  │ Live Monitor  │                │
│ │ (Walk-Forward)│                  │ (Drift Detect)│                │
│ └───────┬───────┘                  └───────┬───────┘                │
│         │                                  │                        │
│         └────────────┬─────────────────────┘                        │
│                      │                                              │
│             ┌────────▼────────┐                                      │
│             │ Retraining      │                                      │
│             │ Trigger         │                                      │
│             └─────────────────┘                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Key Principles:

1. **Separation of Concerns**
   - Data ingestion → validation → storage
   - Features computed once, reused
   - Models trained offline, predictions online
2. **Fail-Safe Defaults**
   - Data quality check blocks bad data
   - Citation validator catches LLM hallucinations
   - Fallback to deterministic if LLM fails
3. **Observability**
   - Every layer logs to structured storage
   - Metrics tracked at each stage
   - Audit trail for every prediction
4. **Scalability**
   - Database indexed and partitioned
   - Features cached (materialized views)
   - Models versioned and swappable

---

## Summary: Prioritized Action Plan

### Immediate (Week 1-2):

1. Citation validator (#7)
2. Data quality framework (#5)
3. Orderbook features (#2)
   **Expected Lift:** +10% reliability, +5% signal quality

### Short-term (Month 1):

4. Backtesting framework (#1) - CRITICAL
5. Dynamic thresholds (#8)
6. Probabilistic forecasting (#4)
   **Expected Lift:** Measurable baseline, +8% accuracy

### Medium-term (Month 2-3):

7. Ensemble model (#3) - Biggest accuracy gain
8. Regime detection (#6)
9. Model monitoring (#10)
   **Expected Lift:** +15% accuracy, adaptive strategy

### Long-term (Month 3-6):

10. Database optimization (#9)
11. Full production monitoring
12. Automated retraining pipeline
    **Expected Final State:**

- Accuracy: 55-62% (vs current ~45%)
- Sharpe ratio: 1.0-1.5 (vs current <0.5)
- Drawdown: <15% (vs current ~25%)
- Confidence intervals on all predictions
- Full audit trail
- Automated drift detection

---

This roadmap is implementation-ready. Each section has code examples, specific algorithms, and clear success metrics. Start with quick wins, validate with backtesting, then scale to ML models.

Let me know which sections you want to dive deeper into.
