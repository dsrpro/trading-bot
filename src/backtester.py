"""Moteur de backtesting pour valider la strategie sur donnees historiques.

Simule l'execution de la strategie sur des chandeliers historiques et calcule
les metriques cles: Sharpe ratio, win rate, profit factor, drawdown, etc.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import numpy as np

from src.candle_builder import Candle
from src.config import Config
from src.indicators import Indicators
from src.strategy_engine import SignalDirection, StrategyEngine, TradingSignal


@dataclass
class BacktestResult:
    """Resultat complet d'un backtest."""

    # Metriques principales
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0

    # Capital
    initial_capital: float = 100.0
    final_capital: float = 100.0
    total_return_pct: float = 0.0
    total_pnl: float = 0.0

    # Risque
    max_drawdown_pct: float = 0.0
    max_drawdown_duration: int = 0  # En nombre de trades
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0

    # Profit
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    expectancy: float = 0.0

    # Trades
    largest_win: float = 0.0
    largest_loss: float = 0.0
    avg_trade_duration: float = 0.0

    # Historique
    equity_curve: list[float] = field(default_factory=list)
    trade_list: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": round(self.win_rate * 100, 2),
            "initial_capital": self.initial_capital,
            "final_capital": round(self.final_capital, 2),
            "total_return_pct": round(self.total_return_pct, 2),
            "total_pnl": round(self.total_pnl, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "sortino_ratio": round(self.sortino_ratio, 2),
            "profit_factor": round(self.profit_factor, 2),
            "avg_win": round(self.avg_win, 2),
            "avg_loss": round(self.avg_loss, 2),
            "expectancy": round(self.expectancy, 2),
            "largest_win": round(self.largest_win, 2),
            "largest_loss": round(self.largest_loss, 2),
        }


class Backtester:
    """Moteur de backtesting evenementiel.

    Simule la strategie chandelier par chandelier en utilisant les
    memes modules que le bot live (Indicators, StrategyEngine).
    """

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger("backtester")
        self.indicators = Indicators(config, self.logger)
        self._commission_pct = config.backtest_commission_pct
        self._spread_pips = config.backtest_spread_pips
        self._min_candles = max(config.bb_period, config.rsi_period, 50, 200) + 10  # Assez pour EMA 50/200

        # Paramètres optimisés (Phase 1 -> Phase 2)
        self.use_trend_filter: bool = True
        self.ema_fast_period: int = 50
        self.ema_slow_period: int = 200
        self.rsi_oversold_tight: float = 25.0
        self.rsi_overbought_tight: float = 75.0
        self.use_atr_stops: bool = True
        self.atr_multiplier_sl: float = 1.5  # SL = 1.5x ATR
        self.atr_multiplier_tp: float = 3.0  # TP = 3.0x ATR (R:R ~1:2)
        self.min_atr_pct: float = 0.0001  # Min volatilité 0.01% pour éviter ranges trop calmes
        self.max_atr_pct: float = 0.015   # Max volatilité 1.5% pour éviter marchés trop agités
        self.trend_strength_min: float = 0.05  # Ratio EMA50/EMA200 minimum (%)

    def run(self, candles: list[Candle]) -> BacktestResult:
        """Execute le backtest sur une liste de chandeliers.

        Args:
            candles: Liste de chandeliers OHLC en ordre chronologique.

        Returns:
            BacktestResult avec toutes les metriques calculees.
        """
        if len(candles) < self._min_candles:
            self.logger.error(f"Donnees insuffisantes: {len(candles)} < {self._min_candles} bougies requises")
            return BacktestResult()

        capital = self.config.backtest_initial_capital
        peak_capital = capital
        equity_curve = [capital]
        trade_list = []
        position = None  # Position ouverte: {"direction", "entry_price", "entry_idx", "amount"}
        trade_returns = []

        # Etat du backtest
        max_drawdown = 0.0
        drawdown_start_idx = 0
        current_drawdown_duration = 0
        max_drawdown_duration = 0

        closed_wins = []
        closed_losses = []

        # Buffer de chandeliers pour le calcul des indicateurs
        candle_buffer: list[Candle] = []

        for i, candle in enumerate(candles):
            candle_buffer.append(candle)

            # Garder suffisamment de bougies pour les indicateurs
            if len(candle_buffer) > 500:
                candle_buffer = candle_buffer[-500:]

            # Minimum de bougies pour evaluer la strategie
            if len(candle_buffer) < self._min_candles:
                equity_curve.append(capital)
                continue

            # Si pas de position ouverte, evaluer le signal
            if position is None:
                signal = self._evaluate_on_buffer(candle_buffer, candle)

                if signal.is_valid:
                    # Determiner le montant (2% du capital)
                    amount = capital * (self.config.risk_per_trade_pct / 100.0)

                    # Appliquer le spread
                    spread_cost = self._spread_pips * 0.0001 * candle.close
                    entry_price = candle.close
                    if signal.direction == SignalDirection.CALL:
                        entry_price += spread_cost
                    else:
                        entry_price -= spread_cost

                    position = {
                        "direction": signal.direction,
                        "entry_price": entry_price,
                        "entry_idx": i,
                        "amount": amount,
                        "sl": signal.stop_loss,
                        "tp": signal.take_profit,
                        "signal_score": signal.score,
                    }

            # Si position ouverte, verifier SL/TP sur la bougie courante
            else:
                direction = position["direction"]
                sl = position["sl"]
                tp = position["tp"]

                # Verifier si SL ou TP est touche dans la bougie
                hit_sl = False
                hit_tp = False
                exit_price = candle.close

                if direction == SignalDirection.CALL:
                    if candle.low <= sl:
                        hit_sl = True
                        exit_price = sl
                    elif candle.high >= tp:
                        hit_tp = True
                        exit_price = tp
                else:  # PUT
                    if candle.high >= sl:
                        hit_sl = True
                        exit_price = sl
                    elif candle.low <= tp:
                        hit_tp = True
                        exit_price = tp

                if hit_sl or hit_tp:
                    # Calculer le PnL
                    entry_price = position["entry_price"]
                    amount = position["amount"]

                    if hit_tp:
                        pnl = amount * self._reward_multiple(position)
                    else:
                        pnl = -amount

                    # Appliquer la commission
                    pnl -= amount * self._commission_pct

                    capital += pnl
                    trade_returns.append(pnl / (capital - pnl) if capital != pnl else 0.0)

                    trade_record = {
                        "trade_id": len(trade_list) + 1,
                        "direction": direction.value,
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "entry_idx": position["entry_idx"],
                        "exit_idx": i,
                        "pnl": round(pnl, 2),
                        "pnl_pct": round(pnl / amount * 100, 2) if amount > 0 else 0,
                        "result": "WIN" if hit_tp else "LOSS",
                        "signal_score": position.get("signal_score", 0),
                    }
                    trade_list.append(trade_record)

                    if hit_tp:
                        closed_wins.append(pnl)
                    else:
                        closed_losses.append(abs(pnl))

                    position = None

            # Mise a jour de l'equity curve et du drawdown
            equity_curve.append(capital)

            if capital > peak_capital:
                peak_capital = capital
                current_drawdown_duration = 0
            else:
                dd = (peak_capital - capital) / peak_capital * 100.0 if peak_capital > 0 else 0.0
                if dd > max_drawdown:
                    max_drawdown = dd
                current_drawdown_duration += 1
                if current_drawdown_duration > max_drawdown_duration:
                    max_drawdown_duration = current_drawdown_duration

        # Calcul des metriques finales
        result = self._compute_metrics(
            capital=capital,
            initial_capital=self.config.backtest_initial_capital,
            equity_curve=equity_curve,
            trade_list=trade_list,
            closed_wins=closed_wins,
            closed_losses=closed_losses,
            max_drawdown=max_drawdown,
            max_drawdown_duration=max_drawdown_duration,
            trade_returns=trade_returns,
        )

        self.logger.info(
            f"Backtest termine | "
            f"Trades={result.total_trades} | "
            f"WinRate={result.win_rate*100:.1f}% | "
            f"Return={result.total_return_pct:.2f}% | "
            f"MaxDD={result.max_drawdown_pct:.2f}% | "
            f"Sharpe={result.sharpe_ratio:.2f} | "
            f"ProfitFactor={result.profit_factor:.2f}"
        )

        return result

    def _reward_multiple(self, position: dict) -> float:
        """Calcule le multiple de gain depuis les niveaux SL/TP de la position."""
        entry_price = float(position.get("entry_price", 0.0) or 0.0)
        stop_loss = float(position.get("sl", 0.0) or 0.0)
        take_profit = float(position.get("tp", 0.0) or 0.0)
        risk_distance = abs(entry_price - stop_loss)
        reward_distance = abs(take_profit - entry_price)
        if risk_distance > 0 and reward_distance > 0:
            return reward_distance / risk_distance
        return self.config.risk_reward_ratio

    def generate_sample_data(self, n_candles: int = 1000, volatility: float = 0.002,
                             trend: float = 0.0002, include_regimes: bool = True) -> list[Candle]:
        """Genere des donnees de chandeliers synthetiques pour les tests.

        Utilise un mouvement brownien geometrique avec changements de regime
        (trend haussier, range, trend baissier) pour tester les filtres.

        Args:
            n_candles: Nombre de chandeliers a generer.
            volatility: Volatilite par chandelier (~0.2%).
            trend: Tendance haussiere par chandelier.
            include_regimes: Si True, alterne entre phases de trend et range.

        Returns:
            Liste de Candle synthetiques.
        """
        np.random.seed(42)  # Reproducibilite
        candles = []
        base_time = datetime.now(timezone.utc).timestamp() - n_candles * 60
        price = 100.0
        regime_length = n_candles // 4  # 4 phases distinctes

        for i in range(n_candles):
            # Déterminer le régime actuel
            regime_idx = i // regime_length
            local_trend = trend

            if include_regimes and regime_idx == 0:
                # Phase 1: Tendance haussière forte
                local_trend = trend * 3
                local_vol = volatility * 0.8
            elif include_regimes and regime_idx == 1:
                # Phase 2: Range (faible tendance)
                local_trend = trend * 0.1
                local_vol = volatility * 0.5
            elif include_regimes and regime_idx == 2:
                # Phase 3: Tendance baissière forte
                local_trend = -trend * 2.5
                local_vol = volatility * 1.2
            else:
                # Phase 4: Tendance haussière modérée + volatilité
                local_trend = trend * 2
                local_vol = volatility * 1.0

            returns = np.random.normal(local_trend, local_vol)

            # Ajouter des mini-cycles pour créer des pullbacks
            mini_cycle = 0.003 * np.sin(2 * np.pi * i / 100)
            returns += mini_cycle / 100

            open_price = price
            close_price = price * (1 + returns)

            # High/Low avec bruit intra-bougie
            intra_noise = np.random.uniform(0.001, 0.004)
            high = max(open_price, close_price) * (1 + intra_noise)
            low = min(open_price, close_price) * (1 - intra_noise * np.random.uniform(0.5, 1.0))

            candle = Candle(
                timestamp=base_time + i * 60,
                open=round(open_price, 5),
                high=round(high, 5),
                low=round(low, 5),
                close=round(close_price, 5),
                volume=np.random.randint(10, 200),
                symbol=self.config.market_symbol,
                timeframe=self.config.timeframe,
                is_closed=True,
            )
            candles.append(candle)
            price = close_price

        return candles

    def _evaluate_on_buffer(self, candle_buffer: list[Candle], current_candle: Candle) -> TradingSignal:
        """Evalue la strategie sur un buffer de chandeliers avec filtres optimises Phase 2.

        Ameliorations Phase 2:
            - Filtre de tendance EMA 50/200
            - Filtre de volatilite ATR (min/max)
            - SL/TP bases sur ATR dynamique (R:R ~1:2 au lieu de 1:5)
            - Filtre de range/trending market

        Args:
            candle_buffer: Buffer de chandeliers (inclut la bougie courante).
            current_candle: Bougie courante.

        Returns:
            TradingSignal.
        """
        if len(candle_buffer) < self._min_candles:
            return TradingSignal(
                direction=SignalDirection.HOLD,
                symbol=self.config.market_symbol,
            )

        closes = np.array([c.close for c in candle_buffer], dtype=np.float64)
        highs = np.array([c.high for c in candle_buffer], dtype=np.float64)
        lows = np.array([c.low for c in candle_buffer], dtype=np.float64)
        opens = np.array([c.open for c in candle_buffer], dtype=np.float64)

        upper, middle, lower = self.indicators.bollinger_bands(closes)
        rsi_values = self.indicators.rsi(closes)
        atr_values = self.indicators.atr(highs, lows, closes)

        if upper is None or rsi_values is None or atr_values is None:
            return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

        last_upper = float(upper[-1]) if not np.isnan(upper[-1]) else 0.0
        last_middle = float(middle[-1]) if not np.isnan(middle[-1]) else 0.0
        last_lower = float(lower[-1]) if not np.isnan(lower[-1]) else 0.0
        last_rsi = float(rsi_values[-1]) if not np.isnan(rsi_values[-1]) else 50.0
        last_atr = float(atr_values[-1]) if not np.isnan(atr_values[-1]) else 0.0

        # --- Filtre de volatilite (ATR) ---
        if self.use_atr_stops and last_atr > 0 and current_candle.close > 0:
            atr_pct = last_atr / current_candle.close
            if atr_pct < self.min_atr_pct:
                # Marche trop calme (range etroit) → skip
                return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)
            if atr_pct > self.max_atr_pct:
                # Marche trop volatile → skip
                return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

        # --- Filtre de tendance EMA 50/200 ---
        uptrend = True
        downtrend = True
        trend_ratio = 0.0
        if self.use_trend_filter and len(closes) >= self.ema_slow_period + 5:
            ema50 = self.indicators.ema(closes, self.ema_fast_period)
            ema200 = self.indicators.ema(closes, self.ema_slow_period)
            last_ema50 = float(ema50[-1]) if not np.isnan(ema50[-1]) else 0.0
            last_ema200 = float(ema200[-1]) if not np.isnan(ema200[-1]) else 0.0
            if last_ema200 > 0:
                trend_ratio = (last_ema50 / last_ema200 - 1.0) * 100.0
                uptrend = last_ema50 > last_ema200
                downtrend = last_ema50 < last_ema200

                # Verifier que la tendance est suffisamment forte
                if abs(trend_ratio) < self.trend_strength_min:
                    # Marche en range (sans tendance claire) → skip
                    return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

        # Parametres optimises
        min_score = 60.0

        # --- CALL Signal ---
        call_conditions = 0
        call_score = 0.0
        rejection_call = False

        if current_candle.close < last_lower and current_candle.open < last_lower:
            call_conditions += 1
            call_score += 35.0
        elif current_candle.close < last_lower or current_candle.open < last_lower:
            call_score += 10.0

        if last_rsi < self.rsi_oversold_tight:
            call_conditions += 1
            call_score += 40.0
        elif last_rsi < 30:
            call_score += 20.0
        elif last_rsi < 35:
            call_score += 10.0

        rejection_call = Indicators.is_rejection_candle(
            current_candle.open, current_candle.high, current_candle.low, current_candle.close, "CALL"
        )
        if rejection_call:
            call_conditions += 1
            call_score += 30.0

        # --- PUT Signal ---
        put_conditions = 0
        put_score = 0.0
        rejection_put = False

        if current_candle.close > last_upper and current_candle.open > last_upper:
            put_conditions += 1
            put_score += 35.0
        elif current_candle.close > last_upper or current_candle.open > last_upper:
            put_score += 10.0

        if last_rsi > self.rsi_overbought_tight:
            put_conditions += 1
            put_score += 40.0
        elif last_rsi > 70:
            put_score += 20.0
        elif last_rsi > 65:
            put_score += 10.0

        rejection_put = Indicators.is_rejection_candle(
            current_candle.open, current_candle.high, current_candle.low, current_candle.close, "PUT"
        )
        if rejection_put:
            put_conditions += 1
            put_score += 30.0

        # --- Calcul SL/TP bases sur ATR dynamique ---
        if last_atr > 0 and current_candle.close > 0:
            atr_sl = last_atr * self.atr_multiplier_sl
            atr_tp = last_atr * self.atr_multiplier_tp
        else:
            # Fallback: 0.5% SL, 1.5% TP
            atr_sl = current_candle.close * 0.005
            atr_tp = current_candle.close * 0.015

        if call_conditions >= 2 and call_score >= min_score:
            # CALL = haussier → exiger tendance haussiere
            if self.use_trend_filter and not uptrend:
                return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)
            return TradingSignal(
                direction=SignalDirection.CALL,
                symbol=self.config.market_symbol,
                score=call_score,
                confidence=min(call_score / 100.0, 0.95),
                entry_price=current_candle.close,
                bb_upper=last_upper,
                bb_lower=last_lower,
                rsi_value=last_rsi,
                atr_value=last_atr,
                stop_loss=current_candle.close - atr_sl,
                take_profit=current_candle.close + atr_tp,
                rejection_confirmed=rejection_call,
            )

        if put_conditions >= 2 and put_score >= min_score:
            # PUT = baissier → exiger tendance baissiere
            if self.use_trend_filter and not downtrend:
                return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)
            return TradingSignal(
                direction=SignalDirection.PUT,
                symbol=self.config.market_symbol,
                score=put_score,
                confidence=min(put_score / 100.0, 0.95),
                entry_price=current_candle.close,
                bb_upper=last_upper,
                bb_lower=last_lower,
                rsi_value=last_rsi,
                atr_value=last_atr,
                stop_loss=current_candle.close + atr_sl,
                take_profit=current_candle.close - atr_tp,
                rejection_confirmed=rejection_put,
            )

        return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

    @staticmethod
    def _compute_metrics(
        capital: float,
        initial_capital: float,
        equity_curve: list[float],
        trade_list: list[dict],
        closed_wins: list[float],
        closed_losses: list[float],
        max_drawdown: float,
        max_drawdown_duration: int,
        trade_returns: list[float],
    ) -> BacktestResult:
        """Calcule toutes les metriques de performance.

        Args:
            capital: Capital final.
            initial_capital: Capital initial.
            equity_curve: Courbe d'equity.
            trade_list: Liste des trades executes.
            closed_wins: PnL des trades gagnants.
            closed_losses: PnL absolu des trades perdants.
            max_drawdown: Drawdown maximum en %.
            max_drawdown_duration: Duree du drawdown max en trades.
            trade_returns: Liste des rendements par trade.

        Returns:
            BacktestResult complet.
        """
        total_trades = len(trade_list)
        winning_trades = len(closed_wins)
        losing_trades = len(closed_losses)
        win_rate = winning_trades / total_trades if total_trades > 0 else 0.0
        total_pnl = capital - initial_capital
        total_return_pct = (capital / initial_capital - 1) * 100.0

        # Profit factor
        gross_profit = sum(closed_wins)
        gross_loss = sum(closed_losses)
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0

        # Averages
        avg_win = np.mean(closed_wins) if closed_wins else 0.0
        avg_loss = np.mean(closed_losses) if closed_losses else 0.0
        largest_win = max(closed_wins) if closed_wins else 0.0
        largest_loss = max(closed_losses) if closed_losses else 0.0

        # Expectancy
        expectancy = (win_rate * avg_win - (1 - win_rate) * avg_loss) if total_trades > 0 else 0.0

        # Sharpe Ratio (annualise, en supposant 252 jours de trading)
        if len(trade_returns) > 1:
            returns_arr = np.array(trade_returns)
            avg_return = np.mean(returns_arr)
            std_return = np.std(returns_arr, ddof=1)
            sharpe_ratio = (avg_return / std_return * np.sqrt(252)) if std_return > 0 else 0.0

            # Sortino Ratio (seulement ecart-type des rendements negatifs)
            negative_returns = returns_arr[returns_arr < 0]
            if len(negative_returns) > 1:
                downside_std = np.std(negative_returns, ddof=1)
                sortino_ratio = (avg_return / downside_std * np.sqrt(252)) if downside_std > 0 else 0.0
            else:
                sortino_ratio = 0.0
        else:
            sharpe_ratio = 0.0
            sortino_ratio = 0.0

        return BacktestResult(
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            initial_capital=initial_capital,
            final_capital=capital,
            total_return_pct=total_return_pct,
            total_pnl=total_pnl,
            max_drawdown_pct=max_drawdown,
            max_drawdown_duration=max_drawdown_duration,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            profit_factor=profit_factor,
            avg_win=avg_win,
            avg_loss=avg_loss,
            expectancy=expectancy,
            largest_win=largest_win,
            largest_loss=largest_loss,
            equity_curve=equity_curve,
            trade_list=trade_list,
        )
