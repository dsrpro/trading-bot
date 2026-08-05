"""Script dédié au Paper Trading Phase 2.

Fonctionnalités Phase 2:
    - Connexion API Deriv (endpoint public + OTP trading)
    - Filtres avancés: EMA 50/200, ATR min/max, range/trend detection
    - SL/TP dynamiques basés ATR (R:R ~1:2)
    - Trailing stop adaptatif
    - Cooldown entre les trades (3 bougies minimum)
    - Logging enrichi pour analyse post-session
    - Dashboard console temps réel
    - Sauvegarde automatique des résultats

Usage:
    python -m src.paper_trading_phase2
    python -m src.paper_trading_phase2 --symbol R_100
    python -m src.paper_trading_phase2 --duration 120
"""

from __future__ import annotations

import asyncio
import argparse
import json
import logging
import signal
import sys
import time as _time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from src.candle_builder import Candle, CandleBuilder
from src.config import Config, load_config
from src.data_streamer import DataStreamer, Tick
from src.deriv_client import DerivClient
from src.indicators import Indicators
from src.logger import setup_logger, log_trade, log_signal, log_error
from src.order_executor import Order, OrderExecutor, OrderStatus
from src.risk_manager import RiskManager, RiskStatus
from src.strategy_engine import SignalDirection, TradingSignal


# ─── Configuration Phase 2 ───────────────────────────────────────────

@dataclass
class Phase2Config:
    """Paramètres spécifiques à la Phase 2."""
    # Filtres
    use_trend_filter: bool = True
    ema_fast: int = 50
    ema_slow: int = 200
    trend_strength_min_pct: float = 0.15  # Ratio EMA50/EMA200 minimum (%)

    # Volatilité
    use_volatility_filter: bool = True
    min_atr_pct: float = 0.0003  # 0.03% minimum
    max_atr_pct: float = 0.008   # 0.8% maximum

    # SL/TP ATR dynamiques
    use_atr_stops: bool = True
    atr_sl_mult: float = 1.5     # SL = 1.5x ATR
    atr_tp_mult: float = 3.0     # TP = 3.0x ATR (R:R = 1:2)

    # Trailing stop
    use_trailing_stop: bool = True
    trailing_activation_pct: float = 0.5  # Activer quand le prix a bougé de 50% vers TP
    trailing_distance_atr_mult: float = 1.0  # Distance du trailing = 1.0x ATR

    # Cooldown
    signal_cooldown_candles: int = 3  # Minimum 3 bougies entre 2 signaux

    # Session
    max_session_duration_min: int = 120  # Max 2h par session
    session_profit_target_pct: float = 4.0  # Objectif 4% par jour
    session_loss_limit_pct: float = 5.0     # Stop 5% par jour

    # Logging & sauvegarde
    save_trades: bool = True
    save_ticks: bool = False  # Trop volumineux pour le long terme
    results_dir: str = "data/phase2_results"


class TrailingStop:
    """Gestionnaire de trailing stop adaptatif.

    Le trailing stop s'active quand le prix a évolué favorablement
    d'au moins X% du chemin vers le TP, puis suit le prix à une
    distance fixe (multiple d'ATR).
    """

    def __init__(self, activation_pct: float = 0.5, distance_atr_mult: float = 1.0):
        self.activation_pct = activation_pct
        self.distance_atr_mult = distance_atr_mult
        self._active = False
        self._current_stop: float = 0.0
        self._best_price: float = 0.0  # Meilleur prix atteint (haussier=baisse, baissier=hausse)

    @property
    def is_active(self) -> bool:
        return self._active

    def initialize(self, entry_price: float, original_sl: float, take_profit: float,
                   direction: SignalDirection, atr_value: float) -> None:
        """Initialise le trailing stop pour un nouveau trade."""
        self._active = False
        self._best_price = entry_price
        self._current_stop = original_sl
        self._entry_price = entry_price
        self._take_profit = take_profit
        self._direction = direction
        self._atr = atr_value
        self._activation_distance = abs(take_profit - entry_price) * self.activation_pct
        self._trail_distance = atr_value * self.distance_atr_mult

    def update(self, current_price: float) -> Optional[float]:
        """Met à jour le trailing stop avec le prix actuel.

        Args:
            current_price: Prix actuel du marché.

        Returns:
            Nouveau niveau de SL si le trailing a bougé, None sinon.
        """
        if self._direction == SignalDirection.CALL:
            # Pour un CALL (haussier), le trailing monte
            if current_price > self._best_price:
                self._best_price = current_price

                # Vérifier l'activation
                if not self._active:
                    progress = abs(self._best_price - self._entry_price)
                    if progress >= self._activation_distance:
                        self._active = True

                # Si actif, ajuster le stop
                if self._active:
                    new_stop = self._best_price - self._trail_distance
                    if new_stop > self._current_stop:
                        self._current_stop = new_stop
                        return self._current_stop

        else:  # PUT
            # Pour un PUT (baissier), le trailing descend
            if current_price < self._best_price:
                self._best_price = current_price

                if not self._active:
                    progress = abs(self._best_price - self._entry_price)
                    if progress >= self._activation_distance:
                        self._active = True

                if self._active:
                    new_stop = self._best_price + self._trail_distance
                    if new_stop < self._current_stop:
                        self._current_stop = new_stop
                        return self._current_stop

        return None


class MarketRegime:
    """Détection du régime de marché (trend / range / volatile)."""

    @staticmethod
    def detect(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray,
               ema50: np.ndarray, ema200: np.ndarray, atr_values: np.ndarray) -> dict:
        """Analyse le régime de marché actuel.

        Returns:
            Dict avec:
                - regime: "trending_up", "trending_down", "ranging", "volatile"
                - trend_strength: force de la tendance (ratio EMA)
                - volatility_pct: volatilité ATR relative
                - bollinger_squeeze: True si les bandes se resserrent
        """
        if len(closes) < 200 or len(ema50) < 2 or len(ema200) < 2:
            return {"regime": "unknown", "trend_strength": 0, "volatility_pct": 0}

        last_ema50 = float(ema50[-1]) if not np.isnan(ema50[-1]) else 0
        last_ema200 = float(ema200[-1]) if not np.isnan(ema200[-1]) else 0
        last_price = float(closes[-1])
        last_atr = float(atr_values[-1]) if not np.isnan(atr_values[-1]) else 0

        trend_strength = 0.0
        regime = "ranging"

        if last_ema200 > 0 and last_ema50 > 0:
            trend_strength = (last_ema50 / last_ema200 - 1.0) * 100.0

            if trend_strength > 0.5:
                regime = "trending_up"
            elif trend_strength < -0.5:
                regime = "trending_down"
            else:
                regime = "ranging"

        volatility_pct = (last_atr / last_price * 100.0) if last_price > 0 else 0

        if volatility_pct > 0.3:
            regime = "volatile"

        return {
            "regime": regime,
            "trend_strength": round(trend_strength, 2),
            "volatility_pct": round(volatility_pct, 3),
        }


# ─── Paper Trading Engine Phase 2 ────────────────────────────────────

class PaperTradingPhase2:
    """Moteur de paper trading pour la Phase 2.

    Inclut tous les filtres avancés, trailing stop, et logging enrichi.
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logger(config, "paper_phase2")

        # Config Phase 2
        self.ph2 = Phase2Config()

        # Modules
        self.data_streamer = DataStreamer(config, self.logger)
        self.candle_builder = CandleBuilder(config, self.logger)
        self.indicators = Indicators(config, self.logger)
        self.risk_manager = RiskManager(config, self.logger)
        self.order_executor = OrderExecutor(config, logger=self.logger)
        self.deriv_client: Optional[DerivClient] = None

        # Trailing stops actifs (order_id -> TrailingStop)
        self._trailing_stops: dict[str, TrailingStop] = {}

        # État
        self._running = False
        self._start_time: float = 0.0
        self._ticks_received = 0
        self._signals_evaluated = 0
        self._signals_generated = 0
        self._trades_executed = 0
        self._last_signal_candle_idx = -10  # Index pour cooldown
        self._session_results: dict = {}

        # Résultats
        self._results_dir = Path(config.base_dir) / self.ph2.results_dir
        self._results_dir.mkdir(parents=True, exist_ok=True)

    async def run(self, duration_minutes: int = 60, connect_api: bool = False) -> dict:
        """Lance la session de paper trading Phase 2.

        Args:
            duration_minutes: Durée de la session en minutes.
            connect_api: Si True, tente de se connecter à l'API Deriv.

        Returns:
            Résumé de la session.
        """
        self.logger.info("=" * 60)
        self.logger.info("   PAPER TRADING PHASE 2 — DÉMARRAGE")
        self.logger.info("=" * 60)
        self.logger.info(f"Symbole: {self.config.market_symbol}")
        self.logger.info(f"Timeframe: {self.config.timeframe}")
        self.logger.info(f"Durée: {duration_minutes} min")
        self.logger.info(f"Capital initial: ${self.config.initial_capital:.2f}")
        self.logger.info(f"Filtres: EMA50/200={'ON' if self.ph2.use_trend_filter else 'OFF'} | "
                         f"ATR={'ON' if self.ph2.use_volatility_filter else 'OFF'} | "
                         f"Trailing={'ON' if self.ph2.use_trailing_stop else 'OFF'}")
        self.logger.info("=" * 60)

        self._running = True
        self._start_time = _time.time()
        session_end = self._start_time + duration_minutes * 60

        # Connexion API (optionnelle)
        if connect_api and self.config.deriv_token:
            self.deriv_client = DerivClient(self.config, self.logger)
            connected = await self.deriv_client.connect()
            if connected:
                self.logger.info("✓ Connecté à l'API Deriv (endpoint public)")
                await self.deriv_client.subscribe_ticks(self.config.market_symbol)
                self.deriv_client.on_tick(self._on_api_tick)
            else:
                self.logger.warning("⚠ Connexion API échouée — fallback sur données synthétiques")
                self.deriv_client = None

        # Wiring des callbacks
        self.data_streamer.subscribe(lambda tick: self.candle_builder.process_tick(tick))

        async def on_candle_closed(candle: Candle):
            await self._on_candle_closed(candle)

        self.candle_builder.on_candle_close(on_candle_closed)

        # Boucle principale — génération de ticks synthétiques si pas d'API
        tick_count = 0
        price = 100.0  # Prix de départ simulé (sera écrasé par l'API)

        while self._running and _time.time() < session_end:
            if self.deriv_client and self.deriv_client.is_connected:
                # Les ticks arrivent via _on_api_tick, on attend juste
                await asyncio.sleep(0.05)
            else:
                # Génération synthétique (mouvement brownien + cycle)
                returns = np.random.normal(0, 0.0003)
                cycle = 0.002 * np.sin(2 * np.pi * tick_count / 500)
                returns += cycle / 500
                price *= (1 + returns)

                tick_data = {
                    "epoch": _time.time(),
                    "quote": round(price, 5),
                    "symbol": self.config.market_symbol,
                }
                self.data_streamer.on_tick(tick_data)
                self._ticks_received += 1
                tick_count += 1
                await asyncio.sleep(0.05)

            # Log périodique
            elapsed = _time.time() - self._start_time
            if int(elapsed) % 60 == 0 and int(elapsed) > 0:
                self._log_status(int(elapsed))

        # Fin de session
        self._running = False
        await self._close_session()

        # Sauvegarde des résultats
        session_summary = self._build_session_summary(duration_minutes)
        self._save_session_results(session_summary)

        return session_summary

    def _on_api_tick(self, tick_data: dict) -> None:
        """Callback pour les ticks de l'API Deriv."""
        self.data_streamer.on_tick(tick_data)
        self._ticks_received += 1

    async def _on_candle_closed(self, candle: Candle) -> None:
        """Appelé à chaque bougie fermée. Évalue la stratégie et gère les positions."""
        candle_idx = self.candle_builder.count()
        self._signals_evaluated += 1

        # 1. Vérifier les positions ouvertes (SL/TP + trailing stop)
        closed_orders = []
        for order in self.order_executor.active_orders[:]:
            # Vérifier le trailing stop
            if self.ph2.use_trailing_stop and order.order_id in self._trailing_stops:
                ts = self._trailing_stops[order.order_id]
                new_sl = ts.update(candle.close)
                if new_sl is not None:
                    # Mettre à jour le SL de l'ordre
                    order.stop_loss = new_sl
                    self.logger.debug(f"Trailing stop ajusté | Order={order.order_id} | SL={new_sl:.5f}")

            # Vérifier SL/TP normal
            closed = await self.order_executor.simulate_price_movement(order, candle.close)
            if closed:
                closed_orders.append(closed)
                self._trailing_stops.pop(order.order_id, None)
                self.risk_manager.on_trade_closed(closed.pnl, closed.exit_price, closed.entry_price)
                self.logger.info(
                    f"[FERMETURE] {closed.direction.value} | "
                    f"Entry={closed.entry_price:.5f} Exit={closed.exit_price:.5f} | "
                    f"PnL=${closed.pnl:.2f} ({closed.pnl_pct:+.2f}%)"
                )

        # 2. Vérifier le cooldown
        candles_since_last_signal = candle_idx - self._last_signal_candle_idx
        if candles_since_last_signal < self.ph2.signal_cooldown_candles:
            return  # Cooldown actif

        # 3. Évaluer le signal
        signal = self._evaluate_phase2_signal(candle)
        self._signals_generated += 1

        if not signal.is_valid:
            return

        # 4. Vérifier les règles de risque
        can_trade, report = self.risk_manager.can_place_trade(signal)
        if not can_trade:
            if report.status != RiskStatus.OK:
                self.logger.debug(f"Trade bloqué: {report.reason_blocked}")
            return

        # 5. Exécuter l'ordre
        order = await self.order_executor.execute_signal(signal, report.position_size)
        if not order:
            return

        self.risk_manager.on_trade_opened(order.amount)
        self._last_signal_candle_idx = candle_idx
        self._trades_executed += 1

        # 6. Initialiser le trailing stop
        if self.ph2.use_trailing_stop and signal.atr_value > 0:
            ts = TrailingStop(
                activation_pct=self.ph2.trailing_activation_pct,
                distance_atr_mult=self.ph2.trailing_distance_atr_mult,
            )
            ts.initialize(
                entry_price=order.entry_price,
                original_sl=order.stop_loss,
                take_profit=order.take_profit,
                direction=signal.direction,
                atr_value=signal.atr_value,
            )
            self._trailing_stops[order.order_id] = ts

        self.logger.info(
            f"[TRADE #{self._trades_executed}] {signal.direction.value} | "
            f"Entry={order.entry_price:.5f} | Amount=${order.amount:.2f} | "
            f"SL={order.stop_loss:.5f} TP={order.take_profit:.5f} | "
            f"Score={signal.score:.0f} Conf={signal.confidence:.2f}"
        )

    def _evaluate_phase2_signal(self, current_candle: Candle) -> TradingSignal:
        """Évalue la stratégie avec tous les filtres Phase 2."""
        candles = self.candle_builder.get_recent_candles(300)
        if len(candles) < 210:
            return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

        closes = np.array([c.close for c in candles], dtype=np.float64)
        highs = np.array([c.high for c in candles], dtype=np.float64)
        lows = np.array([c.low for c in candles], dtype=np.float64)
        opens = np.array([c.open for c in candles], dtype=np.float64)

        # Indicateurs
        upper, middle, lower = self.indicators.bollinger_bands(closes)
        rsi_values = self.indicators.rsi(closes)
        atr_values = self.indicators.atr(highs, lows, closes)

        if upper is None or rsi_values is None or atr_values is None:
            return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

        last_close = current_candle.close
        last_open = current_candle.open
        last_high = current_candle.high
        last_low = current_candle.low
        last_upper = float(upper[-1]) if not np.isnan(upper[-1]) else 0
        last_lower = float(lower[-1]) if not np.isnan(lower[-1]) else 0
        last_middle = float(middle[-1]) if not np.isnan(middle[-1]) else 0
        last_rsi = float(rsi_values[-1]) if not np.isnan(rsi_values[-1]) else 50
        last_atr = float(atr_values[-1]) if not np.isnan(atr_values[-1]) else 0

        # ── Filtre Volatilité ──
        if self.ph2.use_volatility_filter and last_atr > 0 and last_close > 0:
            atr_pct = last_atr / last_close
            if atr_pct < self.ph2.min_atr_pct or atr_pct > self.ph2.max_atr_pct:
                return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

        # ── Filtre Tendance EMA ──
        uptrend = True
        downtrend = True
        if self.ph2.use_trend_filter and len(closes) >= self.ph2.ema_slow + 5:
            ema50 = self.indicators.ema(closes, self.ph2.ema_fast)
            ema200 = self.indicators.ema(closes, self.ph2.ema_slow)
            last_ema50 = float(ema50[-1]) if not np.isnan(ema50[-1]) else 0
            last_ema200 = float(ema200[-1]) if not np.isnan(ema200[-1]) else 0
            if last_ema200 > 0:
                trend_ratio = (last_ema50 / last_ema200 - 1.0) * 100.0
                uptrend = last_ema50 > last_ema200
                downtrend = last_ema50 < last_ema200
                if abs(trend_ratio) < self.ph2.trend_strength_min_pct:
                    return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

        # ── Détection des signaux ──
        min_score = 60.0
        rsi_os = self.config.rsi_oversold
        rsi_ob = self.config.rsi_overbought

        # CALL
        call_conditions = 0
        call_score = 0.0
        rejection_call = False

        if last_close < last_lower and last_open < last_lower:
            call_conditions += 1
            call_score += 35.0
        elif last_close < last_lower or last_open < last_lower:
            call_score += 10.0

        if last_rsi < rsi_os:
            call_conditions += 1
            call_score += 40.0
        elif last_rsi < 35:
            call_score += 20.0

        rejection_call = Indicators.is_rejection_candle(
            last_open, last_high, last_low, last_close, "CALL"
        )
        if rejection_call:
            call_conditions += 1
            call_score += 30.0

        # PUT
        put_conditions = 0
        put_score = 0.0
        rejection_put = False

        if last_close > last_upper and last_open > last_upper:
            put_conditions += 1
            put_score += 35.0
        elif last_close > last_upper or last_open > last_upper:
            put_score += 10.0

        if last_rsi > rsi_ob:
            put_conditions += 1
            put_score += 40.0
        elif last_rsi > 65:
            put_score += 20.0

        rejection_put = Indicators.is_rejection_candle(
            last_open, last_high, last_low, last_close, "PUT"
        )
        if rejection_put:
            put_conditions += 1
            put_score += 30.0

        # ── Calcul SL/TP ATR ──
        if self.ph2.use_atr_stops and last_atr > 0:
            atr_sl = last_atr * self.ph2.atr_sl_mult
            atr_tp = last_atr * self.ph2.atr_tp_mult
        else:
            atr_sl = last_close * 0.005
            atr_tp = last_close * 0.015

        if call_conditions >= 2 and call_score >= min_score:
            if self.ph2.use_trend_filter and not uptrend:
                return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)
            return TradingSignal(
                direction=SignalDirection.CALL,
                symbol=self.config.market_symbol,
                score=call_score,
                confidence=min(call_score / 100.0, 0.95),
                entry_price=last_close,
                bb_upper=last_upper,
                bb_middle=last_middle,
                bb_lower=last_lower,
                rsi_value=last_rsi,
                atr_value=last_atr,
                stop_loss=last_close - atr_sl,
                take_profit=last_close + atr_tp,
                rejection_confirmed=rejection_call,
                timestamp=current_candle.timestamp,
            )

        if put_conditions >= 2 and put_score >= min_score:
            if self.ph2.use_trend_filter and not downtrend:
                return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)
            return TradingSignal(
                direction=SignalDirection.PUT,
                symbol=self.config.market_symbol,
                score=put_score,
                confidence=min(put_score / 100.0, 0.95),
                entry_price=last_close,
                bb_upper=last_upper,
                bb_middle=last_middle,
                bb_lower=last_lower,
                rsi_value=last_rsi,
                atr_value=last_atr,
                stop_loss=last_close + atr_sl,
                take_profit=last_close - atr_tp,
                rejection_confirmed=rejection_put,
                timestamp=current_candle.timestamp,
            )

        return TradingSignal(direction=SignalDirection.HOLD, symbol=self.config.market_symbol)

    def _log_status(self, elapsed_minutes: int) -> None:
        """Affiche le statut en temps réel."""
        report = self.risk_manager.get_report()
        regime = "N/A"
        candles = self.candle_builder.get_recent_candles(300)
        if len(candles) >= 210:
            closes = np.array([c.close for c in candles], dtype=np.float64)
            highs = np.array([c.high for c in candles], dtype=np.float64)
            lows = np.array([c.low for c in candles], dtype=np.float64)
            ema50 = self.indicators.ema(closes, 50)
            ema200 = self.indicators.ema(closes, 200)
            atr_val = self.indicators.atr(highs, lows, closes)
            market = MarketRegime.detect(closes, highs, lows, ema50, ema200, atr_val)
            regime = market["regime"]

        self.logger.info(
            f"[{elapsed_minutes:3d}min] "
            f"Capital=${report.current_capital:.2f} | "
            f"Daily PnL=${report.daily_pnl:+.2f} ({report.daily_pnl_pct:+.2f}%) | "
            f"DD={report.drawdown_pct:.2f}% | "
            f"Trades={report.trades_today}/{report.max_trades_per_day} | "
            f"WinRate={self.risk_manager.win_rate*100:.1f}% | "
            f"Regime={regime} | "
            f"Ticks={self._ticks_received}"
        )

    async def _close_session(self) -> None:
        """Ferme proprement la session."""
        # Fermer toutes les positions ouvertes
        current_price = self.candle_builder.current_candle.close if self.candle_builder.current_candle else 100.0
        closed = self.order_executor.close_all_orders(current_price)
        for order in closed:
            self.risk_manager.on_trade_closed(order.pnl, order.exit_price, order.entry_price)

        # Déconnexion API
        if self.deriv_client:
            await self.deriv_client.disconnect()

        self.logger.info("Session Phase 2 terminée")

    def _build_session_summary(self, duration_minutes: int) -> dict:
        """Construit le résumé de la session."""
        report = self.risk_manager.get_report()

        # Calculer le ratio R:R réel
        actual_rr = 0.0
        if self.order_executor.active_orders:
            closed_orders = [o for o in self.order_executor._order_history]
            wins = [o.pnl for o in closed_orders if o.pnl > 0]
            losses = [abs(o.pnl) for o in closed_orders if o.pnl <= 0]
            avg_win = np.mean(wins) if wins else 0
            avg_loss = np.mean(losses) if losses else 0
            actual_rr = avg_win / avg_loss if avg_loss > 0 else 0

        return {
            "session_info": {
                "start_time": datetime.fromtimestamp(self._start_time, tz=timezone.utc).isoformat(),
                "end_time": datetime.now(timezone.utc).isoformat(),
                "duration_minutes": duration_minutes,
                "symbol": self.config.market_symbol,
                "timeframe": self.config.timeframe,
                "phase": "2",
            },
            "results": {
                "initial_capital": report.initial_capital,
                "final_capital": round(report.current_capital, 2),
                "total_pnl": round(report.total_pnl, 2),
                "total_return_pct": round(report.total_pnl_pct, 2),
                "max_drawdown_pct": round(report.drawdown_pct, 2),
                "win_rate": round(self.risk_manager.win_rate * 100, 2),
                "total_trades": self.risk_manager.total_trades,
                "winning_trades": self.risk_manager.winning_trades,
                "losing_trades": self.risk_manager.losing_trades,
                "actual_risk_reward_ratio": round(actual_rr, 2),
            },
            "activity": {
                "ticks_received": self._ticks_received,
                "signals_evaluated": self._signals_evaluated,
                "signals_generated": self._signals_generated,
                "trades_executed": self._trades_executed,
            },
            "config": {
                "trend_filter": self.ph2.use_trend_filter,
                "volatility_filter": self.ph2.use_volatility_filter,
                "atr_stops": self.ph2.use_atr_stops,
                "trailing_stop": self.ph2.use_trailing_stop,
                "atr_sl_mult": self.ph2.atr_sl_mult,
                "atr_tp_mult": self.ph2.atr_tp_mult,
            },
        }

    def _save_session_results(self, summary: dict) -> None:
        """Sauvegarde les résultats dans un fichier JSON."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self._results_dir / f"session_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Résultats sauvegardés: {filename}")

    def stop(self) -> None:
        """Arrête la session."""
        self._running = False
        self.logger.info("Arrêt demandé...")


# ─── CLI ─────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Paper Trading Phase 2 — Bollinger + RSI optimisé",
    )
    parser.add_argument("--symbol", "-s", type=str, default=None,
                        help="Symbole (défaut: config)")
    parser.add_argument("--duration", "-d", type=int, default=60,
                        help="Durée en minutes (défaut: 60)")
    parser.add_argument("--api", action="store_true",
                        help="Utiliser l'API Deriv (sinon données synthétiques)")
    parser.add_argument("--config", "-c", type=str, default=None,
                        help="Fichier .env personnalisé")
    args = parser.parse_args()

    config = load_config(args.config)
    if args.symbol:
        # On ne peut pas modifier une frozen dataclass, on réassigne
        import os
        os.environ["MARKET_SYMBOL"] = args.symbol
        config = load_config(args.config)

    engine = PaperTradingPhase2(config)

    # Gestion du Ctrl+C
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, engine.stop)
        except NotImplementedError:
            pass

    try:
        summary = await engine.run(
            duration_minutes=args.duration,
            connect_api=args.api,
        )
    except KeyboardInterrupt:
        engine.stop()
        summary = engine._build_session_summary(args.duration)
        engine._save_session_results(summary)
    finally:
        # Afficher le résumé final
        print("\n" + "=" * 60)
        print("   PHASE 2 — RÉSUMÉ DE LA SESSION")
        print("=" * 60)
        r = summary["results"]
        a = summary["activity"]
        c = summary["config"]
        print(f"  Capital final:       ${r['final_capital']:.2f}")
        print(f"  P&L total:           ${r['total_pnl']:+.2f} ({r['total_return_pct']:+.2f}%)")
        print(f"  Drawdown max:        {r['max_drawdown_pct']:.2f}%")
        print(f"  Trades:              {r['total_trades']} ({r['winning_trades']}W / {r['losing_trades']}L)")
        print(f"  Win rate:            {r['win_rate']:.1f}%")
        print(f"  Actual R:R:          {r['actual_risk_reward_ratio']:.2f}")
        print(f"  Ticks:               {a['ticks_received']}")
        print(f"  Signaux générés:     {a['signals_generated']}")
        print(f"  Trades exécutés:     {a['trades_executed']}")
        print(f"  Filtres:             Trend={c['trend_filter']} Vol={c['volatility_filter']} "
              f"ATR-SL={c['atr_stops']} Trailing={c['trailing_stop']}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())