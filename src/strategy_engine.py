"""Moteur de strategie de trading : Bollinger Bands + RSI + Rejection Candlesticks.

Analyse les chandeliers en temps reel et genere des signaux BUY/SELL
avec un score de confiance et une direction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import numpy as np

from src.candle_builder import Candle, CandleBuilder
from src.config import Config
from src.indicators import Indicators


class SignalDirection(Enum):
    """Direction d'un signal de trading."""
    CALL = "CALL"    # Achat / haussier
    PUT = "PUT"      # Vente / baissier
    HOLD = "HOLD"    # Aucun signal


@dataclass
class TradingSignal:
    """Signal de trading genere par le moteur de strategie."""

    direction: SignalDirection
    symbol: str
    strategy: str = "bollinger_rsi"
    score: float = 0.0        # Score 0-100
    confidence: float = 0.0   # Confiance 0.0-1.0

    # Prix au moment du signal
    entry_price: float = 0.0

    # Indicateurs au moment du signal
    bb_upper: float = 0.0
    bb_middle: float = 0.0
    bb_lower: float = 0.0
    rsi_value: float = 0.0
    atr_value: float = 0.0

    # Chandelier declencheur
    candle_open: float = 0.0
    candle_high: float = 0.0
    candle_low: float = 0.0
    candle_close: float = 0.0
    rejection_confirmed: bool = False

    # Timestamp
    timestamp: float = 0.0

    # Parametres de trade calcules
    stop_loss: float = 0.0
    take_profit: float = 0.0

    @property
    def is_valid(self) -> bool:
        """True si le signal est actionnable (pas HOLD)."""
        return self.direction != SignalDirection.HOLD and self.confidence > 0.5

    def to_dict(self) -> dict:
        return {
            "direction": self.direction.value,
            "symbol": self.symbol,
            "strategy": self.strategy,
            "score": self.score,
            "confidence": self.confidence,
            "entry_price": self.entry_price,
            "bb_upper": self.bb_upper,
            "bb_middle": self.bb_middle,
            "bb_lower": self.bb_lower,
            "rsi_value": self.rsi_value,
            "atr_value": self.atr_value,
            "rejection_confirmed": self.rejection_confirmed,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "timestamp": self.timestamp,
        }


class StrategyEngine:
    """Moteur de strategie : Bollinger Bands + RSI + Rejection Candles.

    Conditions d'entree (Plan 1, Section 3.1):

        CALL (BUY):
            - Close ET Open en dessous de la bande inferieure de Bollinger
            - RSI < 30 (survente)
            - Confirmation : pattern de rejet (longue meche inferieure)

        PUT (SELL):
            - Close ET Open au-dessus de la bande superieure de Bollinger
            - RSI > 70 (surachat)
            - Confirmation : pattern de rejet (longue meche superieure)
    """

    def __init__(
        self,
        config: Config,
        indicators: Indicators,
        candle_builder: CandleBuilder,
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config
        self.indicators = indicators
        self.candle_builder = candle_builder
        self.logger = logger or logging.getLogger("strategy_engine")

        # Dernier signal pour eviter les doublons
        self._last_signal_time: float = 0.0
        self._signal_cooldown_candles: int = 2

        # Parametres optimises Phase 2
        self.use_trend_filter: bool = True
        self.ema_fast_period: int = 50
        self.ema_slow_period: int = 200
        self.rsi_oversold_tight: float = 25.0
        self.rsi_overbought_tight: float = 75.0
        self.use_atr_trailing_stop: bool = True
        self.use_volatility_filter: bool = True
        self.atr_sl_multiplier: float = 1.5   # SL = 1.5x ATR
        self.atr_tp_multiplier: float = 3.0   # TP = 3.0x ATR (R:R ~1:2)
        self.min_atr_pct: float = 0.0003      # Filtre volatilité min 0.03%
        self.max_atr_pct: float = 0.008       # Filtre volatilité max 0.8%
        self.trend_strength_min: float = 0.15  # Force de tendance minimum (%)

    def evaluate(self, current_candle: Optional[Candle] = None) -> TradingSignal:
        """Evalue la strategie sur les bougies disponibles.

        Args:
            current_candle: Bougie en cours (optionnelle, pour evaluation intrabar).

        Returns:
            Un TradingSignal (direction HOLD si aucun signal).
        """
        candles = self.candle_builder.get_recent_candles(50)
        if len(candles) < self.config.bb_period + 2:
            self.logger.debug(
                f"Bougies insuffisantes: {len(candles)} < {self.config.bb_period + 2}"
            )
            return self._empty_signal()

        # Extraction des arrays
        closes = np.array([c.close for c in candles], dtype=np.float64)
        highs = np.array([c.high for c in candles], dtype=np.float64)
        lows = np.array([c.low for c in candles], dtype=np.float64)
        opens = np.array([c.open for c in candles], dtype=np.float64)

        # Si une bougie courante est fournie, l'ajouter aux arrays
        if current_candle and not current_candle.is_closed:
            closes = np.append(closes, current_candle.close)
            highs = np.append(highs, current_candle.high)
            lows = np.append(lows, current_candle.low)
            opens = np.append(opens, current_candle.open)

        # Calcul des indicateurs
        upper, middle, lower = self.indicators.bollinger_bands(closes)
        rsi_values = self.indicators.rsi(closes)
        atr_values = self.indicators.atr(highs, lows, closes)

        if upper is None or rsi_values is None:
            return self._empty_signal()

        # Dernieres valeurs
        last_close = closes[-1] if current_candle is None else (current_candle.close if current_candle else closes[-1])
        last_open = opens[-1] if current_candle is None else (current_candle.open if current_candle else opens[-1])
        last_high = highs[-1] if current_candle is None else (current_candle.high if current_candle else highs[-1])
        last_low = lows[-1] if current_candle is None else (current_candle.low if current_candle else lows[-1])

        last_upper = float(upper[-1]) if not np.isnan(upper[-1]) else 0.0
        last_middle = float(middle[-1]) if not np.isnan(middle[-1]) else 0.0
        last_lower = float(lower[-1]) if not np.isnan(lower[-1]) else 0.0
        last_rsi = float(rsi_values[-1]) if not np.isnan(rsi_values[-1]) else 50.0
        last_atr = float(atr_values[-1]) if not np.isnan(atr_values[-1]) else 0.0

        # Detection du signal
        signal = self._detect_signal(
            last_open, last_high, last_low, last_close,
            last_upper, last_middle, last_lower, last_rsi, last_atr,
        )

        # Remplir les donnees du signal
        if signal.direction != SignalDirection.HOLD:
            signal.entry_price = last_close
            signal.bb_upper = last_upper
            signal.bb_middle = last_middle
            signal.bb_lower = last_lower
            signal.rsi_value = last_rsi
            signal.atr_value = last_atr
            signal.candle_open = last_open
            signal.candle_high = last_high
            signal.candle_low = last_low
            signal.candle_close = last_close
            signal.timestamp = candles[-1].timestamp if not current_candle else (
                current_candle.timestamp if current_candle else candles[-1].timestamp
            )

            # Calcul des niveaux de SL/TP base sur l'ATR (volatilite reelle).
            # Utilise les multiplicateurs ATR definis en tete de classe :
            #   SL = 1.5x ATR, TP = 3.0x ATR (R:R ~1:2).
            if last_atr > 0:
                if signal.direction == SignalDirection.CALL:
                    signal.stop_loss = last_close - self.atr_sl_multiplier * last_atr
                    signal.take_profit = last_close + self.atr_tp_multiplier * last_atr
                else:
                    signal.stop_loss = last_close + self.atr_sl_multiplier * last_atr
                    signal.take_profit = last_close - self.atr_tp_multiplier * last_atr
            else:
                # Fallback si ATR indisponible : 1% / 5% autour du prix d'entree
                if signal.direction == SignalDirection.CALL:
                    signal.stop_loss = last_close * 0.99
                    signal.take_profit = last_close * 1.05
                else:
                    signal.stop_loss = last_close * 1.01
                    signal.take_profit = last_close * 0.95

            self.logger.info(
                f"SIGNAL {signal.direction.value} | "
                f"Price={signal.entry_price:.5f} | "
                f"RSI={signal.rsi_value:.1f} | "
                f"BB_Lower={signal.bb_lower:.5f} BB_Upper={signal.bb_upper:.5f} | "
                f"Score={signal.score:.0f} | Confidence={signal.confidence:.2f}"
            )

        return signal

    def _check_trend_filter(self, closes: np.ndarray) -> tuple[bool, bool, float]:
        """Verifie le filtre de tendance EMA 50/200.

        Args:
            closes: Array des prix de cloture (suffisamment long).

        Returns:
            (uptrend, downtrend, ratio_tendance)
            uptrend=True si EMA50 > EMA200
        """
        if len(closes) < self.ema_slow_period + 5:
            return True, True, 0.0  # Pas assez de donnees, on laisse passer

        ema50 = self.indicators.ema(closes, self.ema_fast_period)
        ema200 = self.indicators.ema(closes, self.ema_slow_period)

        last_ema50 = float(ema50[-1]) if not np.isnan(ema50[-1]) else 0.0
        last_ema200 = float(ema200[-1]) if not np.isnan(ema200[-1]) else 0.0

        if last_ema200 == 0:
            return True, True, 0.0

        ratio = (last_ema50 / last_ema200 - 1.0) * 100.0  # % d'ecart
        uptrend = last_ema50 > last_ema200
        downtrend = last_ema50 < last_ema200

        return uptrend, downtrend, ratio

    def _detect_signal(
        self,
        open_price: float,
        high: float,
        low: float,
        close_price: float,
        bb_upper: float,
        bb_middle: float,
        bb_lower: float,
        rsi_value: float,
        atr_value: float,
    ) -> TradingSignal:
        """Logique de detection du signal.

        Args:
            open_price, high, low, close_price: OHLC du dernier chandelier.
            bb_upper, bb_middle, bb_lower: Valeurs des bandes de Bollinger.
            rsi_value: Derniere valeur RSI.
            atr_value: Derniere valeur ATR.

        Returns:
            TradingSignal avec direction et score.
        """
        # Filtre de tendance EMA
        closes_all = self.candle_builder.close_array()
        uptrend, downtrend, trend_ratio = True, True, 0.0
        if self.use_trend_filter and len(closes_all) >= self.ema_slow_period + 5:
            uptrend, downtrend, trend_ratio = self._check_trend_filter(closes_all)

        # --- CALL Signal ---
        call_conditions = 0
        call_score = 0.0

        # Condition 1: Close et Open sous la bande inferieure
        if close_price < bb_lower and open_price < bb_lower:
            call_conditions += 1
            call_score += 35.0
        elif close_price < bb_lower or open_price < bb_lower:
            call_score += 10.0

        # Condition 2: RSI < seuil resserre (survente stricte)
        if rsi_value < self.rsi_oversold_tight:
            call_conditions += 1
            call_score += 40.0
        elif rsi_value < 30:
            call_score += 20.0
        elif rsi_value < 35:
            call_score += 10.0

        # Condition 3: Rejection pattern (longue meche inferieure)
        rejection_call = Indicators.is_rejection_candle(
            open_price, high, low, close_price, direction="CALL"
        )
        if rejection_call:
            call_conditions += 1
            call_score += 30.0

        # --- PUT Signal ---
        put_conditions = 0
        put_score = 0.0

        # Condition 1: Close et Open au-dessus de la bande superieure
        if close_price > bb_upper and open_price > bb_upper:
            put_conditions += 1
            put_score += 35.0
        elif close_price > bb_upper or open_price > bb_upper:
            put_score += 10.0

        # Condition 2: RSI > seuil resserre (surachat strict)
        if rsi_value > self.rsi_overbought_tight:
            put_conditions += 1
            put_score += 40.0
        elif rsi_value > 70:
            put_score += 20.0
        elif rsi_value > 65:
            put_score += 10.0

        # Condition 3: Rejection pattern (longue meche superieure)
        rejection_put = Indicators.is_rejection_candle(
            open_price, high, low, close_price, direction="PUT"
        )
        if rejection_put:
            put_conditions += 1
            put_score += 30.0

        # Détermination de la direction (avec filtre de tendance)
        min_score = 60.0
        min_conditions = 2

        if call_conditions >= min_conditions and call_score >= min_score:
            # CALL = haussier → exige une tendance haussiere (EMA50 > EMA200)
            if self.use_trend_filter and not uptrend:
                self.logger.debug(f"Signal CALL rejete: tendance baissiere (EMA50 < EMA200, ratio={trend_ratio:.1f}%)")
                return self._empty_signal()
            confidence = min(call_score / 100.0, 0.95)
            return TradingSignal(
                direction=SignalDirection.CALL,
                symbol=self.config.market_symbol,
                score=call_score,
                confidence=confidence,
                rejection_confirmed=rejection_call,
            )

        if put_conditions >= min_conditions and put_score >= min_score:
            # PUT = baissier → exige une tendance baissiere (EMA50 < EMA200)
            if self.use_trend_filter and not downtrend:
                self.logger.debug(f"Signal PUT rejete: tendance haussiere (EMA50 > EMA200, ratio={trend_ratio:.1f}%)")
                return self._empty_signal()
            confidence = min(put_score / 100.0, 0.95)
            return TradingSignal(
                direction=SignalDirection.PUT,
                symbol=self.config.market_symbol,
                score=put_score,
                confidence=confidence,
                rejection_confirmed=rejection_put,
            )

        # Pas de signal
        return self._empty_signal()

    def _empty_signal(self) -> TradingSignal:
        """Retourne un signal HOLD (aucune action)."""
        return TradingSignal(
            direction=SignalDirection.HOLD,
            symbol=self.config.market_symbol,
            score=0.0,
            confidence=0.0,
        )

    def reset(self) -> None:
        """Reinitialise le moteur de strategie."""
        self._last_signal_time = 0.0