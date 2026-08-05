"""Calcul des indicateurs techniques.

Implémente Bollinger Bands, RSI, ATR, EMA, MACD et patterns de chandeliers
avec calculs vectorisés NumPy + TA-Lib pour performance maximale.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np

from src.config import Config

# TA-Lib est optionnel — fallback sur NumPy pur si non installé
try:
    import talib
    HAS_TALIB = True
except ImportError:
    HAS_TALIB = False


class Indicators:
    """Calculateur d'indicateurs techniques vectorisés.

    Tous les calculs sont effectués sur des tableaux NumPy pour une performance optimale.
    Supporte TA-Lib si installé, sinon utilise des implémentations NumPy pures.
    """

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger("indicators")
        if not HAS_TALIB:
            self.logger.warning("TA-Lib non installé — utilisation des calculs NumPy purs (moins rapide)")

    # ── Bollinger Bands ──────────────────────────────────────────────

    def bollinger_bands(
        self, close: np.ndarray, period: Optional[int] = None, nbdev: Optional[float] = None
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray] | tuple[None, None, None]:
        """Calcule les bandes de Bollinger.

        Args:
            close: Prix de clôture (1D array).
            period: Période (défaut: config.bb_period).
            nbdev: Nombre d'écarts-types (défaut: config.bb_stddev).

        Returns:
            (upper_band, middle_band, lower_band) — chaque élément est un array ou None si données insuffisantes.
        """
        period = period or self.config.bb_period
        nbdev = nbdev or self.config.bb_stddev

        if len(close) < period:
            return None, None, None

        if HAS_TALIB:
            upper, middle, lower = talib.BBANDS(close, timeperiod=period, nbdevup=nbdev, nbdevdn=nbdev, matype=0)
            return upper, middle, lower

        # Implémentation NumPy pure
        middle = np.convolve(close, np.ones(period) / period, mode="valid")
        # On pad pour avoir la même longueur
        middle_full = np.full_like(close, np.nan)
        middle_full[period - 1:] = middle

        std = np.array([np.std(close[i - period + 1:i + 1], ddof=0) for i in range(period - 1, len(close))])
        std_full = np.full_like(close, np.nan)
        std_full[period - 1:] = std

        upper = middle_full + nbdev * std_full
        lower = middle_full - nbdev * std_full

        return upper, middle_full, lower

    # ── RSI ──────────────────────────────────────────────────────────

    def rsi(self, close: np.ndarray, period: Optional[int] = None) -> np.ndarray:
        """Calcule le Relative Strength Index (RSI).

        Args:
            close: Prix de clôture.
            period: Période (défaut: config.rsi_period).

        Returns:
            Array RSI de même longueur que close (NaN pour les valeurs non calculables).
        """
        period = period or self.config.rsi_period

        if len(close) < period + 1:
            return np.full_like(close, np.nan)

        if HAS_TALIB:
            return talib.RSI(close, timeperiod=period)

        # Implémentation NumPy pure (Wilder's smoothing)
        deltas = np.diff(close)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        rsi = np.full_like(close, np.nan)
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        if avg_loss == 0:
            rsi[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[period] = 100.0 - (100.0 / (1.0 + rs))

        for i in range(period + 1, len(close)):
            avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
            if avg_loss == 0:
                rsi[i] = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi[i] = 100.0 - (100.0 / (1.0 + rs))

        return rsi

    # ── EMA ──────────────────────────────────────────────────────────

    def ema(self, close: np.ndarray, period: int) -> np.ndarray:
        """Calcule l'Exponential Moving Average.

        Args:
            close: Prix de clôture.
            period: Période.

        Returns:
            Array EMA.
        """
        if len(close) < period:
            return np.full_like(close, np.nan)

        if HAS_TALIB:
            return talib.EMA(close, timeperiod=period)

        multiplier = 2.0 / (period + 1.0)
        ema = np.full_like(close, np.nan)
        ema[period - 1] = np.mean(close[:period])
        for i in range(period, len(close)):
            ema[i] = (close[i] - ema[i - 1]) * multiplier + ema[i - 1]
        return ema

    # ── ATR ──────────────────────────────────────────────────────────

    def atr(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
        """Calcule l'Average True Range (ATR).

        Args:
            high: Prix hauts.
            low: Prix bas.
            close: Prix de clôture.
            period: Période.

        Returns:
            Array ATR.
        """
        if len(close) < period + 1:
            return np.full_like(close, np.nan)

        if HAS_TALIB:
            return talib.ATR(high, low, close, timeperiod=period)

        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]

        tr = np.maximum(
            high - low,
            np.maximum(
                np.abs(high - prev_close),
                np.abs(low - prev_close),
            ),
        )

        atr = np.full_like(close, np.nan)
        atr[period] = np.mean(tr[1:period + 1])
        for i in range(period + 1, len(close)):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

        return atr

    # ── MACD ─────────────────────────────────────────────────────────

    def macd(
        self, close: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Calcule le MACD (Moving Average Convergence Divergence).

        Args:
            close: Prix de clôture.
            fast: Période rapide.
            slow: Période lente.
            signal: Période du signal.

        Returns:
            (macd_line, signal_line, histogram).
        """
        if len(close) < slow:
            nan_arr = np.full_like(close, np.nan)
            return nan_arr, nan_arr, nan_arr

        if HAS_TALIB:
            macd_line, signal_line, hist = talib.MACD(close, fastperiod=fast, slowperiod=slow, signalperiod=signal)
            return macd_line, signal_line, hist

        ema_fast = self.ema(close, fast)
        ema_slow = self.ema(close, slow)
        macd_line = ema_fast - ema_slow
        signal_line = self.ema(macd_line, signal)
        # L'EMA retourne NaN au début, on propage
        histogram = macd_line - signal_line

        return macd_line, signal_line, histogram

    # ── Rejection Candlestick Pattern ────────────────────────────────

    @staticmethod
    def is_rejection_candle(
        open_price: float,
        high: float,
        low: float,
        close_price: float,
        direction: str,
        wick_threshold: float = 0.50,
    ) -> bool:
        """Détecte un pattern de rejet (longue mèche).

        Pour un signal BUY (rejet baissier → haussier):
            - Mèche inférieure >= wick_threshold * amplitude totale
            - Corps relativement petit OU chandelier haussier

        Pour un signal SELL (rejet haussier → baissier):
            - Mèche supérieure >= wick_threshold * amplitude totale
            - Corps relativement petit OU chandelier baissier

        Args:
            open_price: Prix d'ouverture.
            high: Prix haut.
            low: Prix bas.
            close_price: Prix de clôture.
            direction: "CALL" (BUY) ou "PUT" (SELL).
            wick_threshold: Ratio minimum mèche/amplitude (0.50 = 50%).

        Returns:
            True si le pattern de rejet est détecté.
        """
        total_range = high - low
        if total_range <= 0:
            return False

        body = abs(close_price - open_price)
        upper_wick = high - max(open_price, close_price)
        lower_wick = min(open_price, close_price) - low

        if direction == "CALL":
            # Rejet baissier : longue mèche inférieure — les vendeurs ont poussé vers le bas
            # mais les acheteurs ont repris le contrôle
            return lower_wick >= wick_threshold * total_range and close_price > low + 0.5 * total_range

        if direction == "PUT":
            # Rejet haussier : longue mèche supérieure — les acheteurs ont poussé vers le haut
            # mais les vendeurs ont repris le contrôle
            return upper_wick >= wick_threshold * total_range and close_price < high - 0.5 * total_range

        return False

    # ── Utilitaires ──────────────────────────────────────────────────

    @staticmethod
    def sma(data: np.ndarray, period: int) -> np.ndarray:
        """Simple Moving Average."""
        if len(data) < period:
            return np.full_like(data, np.nan)
        result = np.convolve(data, np.ones(period) / period, mode="valid")
        full = np.full_like(data, np.nan)
        full[period - 1:] = result
        return full

    @staticmethod
    def rolling_std(data: np.ndarray, period: int) -> np.ndarray:
        """Écart-type roulant."""
        if len(data) < period:
            return np.full_like(data, np.nan)
        result = np.array([np.std(data[i - period + 1:i + 1], ddof=0) for i in range(period - 1, len(data))])
        full = np.full_like(data, np.nan)
        full[period - 1:] = result
        return full

    @staticmethod
    def rolling_max(data: np.ndarray, period: int) -> np.ndarray:
        """Maximum roulant."""
        if len(data) < period:
            return np.full_like(data, np.nan)
        result = np.array([np.max(data[i - period + 1:i + 1]) for i in range(period - 1, len(data))])
        full = np.full_like(data, np.nan)
        full[period - 1:] = result
        return full

    @staticmethod
    def rolling_min(data: np.ndarray, period: int) -> np.ndarray:
        """Minimum roulant."""
        if len(data) < period:
            return np.full_like(data, np.nan)
        result = np.array([np.min(data[i - period + 1:i + 1]) for i in range(period - 1, len(data))])
        full = np.full_like(data, np.nan)
        full[period - 1:] = result
        return full