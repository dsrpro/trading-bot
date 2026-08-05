"""Construction de chandeliers OHLC a partir d'un flux de ticks.

Agrege les ticks en chandeliers (Open, High, Low, Close) selon un timeframe donne.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import numpy as np

from src.config import Config
from src.data_streamer import Tick


@dataclass
class Candle:
    """Representation d'un chandelier OHLC."""

    timestamp: float  # Unix timestamp de debut
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    symbol: str = ""
    timeframe: str = "M1"
    is_closed: bool = False

    @property
    def body(self) -> float:
        """Taille du corps du chandelier (absolue)."""
        return abs(self.close - self.open)

    @property
    def upper_wick(self) -> float:
        """Taille de la meche superieure."""
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        """Taille de la meche inferieure."""
        return min(self.open, self.close) - self.low

    @property
    def total_range(self) -> float:
        """Amplitude totale (high - low)."""
        return self.high - self.low

    @property
    def is_bullish(self) -> bool:
        """True si le chandelier est haussier (close > open)."""
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """True si le chandelier est baissier (close < open)."""
        return self.close < self.open

    @property
    def is_doji(self) -> bool:
        """True si c'est un doji (corps tres petit par rapport a l'amplitude)."""
        if self.total_range == 0:
            return True
        return self.body / self.total_range < 0.05

    @property
    def upper_wick_pct(self) -> float:
        """Pourcentage de la meche superieure par rapport a l'amplitude."""
        if self.total_range == 0:
            return 0.0
        return self.upper_wick / self.total_range

    @property
    def lower_wick_pct(self) -> float:
        """Pourcentage de la meche inferieure par rapport a l'amplitude."""
        if self.total_range == 0:
            return 0.0
        return self.lower_wick / self.total_range

    def to_dict(self) -> dict:
        """Exporte les donnees en dictionnaire."""
        return {
            "timestamp": self.timestamp,
            "datetime": datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


class CandleBuilder:
    """Constructeur de chandeliers OHLC a partir de ticks.

    Responsabilites:
        - Agregation ticks -> chandeliers selon un timeframe
        - Gestion du volume (nombre de ticks par chandelier)
        - Notification lors de la fermeture d'un chandelier
        - Conservation d'un historique de chandeliers
    """

    TIMEFRAME_SECONDS = {
        "M1": 60,
        "M2": 120,
        "M3": 180,
        "M5": 300,
        "M15": 900,
        "M30": 1800,
        "H1": 3600,
        "H4": 14400,
        "D1": 86400,
    }

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger("candle_builder")
        self.timeframe_seconds = self.TIMEFRAME_SECONDS.get(
            config.timeframe, 60
        )
        self._max_history = 500  # Garder 500 dernieres bougies

        # Bougie en cours de construction
        self._current_candle: Optional[Candle] = None
        self._current_start: Optional[float] = None

        # Historique
        self._candles: deque[Candle] = deque(maxlen=self._max_history)

        # Callbacks
        self._on_candle_close: list[callable] = []

        # Volume cumule
        self._tick_count_in_candle: int = 0

    @property
    def current_candle(self) -> Optional[Candle]:
        """Bougie en cours de construction."""
        return self._current_candle

    @property
    def candles(self) -> deque[Candle]:
        """Historique des bougies fermees."""
        return self._candles

    def on_candle_close(self, callback: callable) -> None:
        """Enregistre un callback appele a chaque fermeture de bougie.

        Supporte les callbacks synchrones ET asynchrones (coroutines).
        """
        self._on_candle_close.append(callback)

    def process_tick(self, tick: Tick) -> Optional[Candle]:
        """Traite un tick et met a jour la bougie courante.

        Args:
            tick: Un tick de marche.

        Returns:
            Candle ferme si un chandelier vient de se fermer, None sinon.
        """
        # Determiner le debut du chandelier courant pour ce tick
        candle_start = self._get_candle_start(tick.timestamp)

        if self._current_start is None:
            # Premier tick
            self._start_new_candle(candle_start, tick)
            return None

        if candle_start == self._current_start:
            # Meme chandelier : mise a jour OHLCV
            self._update_candle(tick)
            return None

        if candle_start > self._current_start:
            # Nouveau chandelier : fermer l'ancien, ouvrir le nouveau
            closed_candle = self._close_current_candle()
            self._start_new_candle(candle_start, tick)
            return closed_candle

        # Tick avec timestamp anterieur (arrive en retard) - on ignore
        self.logger.debug(f"Tick ignore (timestamp anterieur): {tick.timestamp} < {self._current_start}")
        return None

    def get_recent_candles(self, n: int) -> list[Candle]:
        """Retourne les n dernieres bougies fermees (la plus recente en dernier).

        Args:
            n: Nombre de bougies.

        Returns:
            Liste de Candle.
        """
        closed = list(self._candles)[-n:]
        return closed

    def close_array(self, n: Optional[int] = None) -> np.ndarray:
        """Retourne les prix de cloture sous forme de tableau NumPy.

        Args:
            n: Nombre de bougies (toutes si None).

        Returns:
            Tableau NumPy de float64.
        """
        candles = list(self._candles)
        if n is not None:
            candles = candles[-n:]
        return np.array([c.close for c in candles], dtype=np.float64)

    def high_array(self, n: Optional[int] = None) -> np.ndarray:
        candles = list(self._candles)
        if n is not None:
            candles = candles[-n:]
        return np.array([c.high for c in candles], dtype=np.float64)

    def low_array(self, n: Optional[int] = None) -> np.ndarray:
        candles = list(self._candles)
        if n is not None:
            candles = candles[-n:]
        return np.array([c.low for c in candles], dtype=np.float64)

    def open_array(self, n: Optional[int] = None) -> np.ndarray:
        candles = list(self._candles)
        if n is not None:
            candles = candles[-n:]
        return np.array([c.open for c in candles], dtype=np.float64)

    def volume_array(self, n: Optional[int] = None) -> np.ndarray:
        candles = list(self._candles)
        if n is not None:
            candles = candles[-n:]
        return np.array([c.volume for c in candles], dtype=np.float64)

    def count(self) -> int:
        """Nombre de bougies fermees disponibles."""
        return len(self._candles)

    def reset(self) -> None:
        """Reinitialise le constructeur."""
        self._current_candle = None
        self._current_start = None
        self._candles.clear()
        self._tick_count_in_candle = 0

    def _get_candle_start(self, timestamp: float) -> float:
        """Calcule le timestamp de debut du chandelier pour un tick donne.

        Aligne sur le debut du timeframe (ex: M5 aligne sur 00, 05, 10...).
        """
        return (int(timestamp) // self.timeframe_seconds) * self.timeframe_seconds

    def _start_new_candle(self, candle_start: float, tick: Tick) -> None:
        """Ouvre un nouveau chandelier."""
        price = tick.price
        self._current_candle = Candle(
            timestamp=candle_start,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=1,
            symbol=tick.symbol,
            timeframe=self.config.timeframe,
        )
        self._current_start = candle_start
        self._tick_count_in_candle = 1

    def _update_candle(self, tick: Tick) -> None:
        """Met a jour le chandelier courant avec un nouveau tick."""
        if self._current_candle is None:
            return
        self._current_candle.high = max(self._current_candle.high, tick.price)
        self._current_candle.low = min(self._current_candle.low, tick.price)
        self._current_candle.close = tick.price
        self._tick_count_in_candle += 1

    def _close_current_candle(self) -> Optional[Candle]:
        """Ferme le chandelier courant et le stocke dans l'historique."""
        if self._current_candle is None:
            return None

        candle = self._current_candle
        candle.volume = self._tick_count_in_candle
        candle.is_closed = True

        self._candles.append(candle)

        # Notification — supporte callbacks sync ET async
        import asyncio
        for cb in self._on_candle_close:
            try:
                if asyncio.iscoroutinefunction(cb):
                    # Callback asynchrone : tenter de le scheduler dans l'event loop
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(cb(candle))
                    except RuntimeError:
                        # Pas d'event loop running — fallback synchrone impossible
                        self.logger.error(
                            "Callback async detecte mais pas d'event loop running — "
                            "utilisez un wrapper synchrone ou une loop asyncio"
                        )
                else:
                    cb(candle)
            except Exception as e:
                self.logger.error(f"Erreur callback candle close: {e}")

        self._current_candle = None
        self._tick_count_in_candle = 0

        return candle