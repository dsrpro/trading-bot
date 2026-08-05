"""Streaming et preprocessing des ticks en temps reel.

Transforme le flux brut de l'API Deriv en donnees structurees utilisables
par le moteur de strategie et le candle builder.
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import numpy as np

from src.config import Config


class Tick:
    """Representation normalisee d'un tick de marche."""

    __slots__ = ("timestamp", "symbol", "price", "epoch")

    def __init__(self, timestamp: float, symbol: str, price: float):
        self.timestamp = timestamp  # Unix timestamp (secondes)
        self.symbol = symbol
        self.price = price
        self.epoch = int(timestamp)

    @classmethod
    def from_deriv(cls, tick_data: dict, symbol: str) -> "Tick":
        """Cree un Tick a partir des donnees brutes Deriv.

        Args:
            tick_data: Dictionnaire tick de l'API Deriv.
            symbol: Symbole du marche.

        Returns:
            Instance de Tick.
        """
        timestamp = tick_data.get("epoch", datetime.now(timezone.utc).timestamp())
        price = float(tick_data.get("quote", 0.0))
        return cls(timestamp=float(timestamp), symbol=symbol, price=price)

    def __repr__(self) -> str:
        return f"Tick({self.symbol}, price={self.price:.5f}, ts={self.timestamp})"


class TickBuffer:
    """Buffer circulaire de ticks avec capacite fixe.

    Optimise pour des operations rapides d'ajout et de lecture.
    """

    def __init__(self, maxlen: int = 1000):
        self._buffer: deque[Tick] = deque(maxlen=maxlen)

    def add(self, tick: Tick) -> None:
        """Ajoute un tick au buffer."""
        self._buffer.append(tick)

    def last(self, n: int = 1) -> list[Tick]:
        """Retourne les n derniers ticks."""
        return list(self._buffer)[-n:]

    def prices(self) -> np.ndarray:
        """Retourne tous les prix sous forme de tableau NumPy."""
        return np.array([t.price for t in self._buffer], dtype=np.float64)

    def timestamps(self) -> np.ndarray:
        """Retourne tous les timestamps sous forme de tableau NumPy."""
        return np.array([t.timestamp for t in self._buffer], dtype=np.float64)

    def __len__(self) -> int:
        return len(self._buffer)

    @property
    def latest_price(self) -> Optional[float]:
        """Dernier prix disponible."""
        if self._buffer:
            return self._buffer[-1].price
        return None

    @property
    def latest_timestamp(self) -> Optional[float]:
        """Dernier timestamp disponible."""
        if self._buffer:
            return self._buffer[-1].timestamp
        return None

    def clear(self) -> None:
        """Vide le buffer."""
        self._buffer.clear()


class DataStreamer:
    """Gestionnaire de flux de donnees de marche.

    Responsabilites:
        - Reception des ticks bruts de l'API Deriv
        - Validation et nettoyage des ticks (valeurs aberrantes, doublons)
        - Distribution aux modules de consommation (buffer, candle builder)
        - Calcul de statistiques de base (spread, volatilite instantanee)
    """

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger("data_streamer")
        self.symbol = config.market_symbol
        self.tick_buffer = TickBuffer(maxlen=config.tick_buffer_size)
        self._subscribers: list[callable] = []
        self._tick_count = 0
        self._last_price: Optional[float] = None
        self._last_timestamp: Optional[float] = None

        # Statistiques instantanees
        self._price_changes: deque[float] = deque(maxlen=100)
        self.instant_volatility: float = 0.0

    def subscribe(self, callback: callable) -> None:
        """Ajoute un callback appele a chaque nouveau tick valide."""
        self._subscribers.append(callback)

    def on_tick(self, tick_data: dict) -> Optional[Tick]:
        """Traite un tick brut de l'API Deriv.

        Args:
            tick_data: Dictionnaire tick de l'API.

        Returns:
            Tick valide ou None si rejete.
        """
        try:
            tick = Tick.from_deriv(tick_data, self.symbol)
        except (KeyError, ValueError, TypeError) as e:
            self.logger.debug(f"Tick invalide ignore: {e} | data={tick_data}")
            return None

        # Validation : prix positif
        if tick.price <= 0:
            self.logger.debug(f"Prix negatif ou nul ignore: {tick.price}")
            return None

        # Validation : prix non-NaN
        if np.isnan(tick.price) or np.isinf(tick.price):
            self.logger.debug(f"Prix NaN/Inf ignore: {tick.price}")
            return None

        # Detection de doublon (meme timestamp + meme prix)
        if self._last_timestamp is not None and self._last_price is not None:
            if tick.timestamp == self._last_timestamp and tick.price == self._last_price:
                return None  # Doublon silencieux

        # Detection de gap anormal (> 10% en un tick)
        if self._last_price is not None and self._last_price != 0:
            change_pct = abs(tick.price - self._last_price) / self._last_price
            if change_pct > 0.10:  # 10% en un tick = suspect
                self.logger.warning(
                    f"Variation suspecte detectee: {change_pct*100:.2f}% "
                    f"({self._last_price:.5f} -> {tick.price:.5f})"
                )

        # Mise a jour du buffer
        self.tick_buffer.add(tick)
        self._tick_count += 1

        # Mise a jour de la volatilite instantanee
        if self._last_price is not None and self._last_price != 0:
            change = abs(tick.price - self._last_price) / self._last_price
            self._price_changes.append(change)
            if len(self._price_changes) >= 2:
                self.instant_volatility = float(np.std(self._price_changes))

        self._last_price = tick.price
        self._last_timestamp = tick.timestamp

        # Notification des souscripteurs
        for cb in self._subscribers:
            try:
                cb(tick)
            except Exception as e:
                self.logger.error(f"Erreur callback data streamer: {e}")

        return tick

    def get_recent_prices(self, n: int) -> np.ndarray:
        """Retourne les n derniers prix sous forme de tableau NumPy.

        Args:
            n: Nombre de ticks recents.

        Returns:
            Tableau NumPy de prix.
        """
        ticks = self.tick_buffer.last(n)
        if not ticks:
            return np.array([], dtype=np.float64)
        return np.array([t.price for t in ticks], dtype=np.float64)

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def latest_price(self) -> Optional[float]:
        return self.tick_buffer.latest_price

    def reset(self) -> None:
        """Reinitialise le streamer."""
        self.tick_buffer.clear()
        self._tick_count = 0
        self._last_price = None
        self._last_timestamp = None
        self._price_changes.clear()
        self.instant_volatility = 0.0