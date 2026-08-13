"""Stockage local des ticks et chandeliers dans SQLite.

Permet d'accumuler un historique long terme sans dependre de l'API.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np


class TickStorage:
    """Base de donnees SQLite pour le stockage local des ticks et chandeliers.

    Schema:
        ticks: (epoch INTEGER, symbol TEXT, price REAL, UNIQUE(epoch, symbol))
        candles: (timestamp INTEGER, symbol TEXT, timeframe TEXT,
                  open REAL, high REAL, low REAL, close REAL, volume INTEGER,
                  UNIQUE(timestamp, symbol, timeframe))
    """

    def __init__(self, db_path: Optional[str] = None, logger: Optional[logging.Logger] = None):
        if db_path is None:
            db_path = str(Path(__file__).resolve().parent.parent / "data" / "market_data.db")
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.logger = logger or logging.getLogger("storage")
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _init_db(self) -> None:
        """Cree les tables si elles n'existent pas."""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    epoch INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    UNIQUE(epoch, symbol)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER DEFAULT 0,
                    UNIQUE(timestamp, symbol, timeframe)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ticks_symbol_epoch ON ticks(symbol, epoch)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts ON candles(symbol, timeframe, timestamp)")
            conn.commit()

    def _get_conn(self) -> sqlite3.Connection:
        """Retourne une connexion (ou la cree)."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        return self._conn

    def insert_ticks(self, ticks: list[dict]) -> int:
        """Insere des ticks dans la base (ignore les doublons).

        Args:
            ticks: Liste de dicts {'epoch': int, 'symbol': str, 'quote': float}

        Returns:
            Nombre de ticks inseres.
        """
        inserted = 0
        with self._get_conn() as conn:
            for tick in ticks:
                try:
                    cursor = conn.execute(
                        "INSERT OR IGNORE INTO ticks (epoch, symbol, price) VALUES (?, ?, ?)",
                        (int(tick["epoch"]), tick.get("symbol", ""), float(tick.get("quote", 0))),
                    )
                    inserted += cursor.rowcount
                except Exception as e:
                    self.logger.debug(f"Erreur insertion tick: {e}")
            conn.commit()
        return inserted

    def insert_candles(self, candles: list) -> int:
        """Insere des chandeliers dans la base (ignore les doublons).

        Args:
            candles: Liste d'objets Candle.

        Returns:
            Nombre de chandeliers inseres.
        """
        inserted = 0
        with self._get_conn() as conn:
            for c in candles:
                try:
                    cursor = conn.execute(
                        """INSERT OR IGNORE INTO candles
                           (timestamp, symbol, timeframe, open, high, low, close, volume)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            int(c.timestamp),
                            c.symbol,
                            c.timeframe,
                            c.open,
                            c.high,
                            c.low,
                            c.close,
                            c.volume,
                        ),
                    )
                    inserted += cursor.rowcount
                except Exception as e:
                    self.logger.debug(f"Erreur insertion chandelier: {e}")
            conn.commit()
        return inserted

    def get_ticks(self, symbol: str, start_epoch: int, end_epoch: int) -> list[dict]:
        """Recupere les ticks dans une plage de temps.

        Args:
            symbol: Symbole du marche.
            start_epoch: Timestamp de debut (inclus).
            end_epoch: Timestamp de fin (inclus).

        Returns:
            Liste de dicts {'epoch': int, 'symbol': str, 'quote': float}
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT epoch, symbol, price FROM ticks WHERE symbol = ? AND epoch >= ? AND epoch <= ? ORDER BY epoch ASC",
                (symbol, start_epoch, end_epoch),
            ).fetchall()
        return [{"epoch": r[0], "symbol": r[1], "quote": r[2]} for r in rows]

    def get_candles(self, symbol: str, timeframe: str, start_ts: int, end_ts: int) -> list[dict]:
        """Recupere les chandeliers dans une plage de temps.

        Args:
            symbol: Symbole du marche.
            timeframe: Timeframe (ex: "M1").
            start_ts: Timestamp de debut (inclus).
            end_ts: Timestamp de fin (inclus).

        Returns:
            Liste de dicts OHLCV.
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT timestamp, symbol, timeframe, open, high, low, close, volume
                   FROM candles
                   WHERE symbol = ? AND timeframe = ? AND timestamp >= ? AND timestamp <= ?
                   ORDER BY timestamp ASC""",
                (symbol, timeframe, start_ts, end_ts),
            ).fetchall()
        return [
            {
                "timestamp": r[0],
                "symbol": r[1],
                "timeframe": r[2],
                "open": r[3],
                "high": r[4],
                "low": r[5],
                "close": r[6],
                "volume": r[7],
            }
            for r in rows
        ]

    def count_ticks(self, symbol: str) -> int:
        """Nombre de ticks stockes pour un symbole."""
        with self._get_conn() as conn:
            row = conn.execute("SELECT COUNT(*) FROM ticks WHERE symbol = ?", (symbol,)).fetchone()
        return row[0] if row else 0

    def count_candles(self, symbol: str, timeframe: str) -> int:
        """Nombre de chandeliers stockes pour un symbole/timeframe."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM candles WHERE symbol = ? AND timeframe = ?",
                (symbol, timeframe),
            ).fetchone()
        return row[0] if row else 0

    def get_date_range(self, symbol: str) -> tuple[int, int] | tuple[None, None]:
        """Premier et dernier timestamp des ticks pour un symbole."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT MIN(epoch), MAX(epoch) FROM ticks WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        if row and row[0] is not None:
            return int(row[0]), int(row[1])
        return None, None

    def close(self) -> None:
        """Ferme la connexion a la base de donnees."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
