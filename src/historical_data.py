"""Historical market data helpers for Deriv backtests.

The live backtest must validate that it is really using a multi-year market
history.  Pulling raw ticks for three years is unnecessarily large, so this
module fetches Deriv OHLC candles directly and checks the resulting coverage.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

from src.candle_builder import Candle
from src.deriv_client import DerivClient
from src.storage import TickStorage


DERIV_CANDLE_PAGE_SIZE = 1000

TIMEFRAME_SECONDS: dict[str, int] = {
    "M1": 60,
    "M2": 120,
    "M5": 300,
    "M10": 600,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H2": 7200,
    "H4": 14400,
    "H8": 28800,
    "D1": 86400,
}


@dataclass(frozen=True)
class HistoricalWindow:
    """Backtest period expressed as aligned epochs."""

    start_epoch: int
    end_epoch: int
    years: int
    timeframe_seconds: int

    @property
    def start_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.start_epoch, tz=timezone.utc)

    @property
    def end_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.end_epoch, tz=timezone.utc)


@dataclass(frozen=True)
class HistoricalCoverage:
    """Coverage summary for a list of historical candles."""

    requested_start: int
    requested_end: int
    first_timestamp: Optional[int]
    last_timestamp: Optional[int]
    candle_count: int
    expected_candles: int
    timeframe_seconds: int

    @property
    def span_coverage_pct(self) -> float:
        if self.first_timestamp is None or self.last_timestamp is None:
            return 0.0
        requested_span = max(self.timeframe_seconds, self.requested_end - self.requested_start)
        actual_span = max(0, self.last_timestamp - self.first_timestamp)
        return min(100.0, (actual_span / requested_span) * 100.0)

    @property
    def density_pct(self) -> float:
        if self.expected_candles <= 0:
            return 0.0
        return min(100.0, (self.candle_count / self.expected_candles) * 100.0)

    @property
    def first_gap_seconds(self) -> Optional[int]:
        if self.first_timestamp is None:
            return None
        return max(0, self.first_timestamp - self.requested_start)

    @property
    def last_gap_seconds(self) -> Optional[int]:
        if self.last_timestamp is None:
            return None
        return max(0, self.requested_end - self.last_timestamp)

    def is_sufficient(
        self,
        *,
        min_span_coverage_pct: float = 99.0,
        min_density_pct: float = 90.0,
        max_boundary_gap_seconds: int = 24 * 3600,
    ) -> bool:
        """Return True when the data covers the requested historical window."""

        if self.candle_count <= 0:
            return False
        if self.span_coverage_pct < min_span_coverage_pct:
            return False
        if self.density_pct < min_density_pct:
            return False
        if self.first_gap_seconds is None or self.first_gap_seconds > max_boundary_gap_seconds:
            return False
        if self.last_gap_seconds is None or self.last_gap_seconds > max_boundary_gap_seconds:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "requested_start": datetime.fromtimestamp(self.requested_start, tz=timezone.utc).isoformat(),
            "requested_end": datetime.fromtimestamp(self.requested_end, tz=timezone.utc).isoformat(),
            "first_timestamp": (
                datetime.fromtimestamp(self.first_timestamp, tz=timezone.utc).isoformat()
                if self.first_timestamp is not None
                else None
            ),
            "last_timestamp": (
                datetime.fromtimestamp(self.last_timestamp, tz=timezone.utc).isoformat()
                if self.last_timestamp is not None
                else None
            ),
            "candle_count": self.candle_count,
            "expected_candles": self.expected_candles,
            "span_coverage_pct": round(self.span_coverage_pct, 4),
            "density_pct": round(self.density_pct, 4),
            "first_gap_seconds": self.first_gap_seconds,
            "last_gap_seconds": self.last_gap_seconds,
            "timeframe_seconds": self.timeframe_seconds,
        }


def timeframe_to_seconds(timeframe: str) -> int:
    """Convert a timeframe label like M1, H1 or D1 to seconds."""

    normalized = timeframe.strip().upper()
    if normalized in TIMEFRAME_SECONDS:
        return TIMEFRAME_SECONDS[normalized]
    if normalized.startswith("M") and normalized[1:].isdigit():
        return int(normalized[1:]) * 60
    if normalized.startswith("H") and normalized[1:].isdigit():
        return int(normalized[1:]) * 3600
    if normalized in {"D", "1D"}:
        return 86400
    raise ValueError(f"Timeframe non supporte: {timeframe}")


def seconds_to_timeframe(seconds: int) -> str:
    """Convert seconds to the compact timeframe labels used in storage."""

    for label, value in TIMEFRAME_SECONDS.items():
        if value == seconds:
            return label
    if seconds % 86400 == 0:
        return f"D{seconds // 86400}"
    if seconds % 3600 == 0:
        return f"H{seconds // 3600}"
    if seconds % 60 == 0:
        return f"M{seconds // 60}"
    return f"S{seconds}"


def subtract_years(value: datetime, years: int) -> datetime:
    """Subtract calendar years, preserving month/day where possible."""

    if years <= 0:
        raise ValueError("years doit etre > 0")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        # February 29 -> February 28 when the target year is not leap.
        return value.replace(year=value.year - years, month=2, day=28)


def align_epoch(epoch: int, timeframe_seconds: int) -> int:
    return (int(epoch) // timeframe_seconds) * timeframe_seconds


def build_historical_window(
    years: int,
    timeframe_seconds: int,
    *,
    end_datetime: Optional[datetime] = None,
) -> HistoricalWindow:
    """Build an exact calendar-year window ending on the last closed candle."""

    if timeframe_seconds <= 0:
        raise ValueError("timeframe_seconds doit etre > 0")

    if end_datetime is None:
        end_datetime = datetime.now(timezone.utc)
    elif end_datetime.tzinfo is None:
        end_datetime = end_datetime.replace(tzinfo=timezone.utc)
    else:
        end_datetime = end_datetime.astimezone(timezone.utc)

    start_datetime = subtract_years(end_datetime, years)
    end_epoch = align_epoch(int(end_datetime.timestamp()), timeframe_seconds) - timeframe_seconds
    start_epoch = align_epoch(int(start_datetime.timestamp()), timeframe_seconds)
    if start_epoch >= end_epoch:
        raise ValueError("Fenetre historique invalide")

    return HistoricalWindow(
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        years=years,
        timeframe_seconds=timeframe_seconds,
    )


def expected_candle_count(start_epoch: int, end_epoch: int, timeframe_seconds: int) -> int:
    if end_epoch < start_epoch:
        return 0
    return ((align_epoch(end_epoch, timeframe_seconds) - align_epoch(start_epoch, timeframe_seconds)) // timeframe_seconds) + 1


def candles_from_deriv(
    raw_candles: Iterable[dict],
    *,
    symbol: str,
    timeframe: str,
) -> list[Candle]:
    """Convert Deriv candle dictionaries into project Candle objects."""

    candles: list[Candle] = []
    for item in raw_candles:
        candles.append(
            Candle(
                timestamp=int(item["epoch"]),
                open=float(item["open"]),
                high=float(item["high"]),
                low=float(item["low"]),
                close=float(item["close"]),
                volume=int(float(item.get("volume", 0) or 0)),
                symbol=symbol,
                timeframe=timeframe,
                is_closed=True,
            )
        )
    return deduplicate_candles(candles)


def candles_from_storage(rows: Iterable[dict]) -> list[Candle]:
    """Convert rows returned by TickStorage.get_candles into Candle objects."""

    candles = [
        Candle(
            timestamp=int(row["timestamp"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=int(row.get("volume", 0) or 0),
            symbol=str(row["symbol"]),
            timeframe=str(row["timeframe"]),
            is_closed=True,
        )
        for row in rows
    ]
    return deduplicate_candles(candles)


def deduplicate_candles(candles: Iterable[Candle]) -> list[Candle]:
    """Sort candles chronologically and keep one candle per timestamp."""

    by_timestamp = {int(c.timestamp): c for c in candles}
    return [by_timestamp[ts] for ts in sorted(by_timestamp)]


def coverage_for_candles(
    candles: Iterable[Candle],
    *,
    start_epoch: int,
    end_epoch: int,
    timeframe_seconds: int,
) -> HistoricalCoverage:
    """Compute coverage metrics for a candle set."""

    timestamps = sorted({int(c.timestamp) for c in candles if start_epoch <= int(c.timestamp) <= end_epoch})
    return HistoricalCoverage(
        requested_start=start_epoch,
        requested_end=end_epoch,
        first_timestamp=timestamps[0] if timestamps else None,
        last_timestamp=timestamps[-1] if timestamps else None,
        candle_count=len(timestamps),
        expected_candles=expected_candle_count(start_epoch, end_epoch, timeframe_seconds),
        timeframe_seconds=timeframe_seconds,
    )


def load_cached_candles(
    storage: TickStorage,
    *,
    symbol: str,
    timeframe: str,
    start_epoch: int,
    end_epoch: int,
) -> list[Candle]:
    rows = storage.get_candles(symbol, timeframe, start_epoch, end_epoch)
    return candles_from_storage(rows)


async def _attempt_reconnect(client: DerivClient, logger: logging.Logger, max_attempts: int = 5) -> bool:
    """Tente de reconnecter le client Deriv avec backoff exponentiel."""
    for attempt in range(1, max_attempts + 1):
        logger.info(f"Tentative de reconnexion Deriv {attempt}/{max_attempts}")
        success = await client.reconnect()
        if success:
            logger.info("Reconnecte a Deriv")
            return True

        delay = min(10.0, 2.0 ** attempt)
        logger.warning(f"Echec de reconnexion, nouvelle tentative dans {delay:.1f}s")
        await asyncio.sleep(delay)

    logger.error("Impossible de reconnecter a Deriv apres plusieurs essais")
    return False


async def fetch_historical_candles(
    client: DerivClient,
    *,
    symbol: str,
    start_epoch: int,
    end_epoch: int,
    timeframe_seconds: int = 60,
    timeframe: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    storage: Optional[TickStorage] = None,
    request_limit: int = DERIV_CANDLE_PAGE_SIZE,
    request_delay_seconds: float = 0.05,
    timeout_seconds: float = 30.0,
    max_requests: Optional[int] = None,
) -> list[Candle]:
    """Fetch Deriv historical candles by paging backwards from end_epoch."""

    if start_epoch >= end_epoch:
        raise ValueError("start_epoch doit etre < end_epoch")
    if request_limit <= 0:
        raise ValueError("request_limit doit etre > 0")

    logger = logger or logging.getLogger("historical_data")
    timeframe = timeframe or seconds_to_timeframe(timeframe_seconds)
    current_end = align_epoch(end_epoch, timeframe_seconds)
    expected = expected_candle_count(start_epoch, end_epoch, timeframe_seconds)
    if max_requests is None:
        max_requests = max(1, expected // request_limit + 20)

    all_candles: list[Candle] = []
    seen_timestamps: set[int] = set()
    request_count = 0
    consecutive_timeouts = 0

    while current_end >= start_epoch and request_count < max_requests:
        request_count += 1
        payload = {
            "ticks_history": symbol,
            "start": int(start_epoch),
            "end": int(current_end),
            "style": "candles",
            "granularity": int(timeframe_seconds),
            "count": int(request_limit),
            "adjust_start_time": 1,
        }

        resp = await client._send_request(payload, timeout=timeout_seconds)
        if resp is None:
            if not client.is_connected:
                logger.warning("Client Deriv deconnecte pendant le telechargement historique")
                reconnected = await _attempt_reconnect(client, logger)
                if not reconnected:
                    if all_candles:
                        logger.warning(
                            "Reconnexion impossible, retour des bougies deja recuperees (%s)",
                            len(all_candles),
                        )
                        return deduplicate_candles(all_candles)
                    raise TimeoutError("Deconnexion Deriv irreparable pendant le telechargement historique")
                continue

            consecutive_timeouts += 1
            if consecutive_timeouts > 3:
                raise TimeoutError(f"Timeout API Deriv apres {consecutive_timeouts} essais consecutifs")
            logger.warning("Timeout historique Deriv, nouvel essai...")
            await asyncio.sleep(1.0)
            continue
        consecutive_timeouts = 0

        if resp.get("error"):
            err = resp["error"]
            code = err.get("code", "UNKNOWN")
            message = err.get("message", "")
            raise RuntimeError(f"Erreur API Deriv {code}: {message}")

        raw_candles = resp.get("candles", [])
        if not raw_candles:
            logger.info("Plus de bougies disponibles pour %s", symbol)
            break

        batch = [
            candle
            for candle in candles_from_deriv(raw_candles, symbol=symbol, timeframe=timeframe)
            if start_epoch <= int(candle.timestamp) <= end_epoch
        ]
        new_candles = [c for c in batch if int(c.timestamp) not in seen_timestamps]
        for candle in new_candles:
            seen_timestamps.add(int(candle.timestamp))

        if new_candles:
            all_candles.extend(new_candles)
            if storage is not None:
                storage.insert_candles(new_candles)

        oldest_epoch = min(int(c.timestamp) for c in batch) if batch else None
        if oldest_epoch is None:
            break
        if oldest_epoch <= start_epoch:
            break

        next_end = oldest_epoch - timeframe_seconds
        if next_end >= current_end:
            logger.warning("Pagination historique bloquee a epoch=%s", current_end)
            break
        current_end = next_end

        if request_count % 25 == 0 or request_count == 1:
            coverage = coverage_for_candles(
                all_candles,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                timeframe_seconds=timeframe_seconds,
            )
            logger.info(
                "Historique %s: %s/%s bougies (densite %.2f%%, debut courant %s)",
                symbol,
                coverage.candle_count,
                coverage.expected_candles,
                coverage.density_pct,
                datetime.fromtimestamp(current_end, tz=timezone.utc).isoformat(),
            )

        if request_delay_seconds > 0:
            await asyncio.sleep(request_delay_seconds)

    candles = deduplicate_candles(all_candles)
    logger.info("Historique recupere: %s bougies %s pour %s", len(candles), timeframe, symbol)
    return candles
