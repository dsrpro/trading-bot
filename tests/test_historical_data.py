"""Tests for real historical-data backtest plumbing."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.historical_data import (
    build_historical_window,
    candles_from_deriv,
    coverage_for_candles,
    fetch_historical_candles,
    subtract_years,
)
from src.storage import TickStorage


def _raw_candles(count: int, *, start_epoch: int = 0, step: int = 60) -> list[dict]:
    candles = []
    for i in range(count):
        price = 100.0 + i
        candles.append(
            {
                "epoch": start_epoch + i * step,
                "open": price,
                "high": price + 1,
                "low": price - 1,
                "close": price + 0.5,
            }
        )
    return candles


class FakeDerivClient:
    def __init__(self, raw_candles: list[dict]):
        self.raw_candles = raw_candles
        self.requests: list[dict] = []

    async def _send_request(self, payload: dict, timeout: float = 10.0) -> dict:
        self.requests.append(dict(payload))
        start = int(payload["start"])
        end = int(payload["end"])
        count = int(payload["count"])
        available = [c for c in self.raw_candles if start <= int(c["epoch"]) <= end]
        return {"candles": available[-count:]}


def test_subtract_years_handles_leap_day():
    value = datetime(2024, 2, 29, 12, 0, tzinfo=timezone.utc)
    assert subtract_years(value, 1) == datetime(2023, 2, 28, 12, 0, tzinfo=timezone.utc)


def test_build_historical_window_uses_calendar_years_and_last_closed_candle():
    end = datetime(2026, 8, 5, 12, 34, 45, tzinfo=timezone.utc)
    window = build_historical_window(3, 60, end_datetime=end)

    assert window.start_datetime == datetime(2023, 8, 5, 12, 34, tzinfo=timezone.utc)
    assert window.end_datetime == datetime(2026, 8, 5, 12, 33, tzinfo=timezone.utc)
    assert window.years == 3


def test_coverage_rejects_short_history():
    candles = candles_from_deriv(_raw_candles(10), symbol="R_75", timeframe="M1")
    coverage = coverage_for_candles(candles, start_epoch=0, end_epoch=59 * 60, timeframe_seconds=60)

    assert coverage.expected_candles == 60
    assert coverage.candle_count == 10
    assert not coverage.is_sufficient(min_density_pct=90.0)


@pytest.mark.asyncio
async def test_fetch_historical_candles_pages_backwards():
    raw = _raw_candles(12)
    client = FakeDerivClient(raw)

    candles = await fetch_historical_candles(
        client,
        symbol="R_75",
        start_epoch=0,
        end_epoch=11 * 60,
        timeframe_seconds=60,
        timeframe="M1",
        request_limit=5,
        request_delay_seconds=0,
    )

    assert [c.timestamp for c in candles] == [i * 60 for i in range(12)]
    assert len(client.requests) == 3
    assert all(req["style"] == "candles" for req in client.requests)
    assert all(req["granularity"] == 60 for req in client.requests)


def test_storage_insert_counts_only_new_rows(tmp_path):
    storage = TickStorage(db_path=str(tmp_path / "market_data.db"))
    try:
        candles = candles_from_deriv(_raw_candles(2), symbol="R_75", timeframe="M1")

        assert storage.insert_candles(candles) == 2
        assert storage.insert_candles(candles) == 0
        assert storage.count_candles("R_75", "M1") == 2
    finally:
        storage.close()
