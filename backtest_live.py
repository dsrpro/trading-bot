"""Backtesting Phase 1 on real Deriv historical candles.

This script uses three calendar years of OHLC candles by default.  It avoids
the old tick-by-tick download path because three years of second-level ticks is
too large and previously produced only a few days of usable M1 candles.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time as _time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.backtester import Backtester
from src.candle_builder import Candle
from src.config import load_config
from src.deriv_client import DerivClient
from src.historical_data import (
    build_historical_window,
    coverage_for_candles,
    deduplicate_candles,
    fetch_historical_candles,
    load_cached_candles,
    seconds_to_timeframe,
    timeframe_to_seconds,
)
from src.logger import setup_logger
from src.storage import TickStorage


def ticks_to_candles(ticks: list[dict], timeframe_seconds: int = 60) -> list[Candle]:
    """Convert legacy tick dictionaries into OHLC candles.

    Kept for manual experiments, but the real three-year backtest fetches
    Deriv candles directly through fetch_historical_candles().
    """

    if not ticks:
        return []

    timeframe = seconds_to_timeframe(timeframe_seconds)
    candles: list[Candle] = []
    current_start = (int(ticks[0]["epoch"]) // timeframe_seconds) * timeframe_seconds
    current_open = None
    current_high = float("-inf")
    current_low = float("inf")
    current_close = None
    volume = 0

    for tick in ticks:
        epoch = int(tick["epoch"])
        price = float(tick["quote"])
        candle_start = (epoch // timeframe_seconds) * timeframe_seconds

        if candle_start != current_start:
            if current_open is not None:
                candles.append(
                    Candle(
                        timestamp=current_start,
                        open=current_open,
                        high=current_high,
                        low=current_low,
                        close=current_close,
                        volume=volume,
                        symbol=tick.get("symbol", ""),
                        timeframe=timeframe,
                        is_closed=True,
                    )
                )

            current_start = candle_start
            current_open = price
            current_high = price
            current_low = price
            current_close = price
            volume = 1
        else:
            current_high = max(current_high, price)
            current_low = min(current_low, price)
            current_close = price
            volume += 1

    if current_open is not None:
        candles.append(
            Candle(
                timestamp=current_start,
                open=current_open,
                high=current_high,
                low=current_low,
                close=current_close,
                volume=volume,
                symbol=ticks[-1].get("symbol", ""),
                timeframe=timeframe,
                is_closed=True,
            )
        )

    return candles


def _format_epoch(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _print_coverage(coverage) -> None:
    print(f"  Bougies attendues : {coverage.expected_candles:,}".replace(",", " "))
    print(f"  Bougies chargees  : {coverage.candle_count:,}".replace(",", " "))
    print(f"  Couverture span   : {coverage.span_coverage_pct:.2f}%")
    print(f"  Densite bougies   : {coverage.density_pct:.2f}%")
    if coverage.first_timestamp is not None and coverage.last_timestamp is not None:
        print(f"  Premiere bougie   : {_format_epoch(coverage.first_timestamp)}")
        print(f"  Derniere bougie   : {_format_epoch(coverage.last_timestamp)}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest Deriv sur historique reel")
    parser.add_argument("--symbol", default=None, help="Symbole Deriv, defaut: MARKET_SYMBOL")
    parser.add_argument("--years", type=int, default=None, help="Nombre d'annees, defaut: BACKTEST_YEARS")
    parser.add_argument("--timeframe", default=None, help="Timeframe, defaut: TIMEFRAME")
    parser.add_argument("--refresh", action="store_true", help="Ignorer le cache SQLite existant")
    parser.add_argument("--min-density-pct", type=float, default=90.0, help="Densite minimale exigee")
    parser.add_argument("--min-span-pct", type=float, default=99.0, help="Couverture temporelle minimale exigee")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    logger = setup_logger(config, "backtest_live")

    symbol = args.symbol or config.market_symbol
    years = args.years if args.years is not None else config.backtest_years
    timeframe = (args.timeframe or config.timeframe).upper()
    timeframe_seconds = timeframe_to_seconds(timeframe)
    timeframe = seconds_to_timeframe(timeframe_seconds)
    window = build_historical_window(years, timeframe_seconds)

    print("=" * 76)
    print("  BACKTESTING PHASE 1 - HISTORIQUE REEL DERIV")
    print("=" * 76)
    print(f"  Symbole    : {symbol}")
    print(f"  Periode    : {_format_epoch(window.start_epoch)} -> {_format_epoch(window.end_epoch)}")
    print(f"  Duree      : {years} ans calendaires")
    print(f"  Timeframe  : {timeframe}")
    print(f"  Capital    : ${config.backtest_initial_capital:.2f}")
    print("  Source     : Deriv candles API + cache SQLite")
    print()

    storage = TickStorage(logger=logger)
    candles: list[Candle] = []

    try:
        if not args.refresh:
            candles = load_cached_candles(
                storage,
                symbol=symbol,
                timeframe=timeframe,
                start_epoch=window.start_epoch,
                end_epoch=window.end_epoch,
            )

        coverage = coverage_for_candles(
            candles,
            start_epoch=window.start_epoch,
            end_epoch=window.end_epoch,
            timeframe_seconds=timeframe_seconds,
        )

        if candles:
            print("[1/4] Cache SQLite detecte")
            _print_coverage(coverage)
            print()
        else:
            print("[1/4] Cache SQLite vide pour cette fenetre")

        if not coverage.is_sufficient(
            min_span_coverage_pct=args.min_span_pct,
            min_density_pct=args.min_density_pct,
        ):
            fetch_end_epoch = window.end_epoch
            if candles and coverage.last_gap_seconds is not None and coverage.last_gap_seconds <= 24 * 3600:
                oldest_cached = min(int(c.timestamp) for c in candles)
                if oldest_cached > window.start_epoch:
                    fetch_end_epoch = oldest_cached - timeframe_seconds
                    print(f"[2/4] Reprise du telechargement avant {_format_epoch(oldest_cached)}")
                else:
                    print("[2/4] Cache incomplet, retentative API sur toute la fenetre")
            else:
                print("[2/4] Telechargement de l'historique Deriv")

            client = DerivClient(config, logger)
            connected = await client.connect()
            if not connected:
                print("[FAIL] Impossible de se connecter a l'API Deriv")
                return 1

            t0 = _time.time()
            try:
                fetched = await fetch_historical_candles(
                    client,
                    symbol=symbol,
                    start_epoch=window.start_epoch,
                    end_epoch=fetch_end_epoch,
                    timeframe_seconds=timeframe_seconds,
                    timeframe=timeframe,
                    logger=logger,
                    storage=storage,
                )
            finally:
                await client.disconnect()

            elapsed = _time.time() - t0
            print(f"  Bougies recuperees API : {len(fetched):,} en {elapsed:.0f}s".replace(",", " "))

            candles = load_cached_candles(
                storage,
                symbol=symbol,
                timeframe=timeframe,
                start_epoch=window.start_epoch,
                end_epoch=window.end_epoch,
            )
            coverage = coverage_for_candles(
                candles,
                start_epoch=window.start_epoch,
                end_epoch=window.end_epoch,
                timeframe_seconds=timeframe_seconds,
            )
            print()
        else:
            print("[2/4] Cache suffisant, pas d'appel API necessaire")
            print()

        candles = deduplicate_candles(candles)

        print("[3/4] Validation couverture 3 ans")
        _print_coverage(coverage)
        if not coverage.is_sufficient(
            min_span_coverage_pct=args.min_span_pct,
            min_density_pct=args.min_density_pct,
        ):
            print("[FAIL] Historique insuffisant pour un vrai backtest 3 ans.")
            print("       Le backtest est volontairement bloque pour eviter un resultat trompeur.")
            return 2
        print("[OK] Couverture historique validee")
        print()

        print("[4/4] Execution du backtesting")
        backtester = Backtester(config, logger)
        t0 = _time.time()
        result = backtester.run(candles)
        elapsed = _time.time() - t0

        print()
        print("=" * 76)
        print("           RESULTATS DU BACKTEST - HISTORIQUE REEL")
        print("=" * 76)
        d = result.to_dict()
        labels = {
            "total_trades": "Total trades",
            "winning_trades": "Trades gagnants",
            "losing_trades": "Trades perdants",
            "win_rate": "Win rate (%)",
            "initial_capital": "Capital initial ($)",
            "final_capital": "Capital final ($)",
            "total_return_pct": "Rendement total (%)",
            "total_pnl": "P&L total ($)",
            "max_drawdown_pct": "Drawdown max (%)",
            "sharpe_ratio": "Sharpe ratio",
            "sortino_ratio": "Sortino ratio",
            "profit_factor": "Profit factor",
            "avg_win": "Gain moyen ($)",
            "avg_loss": "Perte moyenne ($)",
            "expectancy": "Esperance ($)",
            "largest_win": "Plus gros gain ($)",
            "largest_loss": "Plus grosse perte ($)",
        }
        for key, label in labels.items():
            print(f"  {label:<30s}: {d.get(key, 'N/A')}")
        print(f"\n  Duree backtest         : {elapsed:.1f}s")
        print(f"  Bougies traitees       : {len(candles):,}".replace(",", " "))
        print("=" * 76)

        print("\n--- INTERPRETATION ---")
        if result.total_trades < 30:
            print("Trop peu de trades (< 30) : evaluation statistique peu fiable.")
        elif result.profit_factor > 1.5 and result.sharpe_ratio > 1.0 and result.max_drawdown_pct < 20:
            print("La strategie satisfait les criteres de passage en Phase 2.")
        else:
            print("La strategie necessite des ajustements avant la Phase 2.")
            print(f"Profit factor: {result.profit_factor:.2f} (cible > 1.5)")
            print(f"Sharpe ratio : {result.sharpe_ratio:.2f} (cible > 1.0)")
            print(f"Drawdown max : {result.max_drawdown_pct:.2f}% (cible < 20%)")

        output_file = Path(config.base_dir) / "backtest_3ans_results.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "metadata": {
                        "symbol": symbol,
                        "years": years,
                        "timeframe": timeframe,
                        "start_date": window.start_datetime.isoformat(),
                        "end_date": window.end_datetime.isoformat(),
                        "total_candles": len(candles),
                        "source": "Deriv candles API + SQLite cache",
                        "strategy": "Bollinger(20,2) + RSI(14) + Rejection Candles",
                        "coverage": coverage.to_dict(),
                    },
                    "results": d,
                    "trades": result.trade_list[:50],
                },
                f,
                indent=2,
            )
        print(f"\nResultats sauvegardes dans: {output_file}")
        return 0
    finally:
        storage.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
