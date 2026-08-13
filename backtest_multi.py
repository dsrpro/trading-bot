"""Backtesting multi-symboles — teste la strategie sur tous les indices synthetiques.

Execute le backtest sur chaque symbole, compare les resultats,
et identifie le meilleur indice pour la strategie.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import Config, load_config
from src.logger import setup_logger
from src.deriv_client import DerivClient
from src.backtester import Backtester
from src.historical_data import (
    build_historical_window,
    coverage_for_candles,
    deduplicate_candles,
    fetch_historical_candles,
    load_cached_candles,
    timeframe_to_seconds,
)
from src.storage import TickStorage


# Symboles a tester (indices synthetiques)
SYMBOLS = [
    "1HZ10V",   # Volatility 10
    "1HZ25V",   # Volatility 25
    "1HZ50V",   # Volatility 50
    "1HZ75V",   # Volatility 75
    "1HZ100V",  # Volatility 100
    "BOOM500",  # Boom 500
    "BOOM1000", # Boom 1000
    "CRASH500", # Crash 500
    "CRASH1000",# Crash 1000
    "JD10",     # Jump 10
    "JD25",     # Jump 25
    "JD50",     # Jump 50
    "JD75",     # Jump 75
    "JD100",    # Jump 100
    "R_10",     # Volatility 10 Index
    "R_25",     # Volatility 25 Index
    "R_50",     # Volatility 50 Index
    "R_75",     # Volatility 75 Index
    "R_100",    # Volatility 100 Index
]


def _robustness_score(result: dict) -> float:
    """Calcule un score de robustesse pour le classement des symboles."""
    pf = result.get("profit_factor", 0.0)
    wr = result.get("win_rate", 0.0)
    dd = result.get("max_drawdown_pct", 0.0)
    expectancy = result.get("expectancy", 0.0)
    score = pf * 15.0 + wr - dd * 0.3
    if expectancy > 0:
        score += 10.0
    if result.get("total_trades", 0) >= 30:
        score += 5.0
    return score


def _print_coverage(symbol: str, coverage) -> None:
    print(f"  {symbol:<8s} couverture: {coverage.candle_count} bougies / {coverage.expected_candles} attendues | densite={coverage.density_pct:.1f}% | span={coverage.span_coverage_pct:.1f}%")


async def async_main():
    config = load_config()
    logger = setup_logger(config, "backtest_multi")

    # Tentative de connexion API pour recuperer des donnees fraiches
    client = DerivClient(config, logger)
    try:
        connected = await client.connect()
    except Exception:
        connected = False

    storage = TickStorage()
    backtester = Backtester(config, logger)

    results = []

    print("=" * 80)
    print("  BACKTESTING MULTI-SYMBOLES — INDICES SYNTHETIQUES DERIV")
    print("=" * 80)
    print(f"  {len(SYMBOLS)} symboles a tester")
    print(f"  Strategie: Bollinger(20,2) + RSI(25/75) + EMA(50/200) + Rejection")
    print(f"  Capital: ${config.backtest_initial_capital:.0f}")
    print()

    years = 1
    timeframe = "M1"
    timeframe_seconds = timeframe_to_seconds(timeframe)
    window = build_historical_window(years, timeframe_seconds)

    for i, symbol in enumerate(SYMBOLS):
        print(f"[{i+1}/{len(SYMBOLS)}] {symbol} ...")

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
            _print_coverage(symbol, coverage)
        else:
            print(f"  Aucune donnees cachees pour {symbol}")

        if not coverage.is_sufficient(min_span_coverage_pct=95.0, min_density_pct=90.0):
            if not connected:
                print("  SKIP: Connexion Deriv absente et historique incomplet")
                continue

            print(f"  Donnees incompletes pour {symbol}, tentative de telechargement Deriv")
            try:
                fetched = await fetch_historical_candles(
                    client,
                    symbol=symbol,
                    start_epoch=window.start_epoch,
                    end_epoch=window.end_epoch,
                    timeframe_seconds=timeframe_seconds,
                    timeframe=timeframe,
                    logger=logger,
                    storage=storage,
                    request_limit=1000,
                    request_delay_seconds=0.1,
                    timeout_seconds=30.0,
                )
                if fetched:
                    print(f"  Telechargement: {len(fetched)} bougies ajoutees")
            except Exception as exc:
                print(f"  ERREUR telechargement Deriv: {exc}")

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
                _print_coverage(symbol, coverage)

        if not coverage.is_sufficient(min_span_coverage_pct=95.0, min_density_pct=90.0):
            print(
                f"  SKIP: historique insuffisant ({coverage.density_pct:.1f}% densite, span {coverage.span_coverage_pct:.1f}%)"
            )
            continue

        candles = deduplicate_candles(candles)
        if len(candles) < 1000:
            print(f"  SKIP: trop peu de bougies valides ({len(candles)})")
            continue

        print(f"  [VALID] {len(candles)} bougies chargees, execution du backtest")
        result = backtester.run(candles)
        result_data = {
            "symbol": symbol,
            "candles": len(candles),
            "win_rate": result.win_rate * 100.0,
            "profit_factor": result.profit_factor,
            "total_return_pct": result.total_return_pct,
            "max_drawdown_pct": result.max_drawdown_pct,
            "sharpe_ratio": result.sharpe_ratio,
            "total_trades": result.total_trades,
            "expectancy": result.expectancy,
            "robustness_score": 0.0,
        }
        result_data["robustness_score"] = _robustness_score(result_data)
        results.append(result_data)

        print(
            f"  OK | trades={result.total_trades} | WR={result_data['win_rate']:.1f}% | "
            f"PF={result_data['profit_factor']:.2f} | DD={result_data['max_drawdown_pct']:.2f}% | "
            f"Score={result_data['robustness_score']:.1f}"
        )

    if client:
        try:
            await client.disconnect()
        except Exception:
            pass

    storage.close()

    # Affichage du classement
    if not results:
        print("\n[AUCUN RESULTAT] Aucun symbole n'a pu etre teste.")
        return

    # Tri par score de robustesse decroissant
    results.sort(key=lambda r: r.get("robustness_score", 0), reverse=True)

    print("\n" + "=" * 100)
    print("  CLASSEMENT — MEILLEURS INDICES POUR LA STRATEGIE (1 an) ")
    print("=" * 100)
    print(f"  {'Rang':<5s} {'Symbole':<15s} {'Trades':<8s} {'WinRate':<8s} {'PF':<8s} {'DD':<8s} {'Sharpe':<8s} {'RR':<6s} {'Score':<8s}")
    print("-" * 100)

    for rank, r in enumerate(results, 1):
        symbol = r["symbol"]
        trades = r.get("total_trades", 0)
        win_rate = r.get("win_rate", 0)
        pf = r.get("profit_factor", 0)
        dd = r.get("max_drawdown_pct", 0)
        sharpe = r.get("sharpe_ratio", 0)
        ret = r.get("total_return_pct", 0)
        score = r.get("robustness_score", 0)
        print(
            f"  {rank:<5d} {symbol:<15s} {trades:<8d} {win_rate:<7.1f}% {pf:<7.2f} {dd:<7.2f}% {sharpe:<7.2f} {ret:+<6.0f}% {score:<7.1f}"
        )

    print("=" * 100)

    best = results[0]
    print(f"\n[MEILLEUR] {best['symbol']} — Score={best['robustness_score']:.1f}, "
          f"PF={best.get('profit_factor', 0):.2f}, WR={best.get('win_rate', 0):.1f}%")
    print(f"  → Conseil: tester ce symbole en production demo sur 1 mois.")

    output = Path(config.base_dir) / "backtest_multi_results.json"
    with open(output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResultats sauvegardes: {output}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()