"""Backtesting multi-symboles — teste la strategie sur tous les indices synthetiques.

Execute le backtest sur chaque symbole, compare les resultats,
et identifie le meilleur indice pour la strategie.
"""

import asyncio
import sys
import json
import time as _time
from datetime import datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import Config, load_config
from src.logger import setup_logger
from src.deriv_client import DerivClient
from src.candle_builder import Candle, CandleBuilder
from src.backtester import Backtester
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


def ticks_to_candles(ticks: list[dict], timeframe_seconds: int = 60) -> list[Candle]:
    """Convertit des ticks en chandeliers OHLC."""
    if not ticks:
        return []
    candles = []
    current_start = (ticks[0]["epoch"] // timeframe_seconds) * timeframe_seconds
    current_open = None
    current_high = float("-inf")
    current_low = float("inf")
    current_close = None
    volume = 0

    for tick in ticks:
        epoch = tick["epoch"]
        price = float(tick["quote"])
        candle_start = (epoch // timeframe_seconds) * timeframe_seconds

        if candle_start != current_start:
            if current_open is not None:
                candles.append(Candle(
                    timestamp=current_start, open=current_open, high=current_high,
                    low=current_low, close=current_close, volume=volume,
                    symbol=tick.get("symbol", ""), timeframe=f"M{timeframe_seconds // 60}",
                    is_closed=True,
                ))
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
        candles.append(Candle(
            timestamp=current_start, open=current_open, high=current_high,
            low=current_low, close=current_close, volume=volume,
            symbol=ticks[-1].get("symbol", ""), timeframe=f"M{timeframe_seconds // 60}",
            is_closed=True,
        ))
    return candles


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

    for i, symbol in enumerate(SYMBOLS):
        print(f"[{i+1}/{len(SYMBOLS)}] {symbol} ...", end=" ", flush=True)

        # Recuperer les ticks depuis le stockage local OU l'API
        ticks = []
        end_epoch = int(_time.time())
        start_epoch = end_epoch - 2 * 24 * 3600  # 2 jours

        # D'abord le stockage local
        local_ticks = storage.get_ticks(symbol, start_epoch, end_epoch)

        if len(local_ticks) > 1000:
            ticks = local_ticks
        elif connected:
            # Format valide: {"ticks_history": SYM, "end": "latest", "subscribe": 0}
            try:
                resp = await client._send_request(
                    {"ticks_history": symbol, "end": "latest", "subscribe": 0},
                    timeout=30.0,
                )
                if resp and "history" in resp:
                    prices = resp["history"].get("prices", [])
                    times = resp["history"].get("times", [])
                    for j, price in enumerate(prices):
                        epoch = int(times[j]) if j < len(times) else end_epoch
                        ticks.append({"epoch": epoch, "symbol": symbol, "quote": float(price)})
                    if ticks:
                        storage.insert_ticks(ticks)
                elif resp and resp.get("error"):
                    pass  # Symbole non supporte par l'API
            except Exception:
                pass

        if len(ticks) < 500:
            print(f"SKIP ({len(ticks)} ticks)")
            continue

        # Construction chandeliers
        candles = ticks_to_candles(ticks)

        if len(candles) < 50:
            print(f"SKIP ({len(candles)} bougies)")
            continue

        # Backtesting
        result = backtester.run(candles)
        results.append({
            "symbol": symbol,
            "candles": len(candles),
            "ticks": len(ticks),
            **result.to_dict(),
        })

        win_rate = result.to_dict().get("win_rate", 0)
        profit_factor = result.to_dict().get("profit_factor", 0)
        print(f"OK | {result.total_trades} trades | WR={win_rate}% | PF={profit_factor:.2f}")

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

    # Tri par profit factor decroissant
    results.sort(key=lambda r: r.get("profit_factor", 0), reverse=True)

    print("\n" + "=" * 80)
    print("  CLASSEMENT — MEILLEURS INDICES POUR LA STRATEGIE")
    print("=" * 80)
    print(f"  {'Rang':<5s} {'Symbole':<15s} {'Trades':<8s} {'WinRate':<10s} {'Rendement':<12s} {'Drawdown':<10s} {'Prof.Fact':<10s} {'Sharpe':<8s}")
    print("-" * 80)

    for rank, r in enumerate(results, 1):
        symbol = r["symbol"]
        trades = r.get("total_trades", 0)
        win_rate = r.get("win_rate", 0)
        ret = r.get("total_return_pct", 0)
        dd = r.get("max_drawdown_pct", 0)
        pf = r.get("profit_factor", 0)
        sharpe = r.get("sharpe_ratio", 0)
        print(f"  {rank:<5d} {symbol:<15s} {trades:<8d} {win_rate:<9.1f}% {ret:+<11.2f}% {dd:<9.2f}% {pf:<9.2f} {sharpe:<7.2f}")

    print("=" * 80)

    # Meilleur symbole
    best = results[0]
    print(f"\n[MEILLEUR] {best['symbol']} — Profit Factor={best.get('profit_factor', 0):.2f}, "
          f"Rendement={best.get('total_return_pct', 0):.2f}%")
    print(f"  → Utilisez ce symbole dans config/settings.env: MARKET_SYMBOL={best['symbol']}")

    # Sauvegarde
    output = Path(config.base_dir) / "backtest_multi_results.json"
    with open(output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResultats sauvegardes: {output}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()