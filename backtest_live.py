"""Backtesting Phase 1 — 3 ans d'historique reel via l'API Deriv.

Recupere l'historique des prix, construit les chandeliers M1,
puis execute le backtester complet sur ces donnees reelles.
"""

import asyncio
import sys
import json
import time as _time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import Config, load_config
from src.logger import setup_logger
from src.deriv_client import DerivClient
from src.candle_builder import Candle, CandleBuilder
from src.backtester import Backtester


async def fetch_historical_ticks(
    client: DerivClient,
    symbol: str,
    start_epoch: int,
    end_epoch: int,
    logger
) -> list[dict]:
    """Recupere l'historique des ticks entre deux timestamps.

    L'API limite a ~5000 ticks par requete, donc on pagine.
    Chaque tick a une granularite de 1 seconde pour les indices 1s.

    Args:
        client: Client Deriv connecte.
        symbol: Symbole (ex: "1HZ100V").
        start_epoch: Timestamp de debut (secondes).
        end_epoch: Timestamp de fin (secondes).
        logger: Logger.

    Returns:
        Liste de dictionnaires de ticks tries chronologiquement.
    """
    all_ticks = []
    current_end = end_epoch
    max_requests = 500  # Securite
    request_count = 0

    logger.info(f"Recuperation historique {symbol}: "
                f"{datetime.fromtimestamp(start_epoch)} -> {datetime.fromtimestamp(end_epoch)}")

    while current_end > start_epoch and request_count < max_requests:
        request_count += 1
        payload = {
            "ticks_history": symbol,
            "end": str(current_end),
            "start": start_epoch,
            "adjust_start_time": 1,
            "count": 5000,
        }

        resp = await client._send_request(payload, timeout=30.0)

        if resp is None:
            logger.warning(f"Timeout requete #{request_count}, reessai...")
            await asyncio.sleep(1)
            continue

        if resp.get("error"):
            err = resp["error"]
            logger.error(f"Erreur API requete #{request_count}: {err.get('code')} - {err.get('message')}")
            break

        history = resp.get("history", {})
        prices = history.get("prices", [])
        times = history.get("times", [])

        if not prices:
            logger.info(f"Plus de ticks disponibles (requete #{request_count})")
            break

        for i, price in enumerate(prices):
            epoch = int(times[i]) if i < len(times) else current_end
            all_ticks.append({
                "epoch": epoch,
                "quote": float(price),
                "symbol": symbol,
            })

        # Mise a jour: nouvelle fin = timestamp du premier tick recupere - 1
        if times and len(times) > 0:
            current_end = int(times[0]) - 1
        else:
            break

        elapsed = end_epoch - current_end
        pct = min(100.0, (elapsed / (end_epoch - start_epoch)) * 100.0) if end_epoch > start_epoch else 100.0
        if request_count % 10 == 0:
            logger.info(f"  Progression: {pct:.0f}% | {len(all_ticks)} ticks | "
                        f"periode couverte: {datetime.fromtimestamp(current_end)}")

        await asyncio.sleep(0.5)  # Rate limiting

    # Trier par epoch croissant
    all_ticks.sort(key=lambda t: t["epoch"])
    logger.info(f"Historique recupere: {len(all_ticks)} ticks au total")
    return all_ticks


def ticks_to_candles(ticks: list[dict], timeframe_seconds: int = 60) -> list[Candle]:
    """Convertit des ticks en chandeliers OHLC.

    Args:
        ticks: Liste de dictionnaires de ticks (avec 'epoch', 'quote').
        timeframe_seconds: Duree d'un chandelier en secondes (60 = M1).

    Returns:
        Liste de Candle tries chronologiquement.
    """
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
            # Fermer le chandelier precedent
            if current_open is not None:
                candles.append(Candle(
                    timestamp=current_start,
                    open=current_open,
                    high=current_high,
                    low=current_low,
                    close=current_close,
                    volume=volume,
                    symbol=tick.get("symbol", ""),
                    timeframe=f"M{timeframe_seconds // 60}",
                    is_closed=True,
                ))

            # Ouvrir un nouveau chandelier
            current_start = candle_start
            current_open = price
            current_high = price
            current_low = price
            current_close = price
            volume = 1
        else:
            # Meme chandelier: mise a jour
            current_high = max(current_high, price)
            current_low = min(current_low, price)
            current_close = price
            volume += 1

    # Ne pas oublier le dernier chandelier
    if current_open is not None:
        candles.append(Candle(
            timestamp=current_start,
            open=current_open,
            high=current_high,
            low=current_low,
            close=current_close,
            volume=volume,
            symbol=ticks[-1].get("symbol", ""),
            timeframe=f"M{timeframe_seconds // 60}",
            is_closed=True,
        ))

    return candles


async def main():
    config = load_config()
    logger = setup_logger(config, "backtest_live")

    # Parametres du backtest
    SYMBOL = "1HZ100V"  # Volatility 100 Index (1s)
    YEARS = 3

    # Calcul des timestamps
    end_epoch = int(_time.time())
    start_epoch = end_epoch - YEARS * 365 * 24 * 3600

    print("=" * 70)
    print("  BACKTESTING PHASE 1 — 3 ANS D'HISTORIQUE REEL DERIV")
    print("=" * 70)
    print(f"  Symbole    : {SYMBOL} (Volatility 100 Index)")
    print(f"  Periode    : {datetime.fromtimestamp(start_epoch).strftime('%Y-%m-%d')} -> "
          f"{datetime.fromtimestamp(end_epoch).strftime('%Y-%m-%d')} ({YEARS} ans)")
    print(f"  Timeframe  : M1")
    print(f"  Capital    : ${config.backtest_initial_capital:.2f}")
    print(f"  Strategie  : Bollinger(20,2) + RSI(14) + Rejection Candles")
    print(f"  Risk Mgt   : {config.risk_per_trade_pct}%/trade, {config.daily_stop_loss_pct}% daily SL")
    print()

    # 1. Connexion API Deriv
    print("[1/4] Connexion a l'API Deriv...")
    client = DerivClient(config, logger)
    connected = await client.connect()
    if not connected:
        print("[FAIL] Impossible de se connecter a l'API Deriv")
        return
    print("[OK] Connecte au endpoint public\n")

    # 2. Recuperation de l'historique
    print("[2/4] Recuperation de l'historique des ticks...")
    print(f"  Ceci peut prendre plusieurs minutes pour {YEARS} ans de donnees...")
    t0 = _time.time()
    ticks = await fetch_historical_ticks(client, SYMBOL, start_epoch, end_epoch, logger)
    elapsed = _time.time() - t0
    await client.disconnect()

    if len(ticks) < 1000:
        print(f"[FAIL] Seulement {len(ticks)} ticks recuperes — donnees insuffisantes")
        print("  L'API peut limiter l'historique disponible.")
        print("  Essayez avec un timeframe plus court (ex: 1 an).")
        return

    print(f"[OK] {len(ticks)} ticks recuperes en {elapsed:.0f}s\n")

    # 3. Construction des chandeliers M1
    print("[3/4] Construction des chandeliers M1...")
    candles = ticks_to_candles(ticks, timeframe_seconds=60)
    print(f"[OK] {len(candles)} chandeliers M1 generes")
    if candles:
        print(f"  Premier: {datetime.fromtimestamp(candles[0].timestamp)} | "
              f"O={candles[0].open:.2f} H={candles[0].high:.2f} "
              f"L={candles[0].low:.2f} C={candles[0].close:.2f}")
        print(f"  Dernier: {datetime.fromtimestamp(candles[-1].timestamp)} | "
              f"O={candles[-1].open:.2f} H={candles[-1].high:.2f} "
              f"L={candles[-1].low:.2f} C={candles[-1].close:.2f}")
    print()

    # 4. Backtesting
    print("[4/4] Execution du backtesting...")
    backtester = Backtester(config, logger)
    t0 = _time.time()
    result = backtester.run(candles)
    elapsed = _time.time() - t0

    # Affichage des resultats
    print()
    print("=" * 70)
    print("           RESULTATS DU BACKTEST — 3 ANS D'HISTORIQUE")
    print("=" * 70)
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
        value = d.get(key, "N/A")
        print(f"  {label:<30s}: {value}")

    print(f"\n  Duree backtest         : {elapsed:.1f}s")
    print(f"  Bougies traitees       : {len(candles)}")
    print("=" * 70)

    # Interpretation
    print("\n--- INTERPRETATION (Plan 1, Section 5.1) ---")
    if result.total_trades < 30:
        print("⚠ Trop peu de trades (< 30) — evaluation statistique peu fiable.")
    elif result.profit_factor > 1.5 and result.sharpe_ratio > 1.0 and result.max_drawdown_pct < 20:
        print("✅ La strategie satisfait les criteres du Plan 1:")
        print(f"   - Profit factor > 1.5: {result.profit_factor:.2f}")
        print(f"   - Sharpe ratio > 1.0:  {result.sharpe_ratio:.2f}")
        print(f"   - Drawdown < 20%:      {result.max_drawdown_pct:.2f}%")
        print("   → Eligible pour la Phase 2 (Paper Trading)")
    else:
        print("⚠ La strategie necessite des ajustements avant la Phase 2.")
        print(f"   Profit factor: {result.profit_factor:.2f} (cible > 1.5)")
        print(f"   Sharpe ratio:  {result.sharpe_ratio:.2f} (cible > 1.0)")
        print(f"   Drawdown max:  {result.max_drawdown_pct:.2f}% (cible < 20%)")

    # Sauvegarder les resultats
    output_file = Path(config.base_dir) / "backtest_3ans_results.json"
    with open(output_file, "w") as f:
        json.dump({
            "metadata": {
                "symbol": SYMBOL,
                "years": YEARS,
                "start_date": datetime.fromtimestamp(start_epoch).isoformat(),
                "end_date": datetime.fromtimestamp(end_epoch).isoformat(),
                "total_candles": len(candles),
                "total_ticks": len(ticks),
                "strategy": "Bollinger(20,2) + RSI(14) + Rejection Candles",
            },
            "results": d,
            "trades": result.trade_list[:50],  # 50 premiers trades
        }, f, indent=2)
    print(f"\nResultats sauvegardes dans: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())