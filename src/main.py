"""Point d'entree principal du bot de trading.

Supporte les modes:
    - backtest: Executer un backtesting sur donnees synthetiques
    - dry-run: Lancer le bot en simulation sans API
    - paper: Lancer le bot avec connexion API demo Deriv
    - report: Afficher un rapport de risque
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from src.backtester import Backtester
from src.candle_builder import CandleBuilder
from src.config import Config, load_config
from src.data_streamer import DataStreamer
from src.indicators import Indicators
from src.logger import setup_logger
from src.order_executor import OrderExecutor
from src.paper_trading_phase2 import PaperTradingPhase2
from src.risk_manager import RiskManager
from src.strategy_engine import SignalDirection, StrategyEngine


class TradingBot:
    """Orchestrateur principal du bot de trading.

    Coordonne tous les modules :
    DataStreamer -> CandleBuilder -> Indicators -> StrategyEngine
    -> RiskManager -> OrderExecutor.
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logger(config, "trading_bot")

        # Modules core
        self.data_streamer = DataStreamer(config, self.logger)
        self.candle_builder = CandleBuilder(config, self.logger)
        self.indicators = Indicators(config, self.logger)
        self.strategy_engine = StrategyEngine(config, self.indicators, self.candle_builder, self.logger)
        self.risk_manager = RiskManager(config, self.logger)
        self.order_executor = OrderExecutor(config, logger=self.logger)

        # Etat
        self._running = False
        self._ticks_processed = 0
        self._signals_generated = 0
        self._trades_executed = 0

    async def run_dry_run(self, duration_minutes: int = 30, tick_interval: float = 0.1) -> None:
        """Execute le bot en mode dry-run (simulation complete).

        Genere des ticks synthetiques et simule le cycle de trading complet.

        Args:
            duration_minutes: Duree de la simulation en minutes.
            tick_interval: Intervalle entre les ticks en secondes.
        """
        self.logger.info(f"=== DEMARRAGE DRY-RUN ({duration_minutes} min) ===")
        self._running = True

        # Generateur de ticks synthetiques (mouvement brownien)
        import numpy as np
        import time as _time

        np.random.seed(42)
        price = 100.0
        start_time = _time.time()
        end_time = start_time + duration_minutes * 60

        # Connecter le data streamer au candle builder
        self.data_streamer.subscribe(lambda tick: self.candle_builder.process_tick(tick))

        # Callback a chaque bougie fermee pour evaluer la strategie
        async def on_candle_closed(candle):
            nonlocal self
            # Evaluer la strategie
            signal = self.strategy_engine.evaluate()
            self._signals_generated += 1

            if signal.is_valid:
                # Verifier les regles de risque
                can_trade, report = self.risk_manager.can_place_trade(signal)
                if can_trade:
                    # Executer l'ordre
                    order = await self.order_executor.execute_signal(signal, report.position_size)
                    if order:
                        self.risk_manager.on_trade_opened(order.amount)
                        self._trades_executed += 1
                        self.logger.info(f"[EXECUTION] Trade #{self._trades_executed} | {signal.direction.value} @ {signal.entry_price:.5f}")

            # Verifier les ordres ouverts (SL/TP)
            for order in self.order_executor.active_orders[:]:
                closed = await self.order_executor.simulate_price_movement(order, candle.close)
                if closed:
                    self.risk_manager.on_trade_closed(closed.pnl, closed.exit_price, closed.entry_price)

            # Log du rapport toutes les 50 bougies
            if self.candle_builder.count() % 50 == 0:
                report = self.risk_manager.get_report()
                self.logger.info(
                    f"[STATUS] Capital=${report.current_capital:.2f} | "
                    f"Daily PnL=${report.daily_pnl:.2f} ({report.daily_pnl_pct:.2f}%) | "
                    f"DD={report.drawdown_pct:.2f}% | Trades today={report.trades_today} | "
                    f"WinRate={self.risk_manager.win_rate*100:.1f}%"
                )

        self.candle_builder.on_candle_close(on_candle_closed)

        tick_count = 0
        while self._running and _time.time() < end_time:
            # Generer un tick synthetique
            returns = np.random.normal(0, 0.0003)  # ~0.03% volatilite par tick
            cycle = 0.002 * np.sin(2 * np.pi * tick_count / 500)
            returns += cycle / 500
            price *= (1 + returns)

            tick_data = {
                "epoch": _time.time(),
                "quote": round(price, 5),
                "symbol": self.config.market_symbol,
            }

            self.data_streamer.on_tick(tick_data)
            self._ticks_processed += 1
            tick_count += 1

            await asyncio.sleep(tick_interval)

        # Rapport final
        self._print_final_report()

    def _print_final_report(self) -> None:
        """Affiche le rapport final du bot."""
        report = self.risk_manager.get_report()
        self.logger.info("=" * 60)
        self.logger.info("           RAPPORT FINAL DU BOT")
        self.logger.info("=" * 60)
        self.logger.info(f"Capital initial:    ${report.initial_capital:.2f}")
        self.logger.info(f"Capital final:      ${report.current_capital:.2f}")
        self.logger.info(f"P&L total:          ${report.total_pnl:+.2f} ({report.total_pnl_pct:+.2f}%)")
        self.logger.info(f"P&L quotidien:      ${report.daily_pnl:+.2f} ({report.daily_pnl_pct:+.2f}%)")
        self.logger.info(f"Drawdown max:       {report.drawdown_pct:.2f}%")
        self.logger.info(f"Peak capital:       ${report.peak_capital:.2f}")
        self.logger.info(f"Total trades:       {self.risk_manager.total_trades}")
        self.logger.info(f"Win rate:           {self.risk_manager.win_rate*100:.1f}%")
        self.logger.info(f"Ticks traites:      {self._ticks_processed}")
        self.logger.info(f"Signaux generes:    {self._signals_generated}")
        self.logger.info(f"Trades executes:    {self._trades_executed}")
        self.logger.info(f"Kill switch:        {'ACTIF' if report.status.value != 'OK' else 'Inactif'}")
        self.logger.info("=" * 60)

    def stop(self) -> None:
        """Arrete le bot proprement."""
        self.logger.info("Arret du bot demande...")
        self._running = False


def cmd_backtest(config: Config) -> None:
    """Commande backtest."""
    logger = setup_logger(config, "backtest")
    logger.info("=== BACKTESTING ===")

    backtester = Backtester(config, logger)
    logger.info("Generation de donnees synthetiques (5000 bougies M1)...")
    candles = backtester.generate_sample_data(n_candles=5000, volatility=0.001)
    logger.info(f"Donnees generees: {len(candles)} bougies")

    result = backtester.run(candles)

    print("\n" + "=" * 60)
    print("           RESULTATS DU BACKTEST")
    print("=" * 60)
    for key, value in result.to_dict().items():
        print(f"  {key.replace('_', ' ').title():30s}: {value}")
    print("=" * 60)

    # Interpretation
    print("\n--- INTERPRETATION ---")
    if result.profit_factor > 1.5 and result.sharpe_ratio > 1.0 and result.max_drawdown_pct < 20:
        print("✓ La strategie montre des resultats prometteurs.")
    else:
        print("⚠ La strategie necessite des ajustements avant production.")
    if result.total_trades < 10:
        print("⚠ Trop peu de trades pour une evaluation statistique fiable.")


async def cmd_dry_run(config: Config, duration: int) -> None:
    """Commande dry-run."""
    bot = TradingBot(config)

    # Gestion du Ctrl+C
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, bot.stop)
        except NotImplementedError:
            pass  # Windows ne supporte pas add_signal_handler

    try:
        await bot.run_dry_run(duration_minutes=duration, tick_interval=0.05)
    except KeyboardInterrupt:
        bot.logger.info("Interruption clavier detectee")
    finally:
        bot.stop()


async def cmd_paper(config: Config, duration: int = 60) -> None:
    """Commande paper trading (connexion API demo Deriv)."""
    from src.deriv_client import DerivClient

    logger = setup_logger(config, "paper_trading")
    logger.info("=== PAPER TRADING (Compte Demo Deriv) ===")

    if not config.deriv_token:
        logger.error("DERIV_TOKEN manquant dans le fichier .env")
        logger.info("Obtenez un token sur https://app.deriv.com/account/api-token")
        return

    deriv_client = DerivClient(config, logger)

    # Connexion
    connected = await deriv_client.connect()
    if not connected:
        logger.error("Impossible de se connecter a l'API Deriv")
        return

    # Souscription aux ticks
    await deriv_client.subscribe_ticks(config.market_symbol)
    logger.info(f"Souscrit aux ticks de {config.market_symbol}")

    # Lancer le bot (similaire a dry-run mais avec l'API)
    bot = TradingBot(config)
    try:
        await bot.run_dry_run(duration_minutes=duration, tick_interval=1.0)
    except KeyboardInterrupt:
        logger.info("Arret demande")
    finally:
        await deriv_client.disconnect()


async def cmd_phase2(config: Config, duration: int = 60, use_api: bool = False) -> None:
    """Commande Phase 2 de paper trading avec filtres avancés."""
    logger = setup_logger(config, "paper_phase2")
    logger.info("=== PAPER TRADING PHASE 2 ===")

    engine = PaperTradingPhase2(config)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, engine.stop)
        except NotImplementedError:
            pass

    try:
        summary = await engine.run(duration_minutes=duration, connect_api=use_api)
    except KeyboardInterrupt:
        engine.stop()
        summary = engine._build_session_summary(duration)
    finally:
        print("\n" + "=" * 60)
        print("   PHASE 2 — RÉSUMÉ DE LA SESSION")
        print("=" * 60)
        r = summary["results"]
        a = summary["activity"]
        c = summary["config"]
        print(f"  Capital final:       ${r['final_capital']:.2f}")
        print(f"  P&L total:           ${r['total_pnl']:+.2f} ({r['total_return_pct']:+.2f}%)")
        print(f"  Drawdown max:        {r['max_drawdown_pct']:.2f}%")
        print(f"  Trades:              {r['total_trades']} ({r['winning_trades']}W / {r['losing_trades']}L)")
        print(f"  Win rate:            {r['win_rate']:.1f}%")
        print(f"  Actual R:R:          {r['actual_risk_reward_ratio']:.2f}")
        print(f"  Ticks:               {a['ticks_received']}")
        print(f"  Signaux générés:     {a['signals_generated']}")
        print(f"  Trades exécutés:     {a['trades_executed']}")
        print(f"  Filtres:             Trend={c['trend_filter']} Vol={c['volatility_filter']} "
              f"ATR-SL={c['atr_stops']} Trailing={c['trailing_stop']}")
        print("=" * 60)


def cmd_report(config: Config) -> None:
    """Commande rapport de l'etat du bot."""
    logger = setup_logger(config, "report")
    risk_manager = RiskManager(config, logger)
    report = risk_manager.get_report()

    print("\n" + "=" * 50)
    print("         RAPPORT DE RISQUE")
    print("=" * 50)
    print(f"  Statut:              {report.status.value}")
    print(f"  Capital initial:     ${report.initial_capital:.2f}")
    print(f"  Capital actuel:      ${report.current_capital:.2f}")
    print(f"  Drawdown:            {report.drawdown_pct:.2f}%")
    print(f"  Peak capital:        ${report.peak_capital:.2f}")
    print(f"  Position size:       ${report.position_size:.2f}")
    print(f"  Trades aujourd'hui:  {report.trades_today}/{report.max_trades_per_day}")
    print(f"  Trading autorise:    {'OUI' if report.can_trade else 'NON'}")
    if not report.can_trade:
        print(f"  Raison blocage:      {report.reason_blocked}")
    print("=" * 50)


def main():
    """Point d'entree CLI."""
    parser = argparse.ArgumentParser(
        description="Trading Bot — Bollinger Bands + RSI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python -m src.main backtest              # Lancer un backtest
  python -m src.main dry-run --duration 10 # Dry run 10 minutes
  python -m src.main paper                 # Paper trading (demo)
  python -m src.main report                # Rapport de risque
        """,
    )

    parser.add_argument("command", nargs="?", default="backtest",
                        choices=["backtest", "dry-run", "paper", "phase2", "report"],
                        help="Commande a executer")
    parser.add_argument("--config", "-c", type=str, default=None,
                        help="Chemin vers le fichier .env")
    parser.add_argument("--duration", "-d", type=int, default=10,
                        help="Duree en minutes")
    parser.add_argument("--api", action="store_true",
                        help="Utiliser l'API Deriv lorsque supporte")

    args = parser.parse_args()
    config = load_config(args.config)

    if args.command == "backtest":
        cmd_backtest(config)
    elif args.command == "dry-run":
        asyncio.run(cmd_dry_run(config, args.duration))
    elif args.command == "paper":
        asyncio.run(cmd_paper(config, duration=args.duration))
    elif args.command == "phase2":
        asyncio.run(cmd_phase2(config, duration=args.duration, use_api=args.api))
    elif args.command == "report":
        cmd_report(config)


if __name__ == "__main__":
    main()