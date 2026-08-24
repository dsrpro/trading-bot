"""Runner Scalper Multi-Symboles — Trading asynchrone sur plusieurs indices synthetiques.

Surveille simultanement une liste d'indices (R_10, R_25, R_50, R_75, R_100, 1HZ100V),
genere des signaux de scalping M1 en parallele, et applique la gestion centralisee des risques
avec cible de profit journalier ($60 USD) et stop-loss quotidien ($40 USD).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional
import numpy as np

from src.candle_builder import Candle, CandleBuilder
from src.config import Config
from src.data_streamer import Tick
from src.deriv_client import DerivClient
from src.indicators import Indicators
from src.logger import setup_logger
from src.order_executor import OrderExecutor
from src.risk_manager import RiskManager, RiskStatus
from src.strategy_engine import SignalDirection, StrategyEngine, TradingSignal
from src.telegram_manager import TelegramManager


class SymbolContext:
    """Contexte d'analyse dedie a un symbole specifique."""

    def __init__(self, symbol: str, config: Config, logger: logging.Logger):
        self.symbol = symbol
        from dataclasses import replace
        symbol_config = replace(config, market_symbol=symbol)
        self.candle_builder = CandleBuilder(
            config=symbol_config,
            logger=logger,
        )
        self.indicators = Indicators(config=symbol_config, logger=logger)
        self.engine = StrategyEngine(
            config=symbol_config,
            indicators=self.indicators,
            candle_builder=self.candle_builder,
            logger=logger,
        )


class MultiSymbolScalper:
    """Gestionnaire du Scalping Multi-Symboles."""

    def __init__(
        self,
        config: Config,
        logger: Optional[logging.Logger] = None,
        deriv_client: Optional[DerivClient] = None,
    ):
        self.config = config
        self.logger = logger or setup_logger(config, "scalper_multi")
        self.deriv_client = deriv_client

        # Integrer Telegram
        self.telegram = TelegramManager(config=config, logger=self.logger)

        # Decoupage de la liste de symboles (ex: R_10, R_25, R_50, R_75, R_100, 1HZ100V)
        raw_symbols = getattr(config, 'scalping_symbols', "R_10,R_25,R_50,R_75,R_100,1HZ100V")
        self.symbols = [s.strip() for s in raw_symbols.split(",") if s.strip()]
        if not self.symbols:
            self.symbols = [config.market_symbol]

        # Ingestion des contexts par symbole
        self.contexts: dict[str, SymbolContext] = {
            sym: SymbolContext(sym, config, self.logger) for sym in self.symbols
        }

        self.risk_manager = RiskManager(config=config, logger=self.logger)
        self.executor = OrderExecutor(
            config=config,
            deriv_client=self.deriv_client,
            logger=self.logger,
            telegram_manager=self.telegram,
        )

        self._running = False
        self._start_time: float = 0.0

        # Enregistrer les commandes Telegram
        if self.telegram.enabled:
            self.telegram.register_command("/status", self._cmd_telegram_status)
            self.telegram.register_command("/report", self._cmd_telegram_report)
            self.telegram.register_command("/stop", self._cmd_telegram_stop)

    def _on_deriv_tick(self, tick_data: dict) -> None:
        """Callback appele pour chaque tick de marche recu en direct depuis l'API Deriv."""
        symbol = tick_data.get("symbol")
        if symbol and symbol in self.contexts:
            tick = Tick.from_deriv(tick_data, symbol)
            self.contexts[symbol].candle_builder.process_tick(tick)

    async def run(self, duration_minutes: Optional[int] = None) -> dict:
        """Execute la boucle principale de scalping.

        Args:
            duration_minutes: Duree maximale en minutes (None = infini).

        Returns:
            Dict avec statistiques finales de la session.
        """
        self._running = True
        self._start_time = time.time()
        duration_seconds = (duration_minutes * 60.0) if duration_minutes else float("inf")

        self.logger.info("=" * 70)
        self.logger.info("  DEMARRAGE SCALPER MULTI-SYMBOLES DERIV")
        self.logger.info(f"  Symboles actives ({len(self.symbols)}): {', '.join(self.symbols)}")
        self.logger.info(f"  Objectif de profit journalier: ${self.config.daily_profit_target_usd:.2f}")
        self.logger.info(f"  Stop-loss quotidien: ${self.config.daily_stop_loss_usd:.2f}")
        self.logger.info(f"  Risk/Trade: {self.config.risk_per_trade_pct}% | Max trades/jour: {self.config.max_trades_per_day}")
        self.logger.info(f"  Mode: {self.config.mode}")
        self.logger.info("=" * 70)

        # Lancer le polling Telegram si disponible
        if self.telegram.enabled:
            self.telegram.start_polling()
            await self.telegram.send_message(
                f"🚀 *DEMARRAGE SCALPER MULTI-SYMBOLES*\n"
                f"📊 Symboles surveillés ({len(self.symbols)}): {', '.join(self.symbols)}\n"
                f"🎯 Objectif profit: ${self.config.daily_profit_target_usd:.2f}/jour\n"
                f"🛡️ Stop-loss: ${self.config.daily_stop_loss_usd:.2f}\n"
                f"💼 Mode: `{self.config.mode}`"
            )

        # Connecter le listener de ticks Deriv si l'API est active
        if self.deriv_client and self.deriv_client.is_connected:
            self.deriv_client.on_tick(self._on_deriv_tick)

        # Prechargement des bougies (reelles depuis API ou synthétiques)
        await self._preload_all_candles()

        # Souscription aux flux de ticks en direct
        if self.deriv_client and self.deriv_client.is_connected:
            for sym in self.symbols:
                await self.deriv_client.subscribe_ticks(sym)

        last_report_time = time.time()

        try:
            while self._running:
                elapsed = time.time() - self._start_time
                if elapsed >= duration_seconds:
                    self.logger.info(f"Duree cible de {duration_minutes} min atteinte. Arret de la session.")
                    break

                report = self.risk_manager.get_report()
                if not report.can_trade:
                    if report.status == RiskStatus.DAILY_PROFIT_TARGET_REACHED:
                        msg = f"🎉 *OBJECTIF ATTEINT!* Profit du jour = ${report.daily_pnl:.2f}. Trading verrouillé pour la journée."
                        self.logger.info(msg)
                        if self.telegram.enabled:
                            await self.telegram.send_message(msg)
                        break
                    elif report.status in (RiskStatus.DAILY_LOSS_LIMIT_REACHED, RiskStatus.KILL_SWITCH_ACTIVATED):
                        msg = f"🛑 *STOP LOSS DECLENCHE!* ({report.reason_blocked}). Arret."
                        self.logger.warning(msg)
                        if self.telegram.enabled:
                            await self.telegram.send_message(msg)
                        break

                # Generer et evaluer les 6 symboles en parallele
                for sym, ctx in self.contexts.items():
                    # Generer des mouvements de ticks en dry-run si pas d'API en direct
                    if not (self.deriv_client and self.deriv_client.is_connected):
                        tick = self._generate_simulated_tick(sym, ctx)
                        ctx.candle_builder.process_tick(tick)

                    candle = ctx.candle_builder.current_candle
                    signal = ctx.engine.evaluate(current_candle=candle)

                    if signal.is_valid:
                        can_trade, risk_rep = self.risk_manager.can_place_trade(signal)
                        if can_trade:
                            order = await self.executor.execute_signal(signal, risk_rep.position_size)
                            if order:
                                self.risk_manager.on_trade_opened(order.amount)
                                if self.telegram.enabled:
                                    asyncio.create_task(self.telegram.send_message(
                                        f"🟢 *ORDRE OUVERT* | {order.symbol}\n"
                                        f"Direction: `{order.direction.value}`\n"
                                        f"Entree: `{order.entry_price:.5f}`\n"
                                        f"Montant: `${order.amount:.2f}`\n"
                                        f"SL: `{order.stop_loss:.5f}` | TP: `{order.take_profit:.5f}`"
                                    ))

                    # Simuler la gestion des ordres ouverts pour ce symbole
                    if self.executor.has_active_orders:
                        current_price = (
                            ctx.candle_builder.current_candle.close if ctx.candle_builder.current_candle
                            else (ctx.candle_builder.candles[-1].close if len(ctx.candle_builder.candles) > 0 else 1000.0)
                        )
                        for order in list(self.executor.active_orders):
                            if order.symbol == sym:
                                closed_order = await self.executor.simulate_price_movement(order, current_price)
                                if closed_order:
                                    rep = self.risk_manager.on_trade_closed(
                                        closed_order.pnl,
                                        closed_order.exit_price,
                                        closed_order.entry_price,
                                    )
                                    if self.telegram.enabled:
                                        icon = "🟢" if closed_order.pnl > 0 else "🔴"
                                        asyncio.create_task(self.telegram.send_message(
                                            f"{icon} *TRADE FERME* | {closed_order.symbol}\n"
                                            f"PnL: `${closed_order.pnl:+.2f}`\n"
                                            f"Sortie: `{closed_order.exit_price:.5f}`\n"
                                            f"Daily PnL: `${rep.daily_pnl:+.2f}` / `${self.config.daily_profit_target_usd:.2f}`\n"
                                            f"Solde actuel: `${rep.current_capital:.2f}`"
                                        ))

                # Rapport périodique toutes les 60 secondes
                if time.time() - last_report_time >= 60.0:
                    last_report_time = time.time()
                    rep = self.risk_manager.get_report()
                    self.logger.info(
                        f"[STATUS] Daily PnL=${rep.daily_pnl:.2f} / target=${self.config.daily_profit_target_usd:.2f} | "
                        f"Trades={rep.trades_today}/{rep.max_trades_per_day} | Capital=${rep.current_capital:.2f}"
                    )

                await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            self.logger.info("Session scalper interrompue par l'utilisateur.")
        finally:
            self._running = False
            if self.telegram.enabled:
                self.telegram.stop()

        return self.get_summary()

    def _cmd_telegram_status(self, text: str = "") -> str:
        report = self.risk_manager.get_report()
        return (
            f"📊 *STATUT SCALPER MULTI-SYMBOLES*\n"
            f"Symboles surveilles (6): {', '.join(self.symbols)}\n"
            f"Capital actuel: ${report.current_capital:.2f}\n"
            f"PnL du jour: ${report.daily_pnl:+.2f} / ${self.config.daily_profit_target_usd:.2f}\n"
            f"Trades aujourd'hui: {report.trades_today}/{report.max_trades_per_day}\n"
            f"Win Rate: {self.risk_manager.win_rate*100:.1f}%\n"
            f"Statut: `{report.status.value}`"
        )

    def _cmd_telegram_report(self, text: str = "") -> str:
        report = self.risk_manager.get_report()
        return (
            f"📋 *RAPPORT DE RISQUE*\n"
            f"Peak Capital: ${report.peak_capital:.2f}\n"
            f"Drawdown: {report.drawdown_pct:.2f}%\n"
            f"Kill Switch: {'ACTIF' if self.risk_manager.is_kill_switch_active else 'Inactif'}\n"
            f"P&L Total: ${report.total_pnl:+.2f}"
        )

    def _cmd_telegram_stop(self, text: str = "") -> str:
        self._running = False
        return "🛑 Arrêt du scalper demandé via Telegram."

    async def _preload_all_candles(self) -> None:
        """Genere ou charge des bougies initiales pour chaque symbole (API ou simulation)."""
        for sym, ctx in self.contexts.items():
            loaded_real = False
            if self.deriv_client and self.deriv_client.is_connected:
                try:
                    res = await self.deriv_client.get_candles(sym, count=100, granularity=60)
                    if res and "candles" in res:
                        candles_data = res["candles"]
                        for c_data in candles_data:
                            candle = Candle(
                                open=float(c_data["open"]),
                                high=float(c_data["high"]),
                                low=float(c_data["low"]),
                                close=float(c_data["close"]),
                                timestamp=float(c_data.get("epoch", time.time())),
                                symbol=sym,
                                is_closed=True,
                            )
                            ctx.candle_builder._candles.append(candle)
                        loaded_real = True
                        self.logger.info(f"Precharge {len(candles_data)} bougies reelles M1 pour {sym}")
                except Exception as e:
                    self.logger.warning(f"Impossible de precharger les bougies reelles pour {sym}: {e}")

            if not loaded_real:
                now = time.time()
                base_price = 1000.0 + (hash(sym) % 500)
                count = getattr(self.config, 'preload_candles_count', 100)
                for i in range(count, 0, -1):
                    timestamp = now - i * 60
                    drift = np.random.normal(0, 0.5)
                    base_price += drift
                    open_p = base_price
                    high_p = open_p + abs(np.random.normal(0, 0.8))
                    low_p = open_p - abs(np.random.normal(0, 0.8))
                    close_p = open_p + np.random.normal(0, 0.5)
                    candle = Candle(
                        open=open_p,
                        high=high_p,
                        low=low_p,
                        close=close_p,
                        timestamp=timestamp,
                        symbol=sym,
                        is_closed=True,
                    )
                    ctx.candle_builder._candles.append(candle)

    def _generate_simulated_tick(self, symbol: str, ctx: SymbolContext):
        """Genere un tick synthétique réaliste pour les tests dry-run."""
        last_price = 1000.0
        if ctx.candle_builder.current_candle:
            last_price = ctx.candle_builder.current_candle.close
        elif len(ctx.candle_builder.candles) > 0:
            last_price = ctx.candle_builder.candles[-1].close

        change = np.random.normal(0, 0.2)
        quote = round(last_price + change, 4)
        return Tick(timestamp=time.time(), symbol=symbol, price=quote)

    def get_summary(self) -> dict:
        """Retourne le bilan final de la session."""
        report = self.risk_manager.get_report()
        elapsed = time.time() - self._start_time if self._start_time > 0 else 0
        return {
            "duration_seconds": round(elapsed, 1),
            "symbols_scanned": self.symbols,
            "initial_capital": report.initial_capital,
            "final_capital": report.current_capital,
            "daily_pnl": report.daily_pnl,
            "daily_pnl_pct": report.daily_pnl_pct,
            "target_reached": report.daily_pnl >= self.config.daily_profit_target_usd,
            "total_trades": self.risk_manager.total_trades,
            "winning_trades": self.risk_manager.winning_trades,
            "losing_trades": self.risk_manager.losing_trades,
            "win_rate": round(self.risk_manager.win_rate * 100.0, 1),
        }
