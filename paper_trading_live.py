"""Paper Trading LIVE — Bot complet connecté au compte démo Deriv via OTP.

C'est le chaînon qui manquait : le flux OTP branché sur la boucle de trading réelle.

Architecture à deux connexions WebSocket :
    1. Client PUBLIC  → flux de ticks temps réel (endpoint public, pas d'auth)
    2. Client TRADING → ordres (balance, proposal, buy) via OTP

Flux complet :
    ticks réels → DataStreamer → CandleBuilder → StrategyEngine
    → RiskManager → OrderExecutor (get_proposal + buy_contract sur le client trading)

Usage :
    python paper_trading_live.py --duration 60      # 60 minutes
    python paper_trading_live.py --symbol 1HZ75V    # changer de symbole
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import ssl
import sys
import time as _time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import Config, load_config
from src.data_streamer import DataStreamer
from src.candle_builder import Candle, CandleBuilder
from src.deriv_client import DerivClient
from src.historical_data import candles_from_deriv, timeframe_to_seconds
from src.indicators import Indicators
from src.logger import setup_logger
from src.order_executor import OrderExecutor
from src.risk_manager import RiskManager
from src.strategy_engine import SignalDirection, StrategyEngine, TradingSignal
from src.telegram_manager import TelegramManager


def fetch_account_id(config: Config) -> str:
    """Récupère l'account_id. Utilise la config si présente, sinon l'API REST.

    Returns:
        L'account_id (ex: "DOT92983989").
    """
    if config.deriv_account_id:
        print(f"  Account ID (config): {config.deriv_account_id}")
        return config.deriv_account_id

    # Auto-détection via l'API REST
    try:
        accounts_url = "https://api.derivws.com/trading/v1/options/accounts"
        req = Request(accounts_url, headers={
            "Deriv-App-ID": config.deriv_app_id,
            "Authorization": f"Bearer {config.deriv_token}",
        })
        ctx = ssl.create_default_context()
        with urlopen(req, context=ctx, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            accounts = data.get("data", []) if isinstance(data, dict) else data
            demo_accs = [a for a in accounts if a.get("account_type") == "demo"]
            chosen = demo_accs[0] if demo_accs else accounts[0]
            acc_id = chosen.get("account_id")
            print(f"  Account ID (auto-détecté): {acc_id} ({chosen.get('account_type')})")
            return acc_id
    except Exception as e:
        print(f"  [ERREUR] Auto-détection échouée: {e}")
        raise


class PaperTradingLive:
    """Bot de paper trading connecté au compte démo Deriv (OTP)."""

    def __init__(self, config: Config, symbol: str):
        self.config = config
        self.symbol = symbol
        self.logger = setup_logger(config, "paper_live")

        # Modules core
        self.data_streamer = DataStreamer(config, self.logger)
        self.candle_builder = CandleBuilder(config, self.logger)
        self.indicators = Indicators(config, self.logger)
        self.strategy_engine = StrategyEngine(config, self.indicators, self.candle_builder, self.logger)
        self.risk_manager = RiskManager(config, self.logger)

        # Deux clients WebSocket
        self.public_client = DerivClient(config, self.logger)    # ticks
        self.trading_client = DerivClient(config, self.logger)   # ordres

        # L'OrderExecutor utilise le client trading pour les ordres
        self.order_executor = OrderExecutor(config, deriv_client=self.trading_client, logger=self.logger)

        # Telegram (alertes + commandes distantes)
        self.telegram = TelegramManager(config, self.logger)
        self._register_telegram_commands()

        # État
        self._running = False
        self._ticks_received = 0
        self._signals_evaluated = 0
        self._trades_opened = 0
        self._trades_closed = 0
        self._last_status_time = 0.0

    def _register_telegram_commands(self) -> None:
        """Enregistre les commandes Telegram disponibles."""
        self.telegram.register_command("/help", self._tg_help)
        self.telegram.register_command("/start", self._tg_help)
        self.telegram.register_command("/status", self._tg_status)
        self.telegram.register_command("/report", self._tg_report)
        self.telegram.register_command("/kill", self._tg_kill)
        self.telegram.register_command("/resume", self._tg_resume)
        self.telegram.register_command("/stop", self._tg_stop)

    # ── Handlers de commandes Telegram ───────────────────────────────

    def _tg_help(self) -> str:
        return (
            "📊 *Commandes disponibles*\n"
            "/status — Etat actuel du bot\n"
            "/report — Rapport de risque\n"
            "/kill — Arret d'urgence (kill switch)\n"
            "/resume — Desactive le kill switch\n"
            "/stop — Arret propre du bot"
        )

    def _tg_status(self) -> str:
        state = "EN COURS" if self._running else "ARRETE"
        return (
            f"🤖 *Etat:* {state}\n"
            f"📡 Ticks recus: {self._ticks_received}\n"
            f"🕯 Bougies: {self.candle_builder.count()}\n"
            f"📈 Trades ouverts: {self._trades_opened} / fermes: {self._trades_closed}\n"
            f"🛑 Kill switch: {'ACTIF' if self.risk_manager.is_kill_switch_active else 'inactif'}"
        )

    def _tg_report(self) -> str:
        report = self.risk_manager.get_report()
        return (
            f"💰 Capital: ${report.current_capital:.2f} (initial ${report.initial_capital:.2f})\n"
            f"📊 P&L: ${report.total_pnl:+.2f} ({report.total_pnl_pct:+.2f}%)\n"
            f"📉 Drawdown: {report.drawdown_pct:.2f}%\n"
            f"🔁 Trades aujourd'hui: {report.trades_today}/{report.max_trades_per_day}\n"
            f"✅ Win rate: {self.risk_manager.win_rate*100:.1f}%"
        )

    def _tg_kill(self) -> str:
        if not self._running:
            self.risk_manager.activate_kill_switch("Commande Telegram /kill")
            return "🛑 Kill switch active (bot deja arrete)."
        self.risk_manager.activate_kill_switch("Commande Telegram /kill")
        self._running = False
        return "🛑 Kill switch active — trading stoppe. Envoyez /resume pour reprendre."

    def _tg_resume(self) -> str:
        self.risk_manager.deactivate_kill_switch()
        return "✅ Kill switch desactive — trading autorise a nouveau."

    def _tg_stop(self) -> str:
        self._running = False
        return "⏹ Arret propre demande — le bot va se couper."

    async def _preload_history(self) -> int:
        """Pre-charge l'historique OHLC dans le CandleBuilder avant le flux live.

        Recupere `preload_candles_count` bougies (defaut 300) via l'API publique
        et les injecte dans le `CandleBuilder`, afin que la strategie dispose
        immediatement de bougies fermees (sinon il faudrait attendre ~22 min
        de ticks live pour le tout premier signal).

        Returns:
            Nombre de bougies fermees prechargees.
        """
        granularity = timeframe_to_seconds(self.config.timeframe)
        count = getattr(self.config, "preload_candles_count", 300)

        resp = await self.public_client.get_candles(
            self.symbol,
            count=count,
            granularity=granularity,
            end="latest",
        )

        if not resp or resp.get("error"):
            self.logger.warning(
                f"Prechargement historique impossible: "
                f"{resp.get('error', {}).get('message', 'aucune reponse') if resp else 'aucune reponse'}")
            return 0

        raw_candles = resp.get("candles", [])
        if not raw_candles:
            self.logger.warning("Aucune bougie historique retournee par l'API")
            return 0

        candles = candles_from_deriv(
            raw_candles,
            symbol=self.symbol,
            timeframe=self.config.timeframe,
        )
        loaded = self.candle_builder.preload_candles(candles)
        self.logger.info(
            f"Historique precharge: {loaded} bougies fermees "
            f"({self.config.timeframe}) + 1 bougie courante"
        )
        return loaded

    async def run(self, duration_minutes: int) -> None:
        """Lance la session de paper trading réel.

        Args:
            duration_minutes: Durée en minutes (0 = infini jusqu'à Ctrl+C).
        """
        self.logger.info("=" * 66)
        self.logger.info("   PAPER TRADING LIVE — COMPTE DÉMO DERIV")
        self.logger.info("=" * 66)
        self.logger.info(f"  Symbole   : {self.symbol}")
        self.logger.info(f"  Timeframe : {self.config.timeframe}")
        self.logger.info(f"  Durée     : {'infini' if duration_minutes == 0 else f'{duration_minutes} min'}")
        self.logger.info(f"  Risque    : {self.config.risk_per_trade_pct}%/trade, "
                         f"{self.config.daily_stop_loss_pct}%/jour, "
                         f"max {self.config.max_trades_per_day} trades/jour")
        self.logger.info("=" * 66)

        # 1. Récupérer l'account_id
        print("\n[1/4] Récupération du compte démo...")
        account_id = fetch_account_id(self.config)

        # 2. Connexion publique (ticks)
        print("\n[2/4] Connexion au flux de ticks (public)...")
        pub_ok = await self.public_client.connect()
        if not pub_ok:
            self.logger.error("Connexion publique échouée")
            return
        # S'abonner aux ticks du symbole
        sub = await self.public_client.subscribe_ticks(self.symbol)
        if sub and sub.get("subscription"):
            print(f"[OK] Souscrit aux ticks de {self.symbol}")
        elif sub and sub.get("history"):
            print(f"[INFO] Historique reçu, abonnement live en cours...")
        else:
            print(f"[WARN] Souscription tick inhabituelle: {json.dumps(sub)[:200]}")
        self.public_client.on_tick(self._on_tick)
        print("[OK] Flux de ticks public connecté")

        # Précharger l'historique OHLC pour que la stratégie soit prête immédiatement
        print("\n[2b/4] Préchargement de l'historique OHLC...")
        loaded = await self._preload_history()
        if loaded == 0:
            self.logger.warning(
                "Aucun historique precharge — la strategie ne commencera a "
                "emettre des signaux qu'apres accumulation de bougies live"
            )

        # 3. Connexion trading (OTP)
        print("\n[3/4] Connexion trading via OTP...")
        trade_ok = await self.trading_client.connect_trading(
            token=self.config.deriv_token,
            account_id=account_id,
        )
        if not trade_ok:
            self.logger.error("Connexion trading échouée")
            await self.public_client.disconnect()
            return
        print("[OK] Connexion trading établie")

        # Récupérer le solde initial
        balance = await self.trading_client.get_balance()
        if balance and balance.get("balance"):
            b = balance["balance"]
            actual_balance = float(b.get("balance", 0) or 0)
            self.logger.info(f"Solde démo : {b.get('currency', 'USD')} {actual_balance} "
                             f"(compte {b.get('loginid', account_id)})")
            if actual_balance > 0:
                # Aligner le capital du RiskManager sur le solde réel du compte
                self.risk_manager.set_initial_capital(actual_balance)

        # 4. Boucle de trading
        print(f"\n[4/4] Boucle de trading lancée — Ctrl+C pour arrêter\n")
        self._running = True
        self._last_status_time = _time.time()

        # Wiring des callbacks
        self.data_streamer.subscribe(lambda tick: self.candle_builder.process_tick(tick))
        self.candle_builder.on_candle_close(self._on_candle_closed)

        # Telegram : demarrage du polling des commandes + message de demarrage
        if self.telegram.enabled:
            self.telegram.start_polling()
            # Declare les commandes a Telegram pour les rendre visibles
            # dans le menu "/" du chat (setMyCommands).
            await self.telegram.sync_bot_commands()
            await self.telegram.send_message(
                f"🚀 *Bot demarre*\nSymbole: {self.symbol}\n"
                f"Timeframe: {self.config.timeframe}\n"
                f"Bougies prechargees: {loaded}\n"
                f"Envoyez /help pour les commandes."
            )

        start = _time.time()
        end = start + duration_minutes * 60 if duration_minutes > 0 else float("inf")

        while self._running and _time.time() < end:
            await asyncio.sleep(0.1)
            # Statut périodique toutes les 60s
            if _time.time() - self._last_status_time >= 60:
                self._log_status()
                self._last_status_time = _time.time()

        # Clôture
        await self._shutdown()
        self.logger.info("Session paper trading terminée")

    def _on_tick(self, tick_data: dict) -> None:
        """Callback des ticks réels."""
        self.data_streamer.on_tick(tick_data)
        self._ticks_received += 1

    async def _on_candle_closed(self, candle: Candle) -> None:
        """Évalué à chaque bougie fermée."""
        self._signals_evaluated += 1

        # 1. Vérifier les positions ouvertes (SL/TP simulé côté bot)
        for order in self.order_executor.active_orders[:]:
            closed = await self.order_executor.simulate_price_movement(order, candle.close)
            if closed:
                self._trades_closed += 1
                self.risk_manager.on_trade_closed(closed.pnl, closed.exit_price, closed.entry_price)
                self.logger.info(
                    f"[CLÔTURE] {closed.direction.value} | "
                    f"Entry={closed.entry_price:.5f} Exit={closed.exit_price:.5f} | "
                    f"PnL=${closed.pnl:+.2f}"
                )
                await self.telegram.send_message(
                    f"🔴 *CLOTURE* {closed.direction.value}\n"
                    f"Entry={closed.entry_price:.5f} Exit={closed.exit_price:.5f}\n"
                    f"PnL=${closed.pnl:+.2f}"
                )

        # 2. Évaluer la stratégie
        signal = self.strategy_engine.evaluate()
        if not signal.is_valid:
            return

        # 3. Vérifier le risk management
        can_trade, report = self.risk_manager.can_place_trade(signal)
        if not can_trade:
            self.logger.debug(f"Trade bloqué: {report.reason_blocked}")
            return

        # 4. Exécuter l'ordre réel sur le compte démo
        order = await self.order_executor.execute_signal(signal, report.position_size)
        if not order:
            return

        self.risk_manager.on_trade_opened(order.amount)
        self._trades_opened += 1
        self.logger.info(
            f"[OUVERTURE] {signal.direction.value} @ {signal.entry_price:.5f} | "
            f"Amount=${order.amount:.2f} | SL={order.stop_loss:.5f} TP={order.take_profit:.5f}"
        )
        await self.telegram.send_message(
            f"🟢 *OUVERTURE* {signal.direction.value} @ {signal.entry_price:.5f}\n"
            f"Amount=${order.amount:.2f}\n"
            f"SL={order.stop_loss:.5f} TP={order.take_profit:.5f}"
        )

    def _log_status(self) -> None:
        """Affiche le statut périodique."""
        report = self.risk_manager.get_report()
        self.logger.info(
            f"[STATUS] Capital=${report.current_capital:.2f} | "
            f"PnL=${report.total_pnl:+.2f} ({report.total_pnl_pct:+.2f}%) | "
            f"DD={report.drawdown_pct:.2f}% | "
            f"Trades={self._trades_opened} ouvert(s)/{report.trades_today} jour | "
            f"Ticks={self._ticks_received} | Bougies={self.candle_builder.count()}"
        )

    async def _shutdown(self) -> None:
        """Arrêt propre."""
        self._running = False
        # Fermer toutes les positions simulées
        current_price = (
            self.candle_builder.current_candle.close
            if self.candle_builder.current_candle
            else self.data_streamer.latest_price or 0.0
        )
        if self.order_executor.active_orders and current_price > 0:
            closed = self.order_executor.close_all_orders(current_price)
            for o in closed:
                self.risk_manager.on_trade_closed(o.pnl, o.exit_price, o.entry_price)
        # Telegram : arret du polling + message de fin
        self.telegram.stop()
        await self.telegram.send_message("🛑 *Bot arrete* — rapport final ci-dessous.")

        await self.public_client.disconnect()
        await self.trading_client.disconnect()

        # Rapport final
        report = self.risk_manager.get_report()
        print("\n" + "=" * 50)
        print("   RAPPORT FINAL — PAPER TRADING DÉMO")
        print("=" * 50)
        print(f"  Capital initial : ${report.initial_capital:.2f}")
        print(f"  Capital final   : ${report.current_capital:.2f}")
        print(f"  P&L total       : ${report.total_pnl:+.2f} ({report.total_pnl_pct:+.2f}%)")
        print(f"  Drawdown max    : {report.drawdown_pct:.2f}%")
        print(f"  Trades          : {self.risk_manager.total_trades} "
              f"({self.risk_manager.winning_trades}W / {self.risk_manager.losing_trades}L)")
        print(f"  Win rate        : {self.risk_manager.win_rate*100:.1f}%")
        print(f"  Ticks reçus     : {self._ticks_received}")
        print(f"  Signaux évalués : {self._signals_evaluated}")
        print(f"  Trades ouverts  : {self._trades_opened}")
        print("=" * 50)

    def stop(self) -> None:
        self._running = False


async def main():
    parser = argparse.ArgumentParser(description="Paper Trading LIVE — compte démo Deriv (OTP)")
    parser.add_argument("--duration", "-d", type=int, default=0,
                        help="Durée en minutes (0 = infini, défaut)")
    parser.add_argument("--symbol", "-s", type=str, default=None,
                        help="Symbole (défaut: config)")
    parser.add_argument("--config", "-c", type=str, default=None,
                        help="Fichier .env personnalisé")
    args = parser.parse_args()

    config = load_config(args.config)
    # Forcer le mode paper_trading pour que OrderExecutor envoie de vrais ordres démo
    config = replace(config, mode="paper_trading")
    symbol = args.symbol or config.market_symbol

    engine = PaperTradingLive(config, symbol)

    # Gestion Ctrl+C (Windows)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, engine.stop)
        except NotImplementedError:
            pass

    try:
        await engine.run(duration_minutes=args.duration)
    except KeyboardInterrupt:
        print("\nArrêt demandé...")
        engine.stop()
        await engine._shutdown()


if __name__ == "__main__":
    asyncio.run(main())