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
import subprocess
import os

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
from src.markets import resolve_symbol, list_markets, MARKET_CATALOG
from src.instance_registry import (
    register_instance,
    unregister_instance,
    list_instances,
    update_instance,
    append_event,
    read_new_events,
)


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

        # Telegram (alertes + commandes distantes)
        self.telegram = TelegramManager(config, self.logger)
        self._register_telegram_commands()

        # L'OrderExecutor utilise le client trading pour les ordres
        # On lui passe aussi le TelegramManager pour notifications optionnelles
        self.order_executor = OrderExecutor(
            config,
            deriv_client=self.trading_client,
            logger=self.logger,
            telegram_manager=self.telegram,
        )

        # État
        self._running = False
        self._paused = False
        self._ticks_received = 0
        self._signals_evaluated = 0
        self._trades_opened = 0
        self._trades_closed = 0
        self._last_status_time = 0.0
        # Forwarder d'événements : seule l'instance avec Telegram actif
        # lit la file partagée et relaie les trades de TOUS les indices.
        self._events_offset = 0

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
            "/run SYM1,SYM2,... — Demarrer le bot sur un ou plusieurs symboles\n"
            "/markets — Lister les marchés disponibles\n"
            "/choose — Selectionner par index (envoyez /choose pour la liste)\n"
            "/kill — Arret d'urgence (kill switch)\n"
            "/resume — Desactive le kill switch\n"
            "/stop — Arret propre du bot"
        )

    def _tg_status(self) -> str:
        state = "EN COURS" if self._running else "ARRETE"
        open_orders = len(self.order_executor.active_orders) if hasattr(self.order_executor, 'active_orders') else self._trades_opened
        report = self.risk_manager.get_report()
        lines = [
            f"🤖 *Etat:* {state}",
            f"📅 *Heure:* {datetime.now(timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}",
            f"📡 *Ticks reçus:* {self._ticks_received}",
            f"🕯 *Bougies:* {self.candle_builder.count()}",
            f"📈 *Positions ouvertes:* {open_orders}",
            f"💰 *Capital:* ${report.current_capital:.2f} (init ${report.initial_capital:.2f})",
            f"📊 *P&L:* ${report.total_pnl:+.2f} ({report.total_pnl_pct:+.2f}%)",
            f"📉 *Drawdown max:* {report.drawdown_pct:.2f}%",
            f"🔁 *Trades aujourd'hui:* {report.trades_today}/{report.max_trades_per_day}",
            f"✅ *Win rate:* {self.risk_manager.win_rate*100:.1f}%",
        ]
        # Vue agrégée de toutes les instances (ce processus + enfants via /run)
        try:
            others = list_instances()
            if others:
                lines.append('\n*📊 Vue multi-indices:*')
                for inst in others:
                    sym = inst.get('symbol', '?')
                    pnl = inst.get('pnl', 0.0)
                    wr = inst.get('win_rate', 0.0)
                    trades = inst.get('trades', 0)
                    positions = inst.get('positions', []) or []
                    lines.append(
                        f"• {sym}: P&L ${pnl:+.2f} | WR {wr:.1f}% | "
                        f"{len(positions)} pos | {trades} trades"
                    )
        except Exception:
            pass

        return "\n".join(lines)

    def _tg_report(self) -> str:
        report = self.risk_manager.get_report()
        now = datetime.now(timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
        lines = [
            f"📈 *Rapport de trading* — {now}",
            f"💰 *Capital actuel:* ${report.current_capital:.2f}  — *Initial:* ${report.initial_capital:.2f}",
            f"📊 *P&L total:* ${report.total_pnl:+.2f} ({report.total_pnl_pct:+.2f}%)",
            f"📉 *Drawdown max:* {report.drawdown_pct:.2f}%",
            f"🔁 *Trades aujourd'hui:* {report.trades_today}/{report.max_trades_per_day}",
            f"✅ *Win rate:* {self.risk_manager.win_rate*100:.1f}%",
        ]

        # Ajouter compteur de rejets pour montant inferieur a MIN_STAKE
        try:
            rejets = getattr(self.order_executor, 'min_stake_rejections', 0)
            lines.append(f"⚠️ *Rejets (MIN_STAKE):* {rejets}")
        except Exception:
            pass

        # Ajouter positions ouvertes détaillées si présentes
        try:
            active = getattr(self.order_executor, 'active_orders', []) or []
            if active:
                lines.append('\n*Positions ouvertes:*')
                for o in active:
                    # o should have direction, amount, entry_price, stop_loss, take_profit
                    d = getattr(o, 'direction', getattr(o, 'side', ''))
                    amt = getattr(o, 'amount', getattr(o, 'size', 0.0))
                    entry = getattr(o, 'entry_price', getattr(o, 'price', 0.0))
                    sl = getattr(o, 'stop_loss', '-')
                    tp = getattr(o, 'take_profit', '-')
                    lines.append(f"- {getattr(d, 'value', str(d))} | Montant=${amt:.2f} | Entry={entry} | SL={sl} | TP={tp}")
        except Exception:
            pass

        # Ajouter résumé des derniers trades si dispo
        try:
            last = getattr(self.risk_manager, 'last_closed_trade', None)
            if last:
                lines.append('\n*Dernier trade fermé:*')
                lines.append(f"{getattr(last, 'direction', '')} PnL=${getattr(last, 'pnl', 0.0):+.2f} Entry={getattr(last,'entry', '')} Exit={getattr(last,'exit','')}")
        except Exception:
            pass

        # Vue agrégée des positions ouvertes de TOUS les indices
        try:
            others = list_instances()
            has_positions = any(inst.get('positions') for inst in others)
            if has_positions:
                lines.append('\n*🎯 Positions ouvertes (tous indices):*')
                for inst in others:
                    pos_list = inst.get('positions', []) or []
                    for p in pos_list:
                        lines.append(
                            f"• {inst.get('symbol')} {p.get('direction')} "
                            f"Entry={p.get('entry_price')} SL={p.get('stop_loss')} "
                            f"TP={p.get('take_profit')}"
                        )
        except Exception:
            pass

        # Synthèse P&L par instance (cross-process)
        try:
            others = list_instances()
            if others:
                lines.append('\n*📊 P&L par indice:*')
                for inst in others:
                    lines.append(
                        f"• {inst.get('symbol')}: P&L ${inst.get('pnl', 0.0):+.2f} | "
                        f"WR {inst.get('win_rate', 0.0):.1f}% | "
                        f"{inst.get('trades', 0)} trades | ${inst.get('capital', 0.0):.2f}"
                    )
        except Exception:
            pass

        return "\n".join(lines)

    def _tg_kill(self) -> str:
        if not self._running:
            self.risk_manager.activate_kill_switch("Commande Telegram /kill")
            return "🛑 Kill switch active (bot deja arrete)."
        self.risk_manager.activate_kill_switch("Commande Telegram /kill")
        self._running = False
        return "🛑 Kill switch active — trading stoppe. Envoyez /resume pour reprendre."

    def _tg_resume(self) -> str:
        self.risk_manager.deactivate_kill_switch()
        self._paused = False
        self._running = True
        return "✅ Kill switch desactive — trading autorise a nouveau."

    def _tg_stop(self) -> str:
        self._paused = True
        self._running = True
        return "⏹ Arret propre demande — le bot est mis en pause."

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
        self._paused = False
        self._last_status_time = _time.time()

        # Register this running instance so /report can show active symbols
        try:
            register_instance(self.symbol, pid=os.getpid())
        except Exception:
            self.logger.exception("Impossible d'enregistrer l'instance dans le registre")

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
            if self._paused:
                await asyncio.sleep(0.5)
                continue
            await asyncio.sleep(0.1)
            # Résoudre les contrats réels en attente (vrai résultat Deriv)
            for closed in await self.order_executor.poll_contract_resolutions():
                self._trades_closed += 1
                self.risk_manager.on_trade_closed(closed.pnl, closed.exit_price, closed.entry_price)
                self.logger.info(
                    f"[CLÔTURE] {closed.direction.value} | Entry={closed.entry_price:.5f} "
                    f"Exit={closed.exit_price:.5f} | PnL=${closed.pnl:+.2f}"
                )
                await self._notify(
                    f"🔴 *CLOTURE* {closed.direction.value} — {self.symbol}\n"
                    f"Entry={closed.entry_price:.5f} Exit={closed.exit_price:.5f}\n"
                    f"PnL=${closed.pnl:+.2f}"
                )
                self._sync_registry()
            # Relayer vers Telegram les événements des instances enfants
            await self._forward_events()
            # Statut périodique toutes les 60s
            if _time.time() - self._last_status_time >= 60:
                self._log_status()
                self._sync_registry()
                self._last_status_time = _time.time()

        # Clôture
        await self._shutdown()
        self.logger.info("Session paper trading terminée")

    def _on_tick(self, tick_data: dict) -> None:
        """Callback des ticks réels."""
        self.data_streamer.on_tick(tick_data)
        self._ticks_received += 1

    async def _notify(self, message: str) -> None:
        """Notifie un événement de trading, taggé par symbole.

        - Si Telegram est actif (instance « mère ») : envoi direct au chat.
        - Sinon (instance enfant lancée via /run) : écrit dans la file
          d'événements partagée ; la mère la reliera vers Telegram.

        C'est ce qui permet de remonter les trades de TOUS les indices,
        même quand plusieurs instances tournent en parallèle.
        """
        if self.telegram.enabled:
            await self.telegram.send_message(message)
        else:
            append_event({
                "pid": os.getpid(),
                "symbol": self.symbol,
                "message": message,
            })

    async def _forward_events(self) -> None:
        """Instance mère : relaie vers Telegram les événements des enfants.

        Appelée périodiquement dans la boucle principale. Sans effet si
        Telegram est désactivé (instance enfant).
        """
        if not self.telegram.enabled:
            return
        try:
            events, self._events_offset = read_new_events(self._events_offset)
        except Exception:
            return
        for ev in events:
            msg = ev.get("message")
            if not msg:
                continue
            await self.telegram.send_message(msg)

    async def _on_candle_closed(self, candle: Candle) -> None:
        """Évalué à chaque bougie fermée."""
        self._signals_evaluated += 1

        # 1. Résoudre les contrats réels via l'API Deriv (vrai résultat)
        for closed in await self.order_executor.poll_contract_resolutions():
            self._trades_closed += 1
            self.risk_manager.on_trade_closed(closed.pnl, closed.exit_price, closed.entry_price)
            self.logger.info(
                f"[CLÔTURE] {closed.direction.value} | "
                f"Entry={closed.entry_price:.5f} Exit={closed.exit_price:.5f} | "
                f"PnL=${closed.pnl:+.2f}"
            )
            await self._notify(
                f"🔴 *CLOTURE* {closed.direction.value} — {self.symbol}\n"
                f"Entry={closed.entry_price:.5f} Exit={closed.exit_price:.5f}\n"
                f"PnL=${closed.pnl:+.2f}"
            )
            self._sync_registry()

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
        await self._notify(
            f"🟢 *OUVERTURE* {signal.direction.value} — {self.symbol}\n"
            f"@ {signal.entry_price:.5f} | Amount=${order.amount:.2f}\n"
            f"SL={order.stop_loss:.5f} TP={order.take_profit:.5f}"
        )
        self._sync_registry()

    def _sync_registry(self) -> None:
        """Met à jour l'état de cette instance dans le registre partagé.

        Permet aux autres instances (et à /status //report via Telegram)
        de voir en temps réel les positions ouvertes, le P&L et le win rate
        de chaque indice, même dans des processus séparés.
        """
        try:
            report = self.risk_manager.get_report()
            positions = []
            for o in self.order_executor.active_orders:
                positions.append({
                    "direction": getattr(o.direction, "value", str(o.direction)),
                    "entry_price": o.entry_price,
                    "amount": o.amount,
                    "stop_loss": o.stop_loss,
                    "take_profit": o.take_profit,
                })
            update_instance(
                os.getpid(),
                symbol=self.symbol,
                positions=positions,
                pnl=round(report.total_pnl, 2),
                win_rate=round(self.risk_manager.win_rate * 100, 2),
                trades=self.risk_manager.total_trades,
                capital=round(report.current_capital, 2),
            )
        except Exception:
            self.logger.exception("Impossible de mettre à jour le registre d'instance")

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

        # Unregister instance
        try:
            unregister_instance(pid=os.getpid())
        except Exception:
            self.logger.exception("Impossible de desinscrire l'instance du registre")

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
        self._paused = False


class MultiProcessLauncher:
    """Lance des instances separées du script en arrière-plan pour chaque symbole.

    Utilise des processus distincts (subprocess) afin d'isoler les instances
    (évite de dupliquer les WebSocket et le polling Telegram dans le même
    processus). La commande Telegram attend la forme:
        /run SYM1,SYM2,...
    Exemple: `/run EURUSD,1HZ75V`
    """

    def __init__(self, config: Config, logger):
        self.config = config
        self.logger = logger

    def handle_run(self, full_text: str) -> str:
        parts = full_text.split(None, 1)
        if len(parts) < 2 or not parts[1].strip():
            return "Usage: /run SYM1,SYM2,... (ex: /run EURUSD,1HZ75V)"
        syms_text = parts[1]
        syms = [s.strip() for s in syms_text.replace(';', ',').split(',') if s.strip()]
        started = []
        script = Path(__file__).resolve()
        for sym in syms:
            # Resolve labels (ex: "Volatility 75" or "EUR/USD") to canonical codes
            try:
                resolved = resolve_symbol(sym)
            except Exception:
                resolved = sym.strip().upper()
            cmd = [sys.executable, str(script), "--symbol", resolved]
            try:
                env = dict(**os.environ)
                env["TELEGRAM_DISABLE"] = "1"
                p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                started.append(f"{sym} -> pid:{p.pid}")
            except Exception as e:
                err = f"{sym} -> error: {e}"
                self.logger.error(f"Echec demarrage {sym}: {e}")
                started.append(err)
        if not started:
            return "Aucun symbole demarre."
        return "Demarre: " + ", ".join(started)

    def handle_choose(self, full_text: str) -> str:
        """Selection numerique et lancement par index du catalogue.

        Usage:
          /choose           -> renvoie la liste numerotee des marches
          /choose 1,3,5     -> lance les items 1,3,5
          /choose 2-4       -> lance la plage 2 à 4
        """
        parts = full_text.split(None, 1)
        entries = list(MARKET_CATALOG.items())
        if len(parts) < 2 or not parts[1].strip():
            # Retourner la liste numerotee
            lines = [f"{i}) {label}: {code}" for i, (label, code) in enumerate(entries, start=1)]
            return "Liste des marchés:\n" + "\n".join(lines)

        sel = parts[1].strip()
        # Allow either numeric indices or symbol names/codes (comma-separated).
        tokens = [t.strip() for t in sel.replace(';', ',').split(',') if t.strip()]

        # If any token contains letters or non-digit chars (excluding '-'), treat all tokens as symbols
        treat_as_symbols = any(any(c.isalpha() for c in tok) for tok in tokens)

        to_launch: list[tuple[str, str]] = []  # (label, code)

        if treat_as_symbols:
            # Interpret tokens as symbol names/codes
            for tok in tokens:
                try:
                    resolved = resolve_symbol(tok)
                    to_launch.append((tok, resolved))
                except Exception:
                    # fallback to uppercase token as code
                    to_launch.append((tok, tok.strip().upper()))
        else:
            # Interpret as indices / ranges
            indices: list[int] = []
            for tok in tokens:
                if '-' in tok:
                    try:
                        a, b = tok.split('-', 1)
                        a_i = int(a)
                        b_i = int(b)
                        if a_i <= 0 or b_i <= 0:
                            return "Indices doivent etre des entiers positifs"
                        indices.extend(range(a_i, b_i + 1))
                    except Exception:
                        return f"Format invalide pour la plage: {tok}"
                else:
                    try:
                        idx = int(tok)
                        if idx <= 0:
                            return "Indices doivent etre des entiers positifs"
                        indices.append(idx)
                    except Exception:
                        return f"Index invalide: {tok}"

            # Dédupliquer et valider
            indices = sorted(set(indices))
            max_idx = len(entries)
            for idx in indices:
                if idx < 1 or idx > max_idx:
                    return f"Index hors limite: {idx} (1-{max_idx})"
                label, code = entries[idx - 1]
                to_launch.append((label, code))

        # Lancer et collecter résultats
        results = []
        script = Path(__file__).resolve()
        for label, code in to_launch:
            cmd = [sys.executable, str(script), "--symbol", code]
            try:
                env = dict(**os.environ)
                env["TELEGRAM_DISABLE"] = "1"
                p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                results.append(f"{label} ({code}) -> pid:{p.pid}")
            except Exception as e:
                self.logger.error(f"Echec demarrage {label} ({code}): {e}")
                results.append(f"{label} ({code}) -> error: {e}")

        if not results:
            return "Aucun symbole demarre."
        return "Resultats:\n" + "\n".join(results)


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
    # Lanceur pour démarrer d'autres instances du script via Telegram (/run)
    launcher = MultiProcessLauncher(config, engine.logger)
    engine.telegram.register_command("/run", launcher.handle_run)
    engine.telegram.register_command("/markets", lambda: "\n".join(list_markets()))
    engine.telegram.register_command("/choose", launcher.handle_choose)

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