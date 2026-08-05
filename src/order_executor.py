"""Executeur d'ordres — Gere l'execution des trades (achat/vente).

Supporte trois modes:
    - dry_run: Simulation complete sans connexion API
    - paper_trading: Connexion compte demo Deriv
    - live: Connexion compte reel (non implémente sans validation prealable)
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from src.config import Config
from src.deriv_client import DerivClient
from src.logger import log_trade
from src.strategy_engine import SignalDirection, TradingSignal


class OrderStatus(Enum):
    """Statut d'un ordre."""
    PENDING = "PENDING"
    EXECUTED = "EXECUTED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    SIMULATED = "SIMULATED"


@dataclass
class Order:
    """Representation d'un ordre de trading."""

    order_id: str
    symbol: str
    direction: SignalDirection
    entry_price: float
    amount: float
    stop_loss: float
    take_profit: float
    status: OrderStatus = OrderStatus.PENDING
    opened_at: float = 0.0
    closed_at: float = 0.0
    exit_price: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    contract_id: Optional[str] = None
    signal_score: float = 0.0
    signal_confidence: float = 0.0

    def close(self, exit_price: float, pnl: float) -> None:
        """Ferme l'ordre avec le prix et PnL specifies."""
        self.exit_price = exit_price
        self.pnl = pnl
        self.pnl_pct = (pnl / self.amount * 100.0) if self.amount > 0 else 0.0
        self.status = OrderStatus.COMPLETED if self.status != OrderStatus.SIMULATED else OrderStatus.SIMULATED
        self.closed_at = datetime.now(timezone.utc).timestamp()

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "entry_price": self.entry_price,
            "amount": self.amount,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "status": self.status.value,
            "pnl": self.pnl,
            "pnl_pct": self.pnl_pct,
            "signal_score": self.signal_score,
            "signal_confidence": self.signal_confidence,
        }


class OrderExecutor:
    """Executeur d'ordres mult-mode.

    Responsabilites:
        - Execution des trades en mode dry_run / paper_trading / live
        - Simulation realiste avec spread, slippage, et frais
        - Gestion du cycle de vie de l'ordre
        - Retry et gestion des erreurs
    """

    def __init__(
        self,
        config: Config,
        deriv_client: Optional[DerivClient] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config
        self.deriv_client = deriv_client
        self.logger = logger or logging.getLogger("order_executor")

        self._active_orders: dict[str, Order] = {}
        self._order_history: list[Order] = []
        self._simulation_latency_ms: float = 100.0  # Latence simulee

    @property
    def active_orders(self) -> list[Order]:
        return list(self._active_orders.values())

    @property
    def has_active_orders(self) -> bool:
        return len(self._active_orders) > 0

    async def execute_signal(self, signal: TradingSignal, amount: float) -> Optional[Order]:
        """Execute un signal de trading.

        Args:
            signal: Signal de trading valide.
            amount: Montant du stake.

        Returns:
            Order cree ou None si l'execution echoue.
        """
        if self.config.mode == "dry_run":
            return await self._execute_dry_run(signal, amount)
        elif self.config.mode == "paper_trading":
            return await self._execute_paper_trading(signal, amount)
        elif self.config.mode == "live":
            self.logger.warning("Mode LIVE demande — non implemente pour des raisons de securite")
            return None
        else:
            self.logger.error(f"Mode d'execution inconnu: {self.config.mode}")
            return None

    async def simulate_price_movement(self, order: Order, current_price: float) -> Optional[Order]:
        """Simule le mouvement de prix et verifie SL/TP pour un ordre actif.

        Args:
            order: Ordre actif.
            current_price: Prix actuel du marche.

        Returns:
            Order mis a jour si ferme (SL/TP touche), None sinon.
        """
        if order.direction == SignalDirection.CALL:
            # CALL: profit si le prix monte
            if current_price <= order.stop_loss:
                pnl = -order.amount  # Perte totale simplifiee
                order.close(current_price, pnl)
                self.logger.info(f"SL touche CALL | Exit={current_price:.5f} SL={order.stop_loss:.5f} PnL=${pnl:.2f}")
                self._archive_order(order)
                return order
            if current_price >= order.take_profit:
                pnl = order.amount * self.config.risk_reward_ratio  # R:R 1:5
                order.close(current_price, pnl)
                self.logger.info(f"TP touche CALL | Exit={current_price:.5f} TP={order.take_profit:.5f} PnL=${pnl:.2f}")
                self._archive_order(order)
                return order
        else:
            # PUT: profit si le prix baisse
            if current_price >= order.stop_loss:
                pnl = -order.amount
                order.close(current_price, pnl)
                self.logger.info(f"SL touche PUT | Exit={current_price:.5f} SL={order.stop_loss:.5f} PnL=${pnl:.2f}")
                self._archive_order(order)
                return order
            if current_price <= order.take_profit:
                pnl = order.amount * self.config.risk_reward_ratio
                order.close(current_price, pnl)
                self.logger.info(f"TP touche PUT | Exit={current_price:.5f} TP={order.take_profit:.5f} PnL=${pnl:.2f}")
                self._archive_order(order)
                return order

        return None  # Toujours ouvert

    def close_all_orders(self, current_price: float) -> list[Order]:
        """Ferme tous les ordres actifs au prix du marche.

        Args:
            current_price: Prix actuel.

        Returns:
            Liste des ordres fermes.
        """
        closed = []
        for order in list(self._active_orders.values()):
            if order.direction == SignalDirection.CALL:
                pnl = (current_price - order.entry_price) / order.entry_price * order.amount
            else:
                pnl = (order.entry_price - current_price) / order.entry_price * order.amount
            order.close(current_price, pnl)
            self._archive_order(order)
            closed.append(order)
            self.logger.info(f"Fermeture forcee | Order={order.order_id} | PnL=${pnl:.2f}")
        return closed

    # ── Methodes privees ────────────────────────────────────────────

    async def _execute_dry_run(self, signal: TradingSignal, amount: float) -> Order:
        """Execution en mode dry-run (simulation complete)."""
        # Simuler latence reseau
        await asyncio.sleep(self._simulation_latency_ms / 1000.0)

        # Simuler un leger slippage (0.01-0.05%)
        slippage = signal.entry_price * 0.0002  # 0.02% slippage
        if signal.direction == SignalDirection.CALL:
            entry_price = signal.entry_price + slippage
        else:
            entry_price = signal.entry_price - slippage

        order_id = str(uuid.uuid4())[:8]
        order = Order(
            order_id=order_id,
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=entry_price,
            amount=amount,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            status=OrderStatus.SIMULATED,
            opened_at=datetime.now(timezone.utc).timestamp(),
            signal_score=signal.score,
            signal_confidence=signal.confidence,
        )

        self._active_orders[order_id] = order

        log_trade(
            self.logger,
            action="OPEN",
            symbol=order.symbol,
            direction=order.direction.value,
            entry_price=order.entry_price,
            balance=None,
            extra={
                "order_id": order_id,
                "amount": amount,
                "sl": order.stop_loss,
                "tp": order.take_profit,
                "mode": "dry_run",
                "signal_score": signal.score,
            },
        )

        self.logger.info(
            f"[DRY_RUN] Ordre simule #{order_id} | "
            f"{signal.direction.value} @ {entry_price:.5f} | "
            f"Amount=${amount:.2f} | SL={order.stop_loss:.5f} TP={order.take_profit:.5f}"
        )

        return order

    async def _execute_paper_trading(self, signal: TradingSignal, amount: float) -> Optional[Order]:
        """Execution en mode paper trading (compte demo Deriv)."""
        if not self.deriv_client or not self.deriv_client.is_connected:
            self.logger.warning("Paper trading: client Deriv non connecte — fallback dry_run")
            return await self._execute_dry_run(signal, amount)

        # Obtenir une proposition
        contract_type = "CALL" if signal.direction == SignalDirection.CALL else "PUT"
        proposal = await self.deriv_client.get_proposal(
            contract_type=contract_type,
            symbol=signal.symbol,
            amount=amount,
        )

        if not proposal or proposal.get("error"):
            error_msg = proposal.get("error", {}).get("message", "Unknown") if proposal else "No response"
            self.logger.error(f"Proposition echouee: {error_msg}")
            return None

        proposal_id = proposal.get("proposal", {}).get("id", "")
        ask_price = float(proposal.get("proposal", {}).get("ask_price", 0))

        if not proposal_id:
            self.logger.error("Proposition sans ID")
            return None

        # Acheter le contrat
        buy_result = await self.deriv_client.buy_contract(proposal_id, ask_price)

        if not buy_result or buy_result.get("error"):
            error_msg = buy_result.get("error", {}).get("message", "Unknown") if buy_result else "No response"
            self.logger.error(f"Achat echoue: {error_msg}")
            return None

        contract_id = str(buy_result.get("buy", {}).get("contract_id", ""))

        order_id = str(uuid.uuid4())[:8]
        order = Order(
            order_id=order_id,
            symbol=signal.symbol,
            direction=signal.direction,
            entry_price=signal.entry_price,
            amount=amount,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            status=OrderStatus.EXECUTED,
            opened_at=datetime.now(timezone.utc).timestamp(),
            contract_id=contract_id,
            signal_score=signal.score,
            signal_confidence=signal.confidence,
        )

        self._active_orders[order_id] = order

        log_trade(
            self.logger,
            action="OPEN",
            symbol=order.symbol,
            direction=order.direction.value,
            entry_price=order.entry_price,
            balance=None,
            extra={
                "order_id": order_id,
                "contract_id": contract_id,
                "amount": amount,
                "mode": "paper_trading",
            },
        )

        self.logger.info(
            f"[PAPER] Ordre execute #{order_id} | "
            f"Contract={contract_id} | {signal.direction.value} | "
            f"Amount=${amount:.2f}"
        )

        return order

    def _archive_order(self, order: Order) -> None:
        """Deplace un ordre de actif vers l'historique."""
        self._active_orders.pop(order.order_id, None)
        self._order_history.append(order)