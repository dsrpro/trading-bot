"""Suivi des contrats ouverts — Monitor les positions actives et leur statut.

Utile en mode paper_trading et live pour suivre le cycle de vie des contrats Deriv.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from src.config import Config
from src.deriv_client import DerivClient


class ContractStatus(Enum):
    """Statut d'un contrat Deriv."""
    OPEN = "open"
    WON = "won"
    LOST = "lost"
    PURCHASED = "purchased"
    SOLD = "sold"


@dataclass
class Contract:
    """Representation d'un contrat ouvert."""

    contract_id: str
    symbol: str
    contract_type: str  # "CALL" ou "PUT"
    entry_price: float
    buy_price: float
    payout: float
    start_time: float
    expiry_time: Optional[float] = None
    status: ContractStatus = ContractStatus.OPEN
    sell_price: Optional[float] = None
    profit: Optional[float] = None
    exit_tick: Optional[float] = None


class ContractMonitor:
    """Moniteur de contrats ouverts.

    Responsabilites:
        - Suivi des contrats en temps reel
        - Detection des contrats termines (won/lost)
        - Notification des evenements de contrat
        - Reconciliation avec l'API Deriv
    """

    def __init__(
        self,
        config: Config,
        deriv_client: Optional[DerivClient] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.config = config
        self.deriv_client = deriv_client
        self.logger = logger or logging.getLogger("contract_monitor")

        self._contracts: dict[str, Contract] = {}
        self._completed_contracts: list[Contract] = []
        self._on_contract_complete_callbacks: list[callable] = []

    @property
    def active_contracts(self) -> list[Contract]:
        """Contrats actuellement ouverts."""
        return [c for c in self._contracts.values() if c.status == ContractStatus.OPEN]

    @property
    def has_active_contracts(self) -> bool:
        return len(self.active_contracts) > 0

    def on_contract_complete(self, callback: callable) -> None:
        """Enregistre un callback appele a chaque fin de contrat."""
        self._on_contract_complete_callbacks.append(callback)

    def register_contract(self, contract_data: dict) -> Optional[Contract]:
        """Enregistre un nouveau contrat depuis les donnees API.

        Args:
            contract_data: Donnees du contrat depuis l'API Deriv.

        Returns:
            Contract cree ou None si invalide.
        """
        contract_id = str(contract_data.get("contract_id", ""))
        if not contract_id:
            return None

        # Eviter les doublons
        if contract_id in self._contracts:
            return self._contracts[contract_id]

        contract = Contract(
            contract_id=contract_id,
            symbol=contract_data.get("symbol", self.config.market_symbol),
            contract_type=contract_data.get("contract_type", ""),
            entry_price=float(contract_data.get("entry_spot", 0)),
            buy_price=float(contract_data.get("buy_price", 0)),
            payout=float(contract_data.get("payout", 0)),
            start_time=float(contract_data.get("start_time", datetime.now(timezone.utc).timestamp())),
            expiry_time=contract_data.get("expiry_time"),
        )

        self._contracts[contract_id] = contract
        self.logger.info(
            f"Contrat enregistre #{contract_id} | "
            f"{contract.contract_type} @ {contract.entry_price:.5f} | "
            f"Buy=${contract.buy_price:.2f} Payout=${contract.payout:.2f}"
        )
        return contract

    def update_contract(self, update_data: dict) -> Optional[Contract]:
        """Met a jour le statut d'un contrat.

        Args:
            update_data: Donnees de mise a jour du contrat (ex: "proposal_open_contract").

        Returns:
            Contract mis a jour ou None.
        """
        contract_id = str(update_data.get("contract_id", ""))
        if not contract_id or contract_id not in self._contracts:
            return None

        contract = self._contracts[contract_id]

        # Mise a jour du statut
        is_sold = update_data.get("is_sold", False)
        if is_sold:
            contract.status = ContractStatus.SOLD
            contract.sell_price = float(update_data.get("sell_price", 0))
            contract.profit = float(update_data.get("profit", 0))
            self._on_contract_completed(contract)

        # Verifier si le contrat est termine (won/lost)
        status = update_data.get("status", "")
        if status == "won":
            contract.status = ContractStatus.WON
            contract.profit = float(update_data.get("profit", contract.payout - contract.buy_price))
            contract.exit_tick = float(update_data.get("exit_tick", 0))
            self._on_contract_completed(contract)
        elif status == "lost":
            contract.status = ContractStatus.LOST
            contract.profit = -contract.buy_price
            contract.exit_tick = float(update_data.get("exit_tick", 0))
            self._on_contract_completed(contract)

        return contract

    def check_tick_for_contracts(self, tick_price: float) -> list[Contract]:
        """Verifie les contrats actifs contre le prix courant (simulation dry-run).

        En dry-run, on ne recoit pas les updates de l'API, donc on simule
        la fermeture basee sur le prix actuel.

        Args:
            tick_price: Dernier prix du marche.

        Returns:
            Liste des contrats completes.
        """
        completed = []
        for contract in self.active_contracts:
            if contract.contract_type == "CALL":
                if tick_price >= contract.entry_price * 1.01:  # +1% → win
                    contract.status = ContractStatus.WON
                    contract.profit = contract.payout - contract.buy_price
                    completed.append(contract)
                    self._on_contract_completed(contract)
                elif tick_price <= contract.entry_price * 0.99:  # -1% → loss
                    contract.status = ContractStatus.LOST
                    contract.profit = -contract.buy_price
                    completed.append(contract)
                    self._on_contract_completed(contract)
            else:  # PUT
                if tick_price <= contract.entry_price * 0.99:
                    contract.status = ContractStatus.WON
                    contract.profit = contract.payout - contract.buy_price
                    completed.append(contract)
                    self._on_contract_completed(contract)
                elif tick_price >= contract.entry_price * 1.01:
                    contract.status = ContractStatus.LOST
                    contract.profit = -contract.buy_price
                    completed.append(contract)
                    self._on_contract_completed(contract)

        return completed

    def get_all_contracts(self) -> list[Contract]:
        """Tous les contrats (actifs + termines)."""
        return list(self._contracts.values()) + self._completed_contracts

    def get_total_profit(self) -> float:
        """Profit total de tous les contrats completes."""
        return sum(c.profit for c in self._completed_contracts if c.profit is not None)

    def get_win_rate(self) -> float:
        """Taux de reussite des contrats completes."""
        if not self._completed_contracts:
            return 0.0
        wins = sum(1 for c in self._completed_contracts if c.status == ContractStatus.WON)
        return wins / len(self._completed_contracts)

    def reset(self) -> None:
        """Reinitialise le moniteur."""
        self._contracts.clear()
        self._completed_contracts.clear()

    def _on_contract_completed(self, contract: Contract) -> None:
        """Appele lorsqu'un contrat est termine."""
        # Deplacer vers l'historique
        self._completed_contracts.append(contract)
        self._contracts.pop(contract.contract_id, None)

        self.logger.info(
            f"Contrat termine #{contract.contract_id} | "
            f"Status={contract.status.value} | "
            f"Profit=${contract.profit:.2f}" if contract.profit is not None else
            f"Contrat termine #{contract.contract_id} | Status={contract.status.value}"
        )

        # Notifier les callbacks
        for cb in self._on_contract_complete_callbacks:
            try:
                cb(contract)
            except Exception as e:
                self.logger.error(f"Erreur callback contrat: {e}")