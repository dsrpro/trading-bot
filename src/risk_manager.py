"""Gestion des risques — Controle strict du capital, drawdown, et limites quotidiennes.

Applique les regles du Plan 1 Section 4:
    - 2% risque maximum par trade
    - 5% stop-loss quotidien
    - 20% drawdown maximum
    - Kill switch automatique
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from src.config import Config
from src.strategy_engine import SignalDirection, TradingSignal


class RiskStatus(Enum):
    """Statut de gestion des risques."""
    OK = "OK"
    DAILY_LOSS_LIMIT_REACHED = "DAILY_LOSS_LIMIT_REACHED"
    DAILY_PROFIT_TARGET_REACHED = "DAILY_PROFIT_TARGET_REACHED"
    MAX_DRAWDOWN_REACHED = "MAX_DRAWDOWN_REACHED"
    MAX_TRADES_REACHED = "MAX_TRADES_REACHED"
    KILL_SWITCH_ACTIVATED = "KILL_SWITCH_ACTIVATED"


@dataclass
class RiskReport:
    """Rapport d'etat de la gestion des risques."""

    status: RiskStatus = RiskStatus.OK
    initial_capital: float = 0.0
    current_capital: float = 0.0
    daily_pnl: float = 0.0
    daily_pnl_pct: float = 0.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    drawdown_pct: float = 0.0
    trades_today: int = 0
    max_trades_per_day: int = 2
    position_size: float = 0.0
    can_trade: bool = True
    reason_blocked: str = ""
    peak_capital: float = 0.0


class RiskManager:
    """Gestionnaire de risques du bot de trading.

    Responsabilites:
        - Calcul de la taille de position selon le risque defini
        - Suivi du P&L quotidien et total
        - Verification des limites avant chaque trade
        - Kill switch automatique en cas de depassement des seuils
    """

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger("risk_manager")

        # Capital
        self.initial_capital: float = config.initial_capital
        self.current_capital: float = config.initial_capital
        self.peak_capital: float = config.initial_capital

        # Suivi quotidien
        self._daily_pnl: float = 0.0
        self._trades_today: int = 0
        self._daily_start_capital: float = config.initial_capital
        self._last_trade_date: Optional[str] = None

        # Kill switch
        self._kill_switch: bool = False
        self._blocked_reason: str = ""

        # Historique
        self.total_trades: int = 0
        self.winning_trades: int = 0
        self.losing_trades: int = 0
        self.total_pnl: float = 0.0

    def set_initial_capital(self, capital: float) -> None:
        """Reinitialise le capital de reference depuis le solde reel du compte.

        A utiliser au demarrage du paper/live trading apres avoir recupere le
        solde via l'API, afin que le calcul de position et le P&L soient
        coherents avec le vrai solde du compte.

        Args:
            capital: Solde reel du compte (ex: 9987.41 USD).
        """
        if capital <= 0:
            self.logger.warning(f"Capital invalide ignore: {capital}")
            return
        self.initial_capital = capital
        self.current_capital = capital
        self.peak_capital = capital
        self._daily_start_capital = capital
        self._daily_pnl = 0.0
        self._trades_today = 0
        self.total_pnl = 0.0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_trades = 0
        self.logger.info(f"Capital initialise depuis le solde du compte: ${capital:.2f}")

    def _reset_daily_if_needed(self) -> None:
        """Reinitialise les compteurs quotidiens si un nouveau jour commence."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._last_trade_date is None:
            # Premiere initialisation : definir la date sans reset
            self._last_trade_date = today
            self._daily_start_capital = self.current_capital
        elif self._last_trade_date != today:
            # Nouveau jour : reinitialiser les compteurs
            self._daily_pnl = 0.0
            self._trades_today = 0
            self._daily_start_capital = self.current_capital
            self._last_trade_date = today
            self.logger.info(f"Nouveau jour de trading: {today} | Capital: ${self.current_capital:.2f}")

    def can_place_trade(self, signal: TradingSignal) -> tuple[bool, RiskReport]:
        """Verifie si un trade peut etre place selon les regles de risque.

        Args:
            signal: Signal de trading a evaluer.

        Returns:
            (autorise, rapport) — True si le trade est autorise.
        """
        self._reset_daily_if_needed()

        report = RiskReport(
            initial_capital=self.initial_capital,
            current_capital=self.current_capital,
            daily_pnl=self._daily_pnl,
            daily_pnl_pct=(self._daily_pnl / self._daily_start_capital * 100.0) if self._daily_start_capital > 0 else 0.0,
            total_pnl=self.total_pnl,
            total_pnl_pct=(self.total_pnl / self.initial_capital * 100.0) if self.initial_capital > 0 else 0.0,
            drawdown_pct=self._calculate_drawdown_pct(),
            trades_today=self._trades_today,
            max_trades_per_day=self.config.max_trades_per_day,
            position_size=0.0,
            peak_capital=self.peak_capital,
        )

        # 1. Kill switch
        if self._kill_switch:
            report.status = RiskStatus.KILL_SWITCH_ACTIVATED
            report.can_trade = False
            report.reason_blocked = self._blocked_reason
            self.logger.warning(f"Trade bloque: KILL SWITCH - {self._blocked_reason}")
            return False, report

        # 2. Stop-loss quotidien (en % et en USD)
        daily_loss = abs(min(self._daily_pnl, 0))
        daily_loss_pct = (daily_loss / self._daily_start_capital * 100.0) if self._daily_start_capital > 0 else 0.0
        stop_loss_usd_reached = getattr(self.config, 'daily_stop_loss_usd', 0) > 0 and daily_loss >= self.config.daily_stop_loss_usd
        if daily_loss_pct >= self.config.daily_stop_loss_pct or stop_loss_usd_reached:
            report.status = RiskStatus.DAILY_LOSS_LIMIT_REACHED
            report.can_trade = False
            report.reason_blocked = f"Perte quotidienne (PnL=${self._daily_pnl:.2f}, {daily_loss_pct:.2f}%) atteinte"
            self.logger.warning(f"Trade bloque: {report.reason_blocked}")
            self._kill_switch = True
            self._blocked_reason = report.reason_blocked
            return False, report

        # 3. Objectif de profit quotidien (en % et en USD)
        if self._daily_pnl > 0:
            daily_profit_pct = (self._daily_pnl / self._daily_start_capital * 100.0)
            profit_usd_target = getattr(self.config, 'daily_profit_target_usd', 0.0)
            profit_usd_reached = profit_usd_target > 0 and self._daily_pnl >= profit_usd_target
            if daily_profit_pct >= self.config.daily_profit_target_pct or profit_usd_reached:
                report.status = RiskStatus.DAILY_PROFIT_TARGET_REACHED
                report.can_trade = False
                report.reason_blocked = f"Objectif quotidien de profit (${self._daily_pnl:.2f}) atteint"
                self.logger.info(f"Trade bloque: {report.reason_blocked}")
                return False, report

        # 4. Drawdown maximum (20%)
        drawdown = self._calculate_drawdown_pct()
        if drawdown >= self.config.max_drawdown_pct:
            report.status = RiskStatus.MAX_DRAWDOWN_REACHED
            report.can_trade = False
            report.reason_blocked = f"Drawdown {drawdown:.2f}% >= {self.config.max_drawdown_pct}%"
            self.logger.warning(f"Trade bloque: {report.reason_blocked}")
            self._kill_switch = True
            self._blocked_reason = report.reason_blocked
            return False, report

        # 5. Nombre maximum de trades par jour
        if self._trades_today >= self.config.max_trades_per_day:
            report.status = RiskStatus.MAX_TRADES_REACHED
            report.can_trade = False
            report.reason_blocked = f"Max {self.config.max_trades_per_day} trades/jour atteint"
            self.logger.info(f"Trade bloque: {report.reason_blocked}")
            return False, report

        # 6. Verification du signal
        if not signal.is_valid:
            report.can_trade = False
            report.reason_blocked = "Signal invalide (HOLD ou confiance trop faible)"
            return False, report

        # 7. Calcul de la taille de position
        position_size = self._calculate_position_size()
        report.position_size = position_size

        if position_size <= 0:
            report.can_trade = False
            report.reason_blocked = "Capital insuffisant pour ouvrir une position"
            return False, report

        report.can_trade = True
        report.status = RiskStatus.OK
        return True, report

    def on_trade_opened(self, amount: float) -> None:
        """Enregistre l'ouverture d'un trade.

        Args:
            amount: Montant du stake.
        """
        self._trades_today += 1
        self.total_trades += 1
        self.logger.info(
            f"Trade ouvert | Amount=${amount:.2f} | "
            f"Trade #{self._trades_today} du jour | "
            f"Capital=${self.current_capital:.2f}"
        )

    def on_trade_closed(self, pnl: float, exit_price: float, entry_price: float) -> RiskReport:
        """Enregistre la fermeture d'un trade et met a jour le capital.

        Args:
            pnl: Profit/Perte du trade.
            exit_price: Prix de sortie.
            entry_price: Prix d'entree.

        Returns:
            Rapport de risque mis a jour.
        """
        self._reset_daily_if_needed()

        self.current_capital += pnl
        self._daily_pnl += pnl
        self.total_pnl += pnl

        if pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1

        # Mise a jour du peak capital
        if self.current_capital > self.peak_capital:
            self.peak_capital = self.current_capital

        drawdown = self._calculate_drawdown_pct()

        self.logger.info(
            f"Trade ferme | PnL=${pnl:.2f} | "
            f"Entry=${entry_price:.5f} Exit=${exit_price:.5f} | "
            f"Capital=${self.current_capital:.2f} | "
            f"Daily PnL=${self._daily_pnl:.2f} | Drawdown={drawdown:.2f}%"
        )

        # Verifier si le drawdown depasse la limite
        if drawdown >= self.config.max_drawdown_pct:
            self._kill_switch = True
            self._blocked_reason = f"Drawdown {drawdown:.2f}% >= {self.config.max_drawdown_pct}%"
            self.logger.warning(f"KILL SWITCH ACTIVE: {self._blocked_reason}")

        return self.get_report()

    def activate_kill_switch(self, reason: str) -> None:
        """Active le kill switch manuellement.

        Args:
            reason: Raison de l'arret d'urgence.
        """
        self._kill_switch = True
        self._blocked_reason = reason
        self.logger.critical(f"KILL SWITCH MANUEL: {reason}")

    def deactivate_kill_switch(self) -> None:
        """Desactive le kill switch."""
        self._kill_switch = False
        self._blocked_reason = ""
        # Reinitialiser le drawdown tracker
        self._daily_start_capital = self.current_capital
        self._daily_pnl = 0.0
        self.logger.info("Kill switch desactive — compteurs reinitialises")

    def reset_daily_counters(self) -> None:
        """Reinitialise manuellement les compteurs quotidiens."""
        self._daily_pnl = 0.0
        self._trades_today = 0
        self._daily_start_capital = self.current_capital
        self._last_trade_date = None

    def get_report(self) -> RiskReport:
        """Retourne le rapport de risque actuel."""
        self._reset_daily_if_needed()
        daily_pnl_pct = (self._daily_pnl / self._daily_start_capital * 100.0) if self._daily_start_capital > 0 else 0.0

        daily_loss = abs(min(self._daily_pnl, 0))
        stop_loss_usd_reached = getattr(self.config, 'daily_stop_loss_usd', 0) > 0 and daily_loss >= self.config.daily_stop_loss_usd
        profit_usd_target = getattr(self.config, 'daily_profit_target_usd', 0.0)
        profit_usd_reached = profit_usd_target > 0 and self._daily_pnl >= profit_usd_target

        is_daily_stop_hit = (
            (daily_pnl_pct <= -self.config.daily_stop_loss_pct)
            or stop_loss_usd_reached
        )
        is_daily_profit_hit = (
            (self._daily_pnl > 0 and daily_pnl_pct >= self.config.daily_profit_target_pct)
            or profit_usd_reached
        )

        can_trade = (
            not self._kill_switch
            and self._trades_today < self.config.max_trades_per_day
            and self._calculate_drawdown_pct() < self.config.max_drawdown_pct
            and not is_daily_profit_hit
            and not is_daily_stop_hit
        )

        if not can_trade:
            if self._kill_switch:
                status = RiskStatus.KILL_SWITCH_ACTIVATED
            elif is_daily_profit_hit:
                status = RiskStatus.DAILY_PROFIT_TARGET_REACHED
            elif is_daily_stop_hit:
                status = RiskStatus.DAILY_LOSS_LIMIT_REACHED
            elif self._trades_today >= self.config.max_trades_per_day:
                status = RiskStatus.MAX_TRADES_REACHED
            else:
                status = RiskStatus.MAX_DRAWDOWN_REACHED
        else:
            status = RiskStatus.OK

        return RiskReport(
            status=status,
            initial_capital=self.initial_capital,
            current_capital=self.current_capital,
            daily_pnl=self._daily_pnl,
            daily_pnl_pct=daily_pnl_pct,
            total_pnl=self.total_pnl,
            total_pnl_pct=(self.total_pnl / self.initial_capital * 100.0) if self.initial_capital > 0 else 0.0,
            drawdown_pct=self._calculate_drawdown_pct(),
            trades_today=self._trades_today,
            max_trades_per_day=self.config.max_trades_per_day,
            position_size=self._calculate_position_size(),
            can_trade=can_trade,
            reason_blocked=self._blocked_reason,
            peak_capital=self.peak_capital,
        )

    def _calculate_position_size(self) -> float:
        """Calcule la taille de position basee sur le risque par trade (2%), capee a max_stake_usd.

        Returns:
            Montant du stake en USD.
        """
        risk_amount = self.current_capital * (self.config.risk_per_trade_pct / 100.0)
        max_stake = getattr(self.config, 'max_stake_usd', 25.0)
        if max_stake > 0:
            risk_amount = min(risk_amount, max_stake)
        return round(max(risk_amount, float(self.config.min_stake)), 2)

    def _calculate_drawdown_pct(self) -> float:
        """Calcule le drawdown actuel en pourcentage.

        Returns:
            Drawdown en pourcentage (0-100).
        """
        if self.peak_capital <= 0:
            return 0.0
        return (self.peak_capital - self.current_capital) / self.peak_capital * 100.0

    @property
    def win_rate(self) -> float:
        """Taux de victoire (0.0-1.0)."""
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades

    @property
    def is_kill_switch_active(self) -> bool:
        return self._kill_switch

    @property
    def trades_today(self) -> int:
        self._reset_daily_if_needed()
        return self._trades_today