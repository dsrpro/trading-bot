"""Tests unitaires pour la strategie de Scalping ($60/day profit target) et MultiSymbolScalper.
"""

import asyncio
import numpy as np
import pytest
from unittest.mock import MagicMock

from src.config import Config, load_config
from src.risk_manager import RiskManager, RiskStatus
from src.strategy_engine import StrategyEngine, SignalDirection, TradingSignal
from src.indicators import Indicators
from src.candle_builder import CandleBuilder
from src.scalper_multi import MultiSymbolScalper


def test_scalping_config_defaults():
    """Vérifie le chargement des paramètres par défaut de scalping."""
    config = load_config()
    assert config.daily_profit_target_usd == 60.0
    assert config.daily_stop_loss_usd == 60.0
    assert config.max_trades_per_day == 15
    assert "1HZ100V" in config.scalping_symbols


def test_risk_manager_daily_profit_target_usd():
    """Vérifie le verrouillage automatique dès que +$60 USD de profit est atteint."""
    config = Config(
        initial_capital=1000.0,
        daily_profit_target_usd=60.0,
        daily_stop_loss_usd=40.0,
        max_trades_per_day=15,
    )
    rm = RiskManager(config)

    # 1. Simuler 2 trades gagnants de +$35 chacun -> total +$70
    signal = TradingSignal(direction=SignalDirection.CALL, symbol="R_75", confidence=0.8)
    can_trade, report = rm.can_place_trade(signal)
    assert can_trade is True

    rm.on_trade_opened(20.0)
    rm.on_trade_closed(pnl=35.0, exit_price=105.0, entry_price=100.0)

    rm.on_trade_opened(20.0)
    rep = rm.on_trade_closed(pnl=35.0, exit_price=105.0, entry_price=100.0)

    assert rep.daily_pnl == 70.0

    # 2. Tenter un nouveau trade -> doit être bloqué par DAILY_PROFIT_TARGET_REACHED
    can_trade_next, rep_next = rm.can_place_trade(signal)
    assert can_trade_next is False
    assert rep_next.status == RiskStatus.DAILY_PROFIT_TARGET_REACHED


def test_risk_manager_daily_stop_loss_usd():
    """Vérifie l'activation du kill-switch dès que -$40 USD de perte est atteint."""
    config = Config(
        initial_capital=1000.0,
        daily_profit_target_usd=60.0,
        daily_stop_loss_usd=40.0,
        max_trades_per_day=15,
    )
    rm = RiskManager(config)

    signal = TradingSignal(direction=SignalDirection.CALL, symbol="R_75", confidence=0.8)

    # Simuler 2 trades perdants de -$25 chacun -> total -$50
    rm.on_trade_opened(20.0)
    rm.on_trade_closed(pnl=-25.0, exit_price=95.0, entry_price=100.0)

    rm.on_trade_opened(20.0)
    rep = rm.on_trade_closed(pnl=-25.0, exit_price=95.0, entry_price=100.0)

    assert rep.daily_pnl == -50.0

    # Tenter un nouveau trade -> doit être bloqué par DAILY_LOSS_LIMIT_REACHED
    can_trade_next, rep_next = rm.can_place_trade(signal)
    assert can_trade_next is False
    assert rep_next.status in (RiskStatus.DAILY_LOSS_LIMIT_REACHED, RiskStatus.KILL_SWITCH_ACTIVATED)


def test_strategy_engine_scalping_parameters():
    """Vérifie l'initialisation des paramètres scalping dans StrategyEngine."""
    config = Config(
        rsi_period=7,
        bb_period=14,
        atr_sl_multiplier=1.0,
        atr_tp_multiplier=1.5,
    )
    cb = CandleBuilder(config)
    ind = Indicators(config)
    se = StrategyEngine(config, ind, cb)

    assert se.atr_sl_multiplier == 1.0
    assert se.atr_tp_multiplier == 1.5
    assert se.rsi_oversold_tight == 25.0
    assert se.rsi_overbought_tight == 75.0


@pytest.mark.asyncio
async def test_multi_symbol_scalper_dry_run():
    """Vérifie le fonctionnement de MultiSymbolScalper en simulation."""
    config = Config(
        scalping_symbols="R_10,R_25",
        initial_capital=1000.0,
        daily_profit_target_usd=60.0,
    )
    scalper = MultiSymbolScalper(config=config)
    assert len(scalper.symbols) == 2
    assert "R_10" in scalper.symbols
    assert "R_25" in scalper.symbols

    summary = await scalper.run(duration_minutes=0.05)
    assert summary["initial_capital"] == 1000.0
    assert "R_10" in summary["symbols_scanned"]
