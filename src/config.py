"""Configuration centralisee du bot de trading.

Charge les variables depuis un fichier .env et expose une dataclass validee.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Config:
    """Configuration immutable du bot de trading."""

    # --- Chemins ---
    base_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    config_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "config")
    logs_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "logs")
    strategies_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent / "strategies")

    # --- Deriv API ---
    deriv_app_id: str = "1089"
    deriv_api_url: str = "wss://ws.derivws.com/websockets/v3"
    deriv_token: str = ""
    deriv_account_type: str = "demo"  # "demo" ou "real"

    # --- Trading ---
    market_symbol: str = "R_75"
    timeframe: str = "M1"
    max_trades_per_day: int = 2
    daily_profit_target_pct: float = 4.0

    # --- Bollinger Bands ---
    bb_period: int = 20
    bb_stddev: float = 2.0

    # --- RSI ---
    rsi_period: int = 14
    rsi_oversold: float = 30.0
    rsi_overbought: float = 70.0

    # --- Risk Management ---
    risk_per_trade_pct: float = 2.0
    daily_stop_loss_pct: float = 5.0
    max_drawdown_pct: float = 20.0
    sl_pips: int = 20
    tp_pips: int = 100
    risk_reward_ratio: float = 5.0

    # --- Capital de depart (demo) ---
    initial_capital: float = 500.0

    # --- Backtesting ---
    backtest_years: int = 3
    backtest_initial_capital: float = 100.0
    backtest_commission_pct: float = 0.0
    backtest_spread_pips: float = 2.0

    # --- Execution ---
    mode: str = "dry_run"  # "dry_run", "paper_trading", "live"
    reconnect_attempts: int = 5
    reconnect_delay_seconds: float = 2.0
    tick_buffer_size: int = 1000

    # --- Logging ---
    log_level: str = "INFO"
    log_format: str = "json"  # "json" ou "console"

    def __post_init__(self) -> None:
        """Validation des parametres apres l'initialisation."""
        assert self.risk_per_trade_pct > 0, "risk_per_trade_pct doit etre > 0"
        assert self.daily_stop_loss_pct > 0, "daily_stop_loss_pct doit etre > 0"
        assert self.max_drawdown_pct > 0, "max_drawdown_pct doit etre > 0"
        assert self.max_trades_per_day > 0, "max_trades_per_day doit etre > 0"
        assert self.mode in ("dry_run", "paper_trading", "live"), f"mode invalide: {self.mode}"
        assert self.log_format in ("json", "console"), f"log_format invalide: {self.log_format}"


def load_config(env_file: Optional[str] = None) -> Config:
    """Charge la configuration depuis un fichier .env et les variables d'environnement.

    Args:
        env_file: Chemin vers un fichier .env. Si None, utilise config/settings.env

    Returns:
        Une instance de Config validee.
    """
    if env_file is None:
        env_file = str(Path(__file__).resolve().parent.parent / "config" / "settings.env")

    # Chargement du fichier .env si present
    if os.path.isfile(env_file):
        _load_dotenv(env_file)

    return Config(
        deriv_app_id=_env_str("DERIV_APP_ID", "1089"),
        deriv_api_url=_env_str("DERIV_API_URL", "wss://ws.derivws.com/websockets/v3"),
        deriv_token=_env_str("DERIV_TOKEN", ""),
        deriv_account_type=_env_str("DERIV_ACCOUNT_TYPE", "demo"),
        market_symbol=_env_str("MARKET_SYMBOL", "R_75"),
        timeframe=_env_str("TIMEFRAME", "M1"),
        max_trades_per_day=_env_int("MAX_TRADES_PER_DAY", 2),
        daily_profit_target_pct=_env_float("DAILY_PROFIT_TARGET_PCT", 4.0),
        bb_period=_env_int("BB_PERIOD", 20),
        bb_stddev=_env_float("BB_STDDEV", 2.0),
        rsi_period=_env_int("RSI_PERIOD", 14),
        rsi_oversold=_env_float("RSI_OVERSOLD", 30.0),
        rsi_overbought=_env_float("RSI_OVERBOUGHT", 70.0),
        risk_per_trade_pct=_env_float("RISK_PER_TRADE_PCT", 2.0),
        daily_stop_loss_pct=_env_float("DAILY_STOP_LOSS_PCT", 5.0),
        max_drawdown_pct=_env_float("MAX_DRAWDOWN_PCT", 20.0),
        sl_pips=_env_int("SL_PIPS", 20),
        tp_pips=_env_int("TP_PIPS", 100),
        risk_reward_ratio=_env_float("RISK_REWARD_RATIO", 5.0),
        initial_capital=_env_float("INITIAL_CAPITAL", 500.0),
        backtest_years=_env_int("BACKTEST_YEARS", 3),
        backtest_initial_capital=_env_float("BACKTEST_INITIAL_CAPITAL", 100.0),
        backtest_commission_pct=_env_float("BACKTEST_COMMISSION_PCT", 0.0),
        backtest_spread_pips=_env_float("BACKTEST_SPREAD_PIPS", 2.0),
        mode=_env_str("MODE", "dry_run"),
        reconnect_attempts=_env_int("RECONNECT_ATTEMPTS", 5),
        reconnect_delay_seconds=_env_float("RECONNECT_DELAY_SECONDS", 2.0),
        tick_buffer_size=_env_int("TICK_BUFFER_SIZE", 1000),
        log_level=_env_str("LOG_LEVEL", "INFO"),
        log_format=_env_str("LOG_FORMAT", "json"),
    )


def _load_dotenv(filepath: str) -> None:
    """Charge les variables d'un fichier .env dans os.environ (format simple KEY=VALUE)."""
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('\'"')
            # Ne pas ecraser les variables d'environnement existantes
            if key not in os.environ:
                os.environ[key] = value


def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default