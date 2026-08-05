"""Systeme de logging structure pour le bot de trading.

Supporte les formats JSON et console avec rotation automatique.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from src.config import Config


class JsonFormatter(logging.Formatter):
    """Formateur de logs au format JSON structure."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = str(record.exc_info[1])
        return json.dumps(log_entry, ensure_ascii=False)


class ColorConsoleFormatter(logging.Formatter):
    """Formateur console avec couleurs ANSI."""

    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Vert
        "WARNING": "\033[33m",   # Jaune
        "ERROR": "\033[31m",     # Rouge
        "CRITICAL": "\033[1;31m", # Rouge gras
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        return (
            f"{color}[{timestamp}] [{record.levelname:8s}] "
            f"[{record.name}] {record.getMessage()}{self.RESET}"
        )


def setup_logger(config: Config, name: str = "trading_bot") -> logging.Logger:
    """Configure et retourne le logger principal.

    Args:
        config: Instance de Config contenant les parametres de logging.
        name: Nom du logger.

    Returns:
        Logger configure.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))

    # Eviter les doublons de handlers
    if logger.handlers:
        return logger

    # Formatteur selon la configuration
    if config.log_format == "json":
        # Handler fichier avec rotation
        log_file = Path(config.logs_dir) / "bot.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            str(log_file),
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=10,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)

        # Handler console en couleur pour les logs critiques
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(ColorConsoleFormatter())
        logger.addHandler(console_handler)
    else:
        # Mode console uniquement
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(ColorConsoleFormatter())
        logger.addHandler(console_handler)

    # Fichier dedie pour les erreurs
    error_log_file = Path(config.logs_dir) / "errors.log"
    error_handler = RotatingFileHandler(
        str(error_log_file),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(JsonFormatter())
    logger.addHandler(error_handler)

    return logger


def log_trade(
    logger: logging.Logger,
    action: str,
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: Optional[float] = None,
    pnl: Optional[float] = None,
    balance: Optional[float] = None,
    extra: Optional[dict] = None,
) -> None:
    """Enregistre un trade dans les logs de maniere structuree.

    Args:
        logger: Logger configure.
        action: Action (BUY, SELL, CLOSE).
        symbol: Symbole du marche.
        direction: Direction (CALL, PUT).
        entry_price: Prix d'entree.
        exit_price: Prix de sortie (optionnel).
        pnl: Profit/Perte (optionnel).
        balance: Solde apres le trade (optionnel).
        extra: Donnees supplementaires (optionnel).
    """
    trade_data = {
        "event": "trade",
        "action": action,
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "pnl": pnl,
        "balance": balance,
    }
    if extra:
        trade_data.update(extra)
    logger.info(json.dumps(trade_data, ensure_ascii=False))


def log_signal(
    logger: logging.Logger,
    symbol: str,
    direction: str,
    score: float,
    confidence: float,
    strategy: str,
    indicators: Optional[dict] = None,
) -> None:
    """Enregistre un signal de trading dans les logs.

    Args:
        logger: Logger configure.
        symbol: Symbole du marche.
        direction: Direction (CALL, PUT).
        score: Score du signal (0-100).
        confidence: Confiance (0.0-1.0).
        strategy: Nom de la strategie.
        indicators: Valeurs des indicateurs (optionnel).
    """
    signal_data = {
        "event": "signal",
        "symbol": symbol,
        "direction": direction,
        "score": score,
        "confidence": confidence,
        "strategy": strategy,
    }
    if indicators:
        signal_data["indicators"] = indicators
    logger.info(json.dumps(signal_data, ensure_ascii=False))


def log_error(logger: logging.Logger, error_type: str, message: str, details: Optional[dict] = None) -> None:
    """Enregistre une erreur structuree.

    Args:
        logger: Logger configure.
        error_type: Type d'erreur (API, CONNECTION, STRATEGY, RISK, etc.).
        message: Message descriptif.
        details: Details supplementaires (optionnel).
    """
    error_data = {
        "event": "error",
        "error_type": error_type,
        "message": message,
    }
    if details:
        error_data["details"] = details
    logger.error(json.dumps(error_data, ensure_ascii=False))