"""Catalogue des marchés Deriv sélectionnables depuis Telegram.

Regroupe les indices synthétiques les plus utilisés et quelques paires de
devises (forex). Les codes "frx..." sont ceux attendus par l'API Deriv pour
les paires de devises.
"""

from __future__ import annotations

MARKET_CATALOG: dict[str, str] = {
    # Indices synthétiques — Volatility
    "Volatility 10": "1HZ10V",
    "Volatility 25": "1HZ25V",
    "Volatility 50": "1HZ50V",
    "Volatility 75": "1HZ75V",
    "Volatility 100": "1HZ100V",
    # Indices synthétiques — Jump
    "Jump 10": "JD10",
    "Jump 25": "JD25",
    "Jump 50": "JD50",
    "Jump 75": "JD75",
    "Jump 100": "JD100",
    # Indices synthétiques — Boom / Crash
    "Boom 300": "BOOM300N",
    "Boom 500": "BOOM500",
    "Boom 1000": "BOOM1000",
    "Crash 300": "CRASH300N",
    "Crash 500": "CRASH500",
    "Crash 1000": "CRASH1000",
    # Indice synthétique — Step
    "Step Index": "STPRNG",
    # Paires de devises (forex)
    "EUR/USD": "frxEURUSD",
    "GBP/USD": "frxGBPUSD",
    "USD/JPY": "frxUSDJPY",
    "USD/CHF": "frxUSDCHF",
    "USD/CAD": "frxUSDCAD",
    "AUD/USD": "frxAUDUSD",
    "EUR/GBP": "frxEURGBP",
    "EUR/JPY": "frxEURJPY",
}


def resolve_symbol(value: str) -> str:
    """Résout un code brut ou un libellé en code de symbole Deriv canonique.

    Args:
        value: Code brut (ex: "1HZ100V") ou libellé (ex: "Volatility 100").

    Returns:
        Code de symbole normalisé (majuscules).
    """
    normalized = value.strip().upper()

    for label, code in MARKET_CATALOG.items():
        if label.upper() == normalized or code.upper() == normalized:
            return code

    # On part du principe qu'il s'agit déjà d'un code brut (ex: "1HZ100V").
    return value.strip().upper()


def list_markets() -> list[str]:
    """Retourne les lignes affichables du catalogue (libellé: code)."""
    return [f"{label}: {code}" for label, code in MARKET_CATALOG.items()]