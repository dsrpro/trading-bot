"""Catalogue des marchés Deriv sélectionnables depuis Telegram.

Regroupe les indices synthétiques (Volatility, Jump, Step, Boom/Crash) et
quelques paires de devises (forex). Les codes sont ceux attendus par l'API
Deriv (interrogés via `active_symbols`).

Nota: la Volatility standard (ex: "Volatility 10 Index") et la variante
"(1s)" sont des symboles distincts (R_* vs 1HZ*V). Le catalogue distingue
les deux pour éviter toute confusion.
"""

from __future__ import annotations

import re

# Volatility — standard (pas de "(1s)")
_VOLATILITY_STANDARD: dict[str, str] = {
    "Volatility 10 Index": "R_10",
    "Volatility 25 Index": "R_25",
    "Volatility 50 Index": "R_50",
    "Volatility 75 Index": "R_75",
    "Volatility 100 Index": "R_100",
}

# Volatility — variantes "(1s)"
_VOLATILITY_1S: dict[str, str] = {
    "Volatility 10 (1s) Index": "1HZ10V",
    "Volatility 15 (1s) Index": "1HZ15V",
    "Volatility 25 (1s) Index": "1HZ25V",
    "Volatility 30 (1s) Index": "1HZ30V",
    "Volatility 50 (1s) Index": "1HZ50V",
    "Volatility 75 (1s) Index": "1HZ75V",
    "Volatility 90 (1s) Index": "1HZ90V",
    "Volatility 100 (1s) Index": "1HZ100V",
}

_JUMP: dict[str, str] = {
    "Jump 10 Index": "JD10",
    "Jump 25 Index": "JD25",
    "Jump 50 Index": "JD50",
    "Jump 75 Index": "JD75",
    "Jump 100 Index": "JD100",
}

_STEP: dict[str, str] = {
    "Step Index 100": "stpRNG",
    "Step Index 200": "stpRNG2",
    "Step Index 300": "stpRNG3",
    "Step Index 400": "stpRNG4",
    "Step Index 500": "stpRNG5",
}

_BOOM_CRASH: dict[str, str] = {
    "Boom 300": "BOOM300N",
    "Boom 500": "BOOM500",
    "Boom 1000": "BOOM1000",
    "Crash 300": "CRASH300N",
    "Crash 500": "CRASH500",
    "Crash 1000": "CRASH1000",
}

_FOREX: dict[str, str] = {
    "EUR/USD": "frxEURUSD",
    "GBP/USD": "frxGBPUSD",
    "USD/JPY": "frxUSDJPY",
    "USD/CHF": "frxUSDCHF",
    "USD/CAD": "frxUSDCAD",
    "AUD/USD": "frxAUDUSD",
    "EUR/GBP": "frxEURGBP",
    "EUR/JPY": "frxEURJPY",
}

MARKET_CATALOG: dict[str, str] = {
    **_VOLATILITY_STANDARD,
    **_VOLATILITY_1S,
    **_JUMP,
    **_STEP,
    **_BOOM_CRASH,
    **_FOREX,
}


def _normalize(value: str) -> str:
    """Normalise un libelle : casse + espaces + suppression du mot 'index'."""
    text = " ".join(value.strip().upper().split())
    # Retirer un éventuel "INDEX" final (ex: "Volatility 10 Index" -> "VOLATILITY 10")
    text = re.sub(r"\s+INDEX$", "", text)
    return text


def resolve_symbol(value: str) -> str:
    """Résout un code brut ou un libellé en code de symbole Deriv canonique.

    Exemple:
        resolve_symbol("Volatility 100 (1s)") -> "1HZ100V"
        resolve_symbol("1HZ100V")             -> "1HZ100V"
        resolve_symbol("step index 300")      -> "stpRNG3"

    Args:
        value: Code brut (ex: "1HZ100V") ou libellé (ex: "Volatility 100 (1s) Index").

    Returns:
        Code de symbole normalisé (majuscules, sauf les codes Step qui sont
        volontairement en minuscules car Deriv les attend ainsi).
    """
    text = value.strip()

    # 1. Match direct sur un code de symbole connu (insensible à la casse)
    for label, code in MARKET_CATALOG.items():
        if text.upper() == code.upper():
            return code  # préserver la casse exacte du code (ex: stpRNG3)

    # 2. Match sur le libellé normalisé
    norm = _normalize(text)
    for label, code in MARKET_CATALOG.items():
        if _normalize(label) == norm:
            return code

    # 3. Normaliser les codes "stpRNG..." fournis en majuscules ("STPRNG3")
    lower_norm = text.replace(" ", "").upper()
    if lower_norm.startswith("STPRNG"):
        suffix = lower_norm[len("STPRNG"):]
        code = "stpRNG" + suffix
        if code in MARKET_CATALOG.values():
            return code

    # 4. On part du principe qu'il s'agit déjà d'un code brut (ex: "1HZ100V").
    return text.upper()


def list_markets() -> list[str]:
    """Retourne les lignes affichables du catalogue (libellé: code)."""
    return [f"{label}: {code}" for label, code in MARKET_CATALOG.items()]


def market_codes() -> list[str]:
    """Retourne la liste des codes de symboles du catalogue."""
    return list(MARKET_CATALOG.values())
