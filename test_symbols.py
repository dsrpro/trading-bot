"""Test de plusieurs symboles d'indices synthetiques Deriv pour trouver les bons."""
import asyncio
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import load_config
from src.logger import setup_logger
from src.deriv_client import DerivClient


# Anciens et nouveaux symboles possibles
SYMBOLS_TO_TEST = [
    # Anciens noms
    "R_75", "R_100", "R_50", "R_25", "R_10",
    # Noms modernes possibles
    "1HZ75V", "1HZ100V", "1HZ50V", "1HZ25V", "1HZ10V",
    "BOOM500", "BOOM1000", "BOOM300N",
    "CRASH500", "CRASH1000", "CRASH300N",
    "JD75", "JD100",
    "VOL75", "VOL100", "VOL50", "VOL25", "VOL10",
    "VOLATILITY75", "VOLATILITY100",
    "frxEURUSD",  # Pour tester si l'API repond du tout
]


async def test_symbol(client, symbol, logger):
    """Teste un symbole."""
    sub = await client.subscribe_ticks(symbol)
    if sub is None:
        return None, "No response"
    if sub.get("error"):
        return False, sub["error"].get("code", "?")
    if sub.get("subscription"):
        sub_id = sub["subscription"].get("id", "")
        # Essayer de recevoir un tick
        ticks = []
        client.on_tick(lambda t: ticks.append(t))
        await asyncio.sleep(2)
        await client.unsubscribe_ticks(symbol)
        return True, f"ID={sub_id}, ticks={len(ticks)}"
    return None, f"Unexpected: {str(sub)[:100]}"


async def main():
    config = load_config()
    # Anonyme pour tester les symboles
    config = type(config)(
        deriv_token="",
        deriv_api_url=config.deriv_api_url,
        deriv_app_id=config.deriv_app_id,
    )
    logger = setup_logger(config, "test_symbols")

    client = DerivClient(config, logger)
    print("[CONNECT] Connexion a l'API Deriv...")
    if not await client.connect():
        print("[FAIL] Connexion echouee")
        return

    print("[OK] Connecte\n")

    valid_symbols = []
    invalid_symbols = []

    for symbol in SYMBOLS_TO_TEST:
        status, detail = await test_symbol(client, symbol, logger)
        if status is True:
            print(f"  [VALID]   {symbol:20s} -> {detail}")
            valid_symbols.append(symbol)
        elif status is False:
            codestr = detail
            print(f"  [INVALID] {symbol:20s} -> {codestr}")
            invalid_symbols.append((symbol, codestr))
        else:
            print(f"  [ERROR]   {symbol:20s} -> {detail}")

    await client.disconnect()

    print(f"\n{'='*50}")
    print(f"Symboles valides ({len(valid_symbols)}): {', '.join(valid_symbols) if valid_symbols else 'AUCUN'}")
    print(f"Symboles refuses: {len(invalid_symbols)}")
    if valid_symbols:
        print(f"\n[RECOMMENDATION] Utilisez {valid_symbols[0]} dans config/settings.env")
    else:
        print("\n[ATTENTION] Aucun symbole valide trouve en anonyme.")
        print("L'API peut exiger un token pour acceder aux indices synthetiques.")


if __name__ == "__main__":
    asyncio.run(main())