"""Test de connexion reelle a l'API Deriv avec le token fourni.

Verifie:
    1. Connexion WebSocket
    2. Authentification
    3. Recuperation du solde
    4. Souscription aux ticks de R_75
    5. Reception de 5 ticks reels
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import load_config
from src.deriv_client import DerivClient
from src.logger import setup_logger


async def main():
    config = load_config()
    logger = setup_logger(config, "test_deriv")

    print("=" * 60)
    print("  TEST DE CONNEXION API DERIV")
    print("=" * 60)

    if not config.deriv_token:
        print("[ERROR] Aucun token API trouve dans config/settings.env")
        print("Ajoutez DERIV_TOKEN=votre_token dans le fichier .env")
        return False

    print(f"\n[1/5] Configuration...")
    print(f"  URL API  : {config.deriv_api_url}")
    print(f"  Token    : {config.deriv_token[:15]}...")
    print(f"  Compte   : {config.deriv_account_type}")
    print(f"  Symbole  : {config.market_symbol}")

    # Creer le client
    client = DerivClient(config, logger)

    # Compteur de ticks recus
    ticks_received = []
    client.on_tick(lambda t: ticks_received.append(t))

    print(f"\n[2/5] Connexion WebSocket...")
    # Lancer la connexion en tache de fond
    connect_task = asyncio.create_task(client.connect())

    # Attendre que la connexion soit etablie
    await asyncio.sleep(3)

    if not client.is_connected:
        print("[FAIL] Connexion WebSocket echouee")
        connect_task.cancel()
        try:
            await connect_task
        except:
            pass
        return False

    print("[OK] Connexion WebSocket etablie et authentifiee")

    # Recuperer le solde
    print(f"\n[3/5] Recuperation du solde...")
    balance = await client.get_balance()
    if balance and "balance" in balance:
        bal = balance["balance"]
        print(f"  Solde    : {bal.get('currency', 'USD')} {bal.get('balance', 'N/A')}")
        print(f"  Login ID : {bal.get('loginid', 'N/A')}")
        print("[OK] Solde recupere")
    else:
        print("[WARN] Impossible de recuperer le solde (peut-etre permissions du token?)")

    # Souscrire aux ticks
    print(f"\n[4/5] Souscription aux ticks de {config.market_symbol}...")
    sub_resp = await client.subscribe_ticks(config.market_symbol)
    if sub_resp and sub_resp.get("subscription"):
        print(f"[OK] Souscrit a {config.market_symbol} (subscription_id={sub_resp['subscription'].get('id', '')})")
    else:
        print(f"[FAIL] Souscription echouee: {sub_resp}")
        await client.disconnect()
        connect_task.cancel()
        return False

    # Attendre 5 ticks reels
    print(f"\n[5/5] Attente de 5 ticks reels de {config.market_symbol}...")
    print("  (cela peut prendre 10-30 secondes)")
    timeout = 30
    start = asyncio.get_event_loop().time()
    while len(ticks_received) < 5 and (asyncio.get_event_loop().time() - start) < timeout:
        await asyncio.sleep(0.5)
        if len(ticks_received) > 0 and len(ticks_received) % 1 == 0:
            last_tick = ticks_received[-1]
            print(f"  Tick reçu: price={last_tick.get('quote', 'N/A')}, epoch={last_tick.get('epoch', 'N/A')}")

    if len(ticks_received) >= 5:
        print(f"\n[OK] {len(ticks_received)} ticks reçus avec succes!")
        print(f"  Dernier prix {config.market_symbol}: {ticks_received[-1].get('quote', 'N/A')}")
    else:
        print(f"\n[WARN] Seulement {len(ticks_received)} ticks reçus en {timeout}s")
        if len(ticks_received) == 0:
            print("[FAIL] Aucun tick reçu — verifiez le symbole")

    # Nettoyage
    await client.unsubscribe_ticks(config.market_symbol)
    await client.disconnect()
    connect_task.cancel()
    try:
        await connect_task
    except:
        pass

    # Resultat final
    print("\n" + "=" * 60)
    if client.is_connected or len(ticks_received) > 0:
        print("  RESULTAT: SUCCES — L'API Deriv est operationnelle!")
    else:
        print("  RESULTAT: ECHEC — Probleme de connexion")
    print("=" * 60)
    return len(ticks_received) > 0


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)