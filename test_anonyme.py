"""Test rapide de connexion anonyme a l'API Deriv (sans token)."""
import asyncio
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import load_config
from src.logger import setup_logger
from src.deriv_client import DerivClient


async def main():
    config = load_config()
    # Forcer anonyme (pas de token)
    config = type(config)(
        deriv_token="",
        deriv_api_url=config.deriv_api_url,
        deriv_app_id=config.deriv_app_id,
    )
    logger = setup_logger(config, "test_anonyme")

    client = DerivClient(config, logger)

    print("[1] Connexion WebSocket (anonyme)...")
    success = await client.connect()
    if not success:
        print("[FAIL] Connexion impossible")
        return

    print("[OK] Connexion WebSocket etablie (anonyme)")

    # Test ping
    print("[2] Test ping/pong...")
    resp = await client._send_request({"ping": 1})
    if resp and resp.get("ping") == "pong":
        print("[OK] Ping/pong OK")
    else:
        print(f"[INFO] Reponse ping: {json.dumps(resp)}")

    # Souscription aux ticks
    print("[3] Souscription ticks R_75...")
    sub = await client.subscribe_ticks("R_75")
    if sub is None:
        print("[FAIL] Aucune reponse du serveur")
    elif sub.get("error"):
        err = sub["error"]
        print(f"[FAIL] Erreur: {err.get('code')} - {err.get('message')}")
        print("  -> L'acces anonyme aux ticks est peut-etre restreint.")
    elif sub.get("subscription"):
        sub_id = sub["subscription"].get("id", "?")
        print(f"[OK] Souscription reussie! ID={sub_id}")

        ticks = []
        client.on_tick(lambda t: ticks.append(t))
        print("  Attente de 5 secondes de ticks...")
        await asyncio.sleep(5)
        print(f"  Ticks recus: {len(ticks)}")
        if ticks:
            t = ticks[-1]
            print(f"  Dernier tick: epoch={t.get('epoch')} quote={t.get('quote')} symbol={t.get('symbol')}")
            print("[OK] Flux de ticks operationnel!")
        else:
            print("[WARN] Aucun tick recu en 5 secondes")
    else:
        print(f"[WARN] Reponse inattendue: {json.dumps(sub)[:300]}")

    await client.disconnect()
    print("\n[DONE] Test termine.")


if __name__ == "__main__":
    asyncio.run(main())