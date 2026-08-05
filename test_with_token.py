"""Test avec token pour trouver les symboles disponibles et valider l'auth."""
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

    if not config.deriv_token:
        print("[ERROR] Aucun token dans config/settings.env")
        return

    logger = setup_logger(config, "test_token")

    client = DerivClient(config, logger)

    print("[1] Connexion + Auth avec token...")
    print(f"    Token: {config.deriv_token[:20]}...")
    success = await client.connect()
    if not success:
        print("[FAIL] Connexion/auth echouee")
        return

    print("[OK] Authentifie avec succes!")

    # Recuperer la liste des symboles disponibles
    print("\n[2] Recuperation des symboles actifs...")
    active_symbols_resp = await client._send_request({"active_symbols": "brief"})
    if active_symbols_resp is None:
        print("[FAIL] Aucune reponse du serveur")
    elif active_symbols_resp.get("error"):
        err = active_symbols_resp["error"]
        print(f"[FAIL] Erreur: {err.get('code')} - {err.get('message')}")
        print("  -> Le token n'a pas les permissions suffisantes.")
        print("  -> Verifiez les scopes du token: Read, Trade, Payments, Admin")
    elif "active_symbols" in active_symbols_resp:
        symbols = active_symbols_resp["active_symbols"]
        print(f"[OK] {len(symbols)} symboles disponibles!\n")

        # Filtrer pour les indices synthetiques
        synthetic = [s for s in symbols if "synthetic" in str(s.get("market", "")).lower()
                     or "volatility" in str(s.get("display_name", "")).lower()
                     or "boom" in str(s.get("display_name", "")).lower()
                     or "crash" in str(s.get("display_name", "")).lower()
                     or "jump" in str(s.get("display_name", "")).lower()
                     or "range" in str(s.get("display_name", "")).lower()]

        if synthetic:
            print(f"Indices synthetiques trouves ({len(synthetic)}):")
            for s in synthetic[:15]:
                print(f"  {s.get('symbol', '?'):20s} -> {s.get('display_name', s.get('name', '?'))}")
        else:
            print("Aucun indice synthetique identifie dans les symboles.")

        # Afficher tous les symboles pour inspection
        print(f"\nTous les symboles disponibles (premiers 20):")
        for s in symbols[:20]:
            print(f"  {s.get('symbol', '?'):25s} | {s.get('market', '?')} | {s.get('display_name', s.get('name', '?'))}")

        # Tester la souscription sur le premier symbole trouve
        if synthetic:
            first_symbol = synthetic[0].get("symbol")
            print(f"\n[3] Test souscription sur {first_symbol}...")
            sub = await client.subscribe_ticks(first_symbol)
            if sub and sub.get("subscription"):
                ticks = []
                client.on_tick(lambda t: ticks.append(t))
                print(f"[OK] Souscrit! Attente de 3 secondes de ticks...")
                await asyncio.sleep(3)
                print(f"[OK] {len(ticks)} ticks reçus")
                if ticks:
                    t = ticks[-1]
                    print(f"  Prix={t.get('quote')}, epoch={t.get('epoch')}")
                await client.unsubscribe_ticks(first_symbol)
            elif sub and sub.get("error"):
                print(f"[FAIL] {sub['error'].get('message')}")
            else:
                print(f"[FAIL] Reponse inattendue: {sub}")
    else:
        print(f"[WARN] Reponse inattendue: {json.dumps(active_symbols_resp)[:500]}")

    # Test balance
    print("\n[4] Recuperation du solde...")
    bal = await client.get_balance()
    if bal and bal.get("balance"):
        b = bal["balance"]
        print(f"[OK] Solde: {b.get('currency', 'USD')} {b.get('balance', '?')}")
        print(f"     Login: {b.get('loginid', '?')}")
        print(f"     Type:  {b.get('account_type', bal.get('account_type', '?'))}")
    else:
        print(f"[WARN] Solde indisponible. Reponse: {json.dumps(bal)[:200] if bal else 'None'}")

    await client.disconnect()
    print("\n[DONE]")


if __name__ == "__main__":
    asyncio.run(main())