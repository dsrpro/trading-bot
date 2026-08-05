"""Test du nouvel endpoint public Deriv API v2."""
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
    logger = setup_logger(config, "test_new_api")

    client = DerivClient(config, logger)

    print("[1] Connexion au nouvel endpoint public...")
    print(f"    URL: {DerivClient.PUBLIC_WS_URL}?app_id={config.deriv_app_id}")
    success = await client.connect()
    if not success:
        print("[FAIL] Connexion echouee")
        return

    print("[OK] Connecte au endpoint public!\n")

    # Test 1: Active symbols
    print("[2] Recuperation des symboles actifs...")
    symbols_resp = await client.get_active_symbols()
    if symbols_resp is None:
        print("[FAIL] Aucune reponse")
    elif symbols_resp.get("error"):
        err = symbols_resp["error"]
        print(f"[FAIL] {err.get('code')} - {err.get('message')}")
        print(f"  Full response: {json.dumps(symbols_resp)[:500]}")
    elif "active_symbols" in symbols_resp:
        symbols = symbols_resp["active_symbols"]
        print(f"[OK] {len(symbols)} symboles actifs!\n")

        # Chercher les indices synthetiques
        synthetic = []
        for s in symbols:
            name = str(s.get("display_name", "")).lower()
            market = str(s.get("market", "")).lower()
            sym = str(s.get("symbol", ""))
            if any(kw in name or kw in market or kw in sym.lower()
                   for kw in ["volatility", "synthetic", "boom", "crash", "jump", "range break"]):
                synthetic.append(s)

        if synthetic:
            print(f"Indices synthetiques ({len(synthetic)}):")
            for s in synthetic[:20]:
                print(f"  {s.get('symbol', '?'):25s} | {s.get('display_name', '?')}")
        else:
            print("Aucun indice synthetique trouve.")
            print("\nTous les symboles (premiers 30):")
            for s in symbols[:30]:
                print(f"  {s.get('symbol', '?'):30s} | {s.get('market', '?')} | {s.get('display_name', s.get('name', '?'))}")

        # Test souscription sur le premier symbole
        if symbols:
            first = synthetic[0] if synthetic else symbols[0]
            first_symbol = first.get("symbol", "")
            print(f"\n[3] Test souscription sur '{first_symbol}'...")
            sub = await client.subscribe_ticks(first_symbol)
            if sub is None:
                print("[FAIL] Aucune reponse (tous formats echoues)")
            elif sub.get("error"):
                print(f"[FAIL] {sub['error'].get('code')} - {sub['error'].get('message')}")
            elif sub.get("subscription"):
                ticks = []
                client.on_tick(lambda t: ticks.append(t))
                print(f"[OK] Souscrit! Attente 3s de ticks...")
                await asyncio.sleep(3)
                print(f"[OK] {len(ticks)} ticks recus")
                if ticks:
                    t = ticks[-1]
                    print(f"  Quote={t.get('quote')}, epoch={t.get('epoch')}")
            elif sub.get("tick"):
                print(f"[OK] Tick direct recu: {json.dumps(sub.get('tick'))}")
            elif sub.get("ticks_history"):
                print(f"[OK] Ticks history: {json.dumps(sub['ticks_history'])[:300]}")
            else:
                print(f"[INFO] Reponse (peut etre OK): {json.dumps(sub)[:300]}")
    else:
        print(f"[WARN] Reponse inattendue: {json.dumps(symbols_resp)[:500]}")

    await client.disconnect()
    print("\n[DONE]")


if __name__ == "__main__":
    asyncio.run(main())