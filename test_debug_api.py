"""Debug: inspecter la structure exacte des reponses de la nouvelle API Deriv."""
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
    logger = setup_logger(config, "debug")

    client = DerivClient(config, logger)
    await client.connect()

    print("=== active_symbols (full) ===")
    resp = await client._send_request({"active_symbols": "full"})
    if resp and "active_symbols" in resp:
        symbols = resp["active_symbols"]
        print(f"Total: {len(symbols)} symboles\n")

        # Afficher les indices synthetiques avec le bon champ: underlying_symbol
        print("--- Indices synthetiques ---")
        synthetic_found = []
        for s in symbols:
            name = str(s.get("underlying_symbol_name", ""))
            sym = str(s.get("underlying_symbol", ""))
            if any(kw in name.lower() or kw in sym.lower()
                   for kw in ["volatility", "boom", "crash", "jump", "range"]):
                print(f"  {sym:20s} | {s.get('market', '?'):20s} | {name}")
                synthetic_found.append(s)

        print(f"\n{len(synthetic_found)} indices synthetiques trouves")

        # Test souscription sur le premier
        if synthetic_found:
            first_sym = synthetic_found[0].get("underlying_symbol", "")
            print(f"\n=== Test souscription sur '{first_sym}' ===")

            formats = [
                {"ticks_history": first_sym, "subscribe": 1},
                {"ticks_history": first_sym, "end": "latest", "subscribe": 1},
                {"ticks": first_sym, "subscribe": 1},
            ]

            for i, fmt in enumerate(formats):
                print(f"\n  Format {i+1}: {json.dumps(fmt)}")
                sub = await client._send_request(fmt)
                if sub is None:
                    print("  -> Aucune reponse")
                elif sub.get("error"):
                    print(f"  -> Erreur: {sub['error'].get('code')} - {sub['error'].get('message')}")
                else:
                    print(f"  -> SUCCES! Reponse: {json.dumps(sub)[:400]}")
                    if sub.get("subscription"):
                        ticks = []
                        client.on_tick(lambda t: ticks.append(t))
                        print("  Attente 3 secondes de ticks live...")
                        await asyncio.sleep(3)
                        print(f"  Ticks reçus: {len(ticks)}")
                        if ticks:
                            t = ticks[-1]
                            print(f"  Dernier tick: quote={t.get('quote')}, epoch={t.get('epoch')}, symbol={t.get('symbol')}")
                            print("\n[SUCCES] Flux de ticks operationnel!")
                    break

    await client.disconnect()
    print("\n[DONE]")


if __name__ == "__main__":
    asyncio.run(main())