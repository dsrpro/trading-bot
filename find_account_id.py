"""Trouve automatiquement l'Account ID de ton compte Deriv demo via WebSocket."""
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
    logger = setup_logger(config, "find_account")

    print("=" * 60)
    print("  RECHERCHE DE L'ACCOUNT ID DERIV (VRTC...)")
    print("=" * 60)

    if not config.deriv_token:
        print("\n[ERREUR] Aucun token dans config/settings.env")
        return

    print(f"\nToken : {config.deriv_token[:30]}...")
    print(f"App ID: {config.deriv_app_id}")

    # Connexion WebSocket publique
    client = DerivClient(config, logger)
    print("\n[1] Connexion au WebSocket public...")
    if not await client.connect():
        print("[FAIL]")
        return
    print("[OK] Connecté")

    # Essayer authorize pour récupérer le login_id
    print("\n[2] Authentification avec le token...")
    auth_resp = await client._send_request({"authorize": config.deriv_token})
    if auth_resp and auth_resp.get("authorize"):
        auth = auth_resp["authorize"]
        login_id = auth.get("loginid", "")
        print(f"[OK] Login ID trouvé : {login_id}")
        print(f"     Nom    : {auth.get('fullname', '?')}")
        print(f"     Email  : {auth.get('email', '?')}")
        print(f"     Devise : {auth.get('currency', '?')}")
        print(f"\n>>> Utilise cet ID dans le test OTP : {login_id}")
        print(f"    python test_otp_trading.py (puis entre {login_id})")
        await client.disconnect()
        return

    # Si authorize échoue, essayer de lister les comptes
    print("[INFO] Authorize non supporté sur endpoint public")
    print("\n[3] Essai de récupération de la liste des comptes...")

    # Via l'API publique, on peut get_limits ou account_list
    for method in [
        {"website_status": 1},
        {"get_account_status": 1},
    ]:
        resp = await client._send_request(method)
        if resp and resp.get("error"):
            print(f"  {list(method.keys())[0]} : {resp['error'].get('message')}")
        elif resp:
            print(f"  {list(method.keys())[0]} : {json.dumps(resp)[:200]}")

    await client.disconnect()

    print("\n" + "=" * 60)
    print("  COMMENT TROUVER TON VRTC...")
    print("=" * 60)
    print("""
1. Va sur https://app.deriv.com/ (interface web, PAS MT5)
2. Connecte-toi avec ton compte Deriv
3. En haut à droite, clique sur ton nom
4. Regarde le champ 'Login ID' ou 'ID du compte'
5. Il doit commencer par VRTC (démo) ou CR (réel)

Exemples:
  - VRTC1234567  (compte démo)
  - CR90012345   (compte réel)

⚠ NE PAS utiliser le Login ID de MT5 (41207424)
   Celui-ci est pour la plateforme de trading, pas pour l'API.
""")


if __name__ == "__main__":
    asyncio.run(main())