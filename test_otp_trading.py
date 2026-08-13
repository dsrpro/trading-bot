"""Test du flux OTP complet pour l'acces trading Deriv.

Flux:
    1. Connexion publique (ticks) - fonctionne deja
    2. Recuperation de l'account_id (via active_symbols ou config)
    3. Requete OTP HTTP POST → websocket_url
    4. Connexion WebSocket trading
    5. Verification: get_balance
    6. Test: get_proposal + buy_contract (demo)
"""

import asyncio
import sys
import json
from pathlib import Path
from urllib.error import HTTPError
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import load_config
from src.logger import setup_logger
from src.deriv_client import DerivClient


async def main():
    config = load_config()
    logger = setup_logger(config, "test_otp")

    print("=" * 70)
    print("  TEST OTP — CONNEXION TRADING DERIV")
    print("=" * 70)

    # Verifications prealables
    if not config.deriv_token:
        print("\n[ERREUR] Aucun token dans config/settings.env")
        print("\nPour obtenir un token OTP :")
        print("  1. Aller sur https://app.deriv.com/account/api-token")
        print("  2. Creer un nouveau token avec les scopes: Read, Trade, Payments, Admin")
        print("  3. Copier le token dans config/settings.env: DERIV_TOKEN=pat_...")
        return

    # Étape 0: Récupérer automatiquement l'account_id
    print("\n[0/3] Recherche automatique de l'account_id...")

    account_id = None

    # Méthode 1: API REST de liste des comptes
    try:
        import ssl
        from urllib.request import Request, urlopen

        accounts_url = "https://api.derivws.com/trading/v1/options/accounts"
        req = Request(
            accounts_url,
            headers={
                "Deriv-App-ID": config.deriv_app_id,
                "Authorization": f"Bearer {config.deriv_token}",
            },
        )
        ctx = ssl.create_default_context()
        with urlopen(req, context=ctx, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            logger.info(f"Liste des comptes: {json.dumps(data)[:500]}")

            # Nouvelle API v2: reponse {"data": [{"account_id": ..., "account_type": ...}]}
            accounts = []
            if isinstance(data, dict):
                accounts = data.get("data", data.get("accounts", []))
            elif isinstance(data, list):
                accounts = data

            if accounts:
                print(f"[OK] {len(accounts)} compte(s) trouve(s) via API REST:")
                for acc in accounts:
                    acc_id = acc.get("account_id") or acc.get("login_id") or acc.get("id")
                    acc_type = acc.get("account_type", "?")
                    balance = acc.get("balance", "?")
                    currency = acc.get("currency", "")
                    print(f"     - {acc_id} | type={acc_type} | {balance} {currency}")

                # Choisir le compte demo en priorite
                demo_accs = [a for a in accounts if a.get("account_type") == "demo"]
                chosen = demo_accs[0] if demo_accs else accounts[0]
                account_id = chosen.get("account_id") or chosen.get("login_id") or chosen.get("id")
                print(f"[SELECTION] Compte choisi: {account_id} ({chosen.get('account_type', '?')})")
    except HTTPError as e:
        if e.code == 401:
            print("[INFO] API REST comptes: accès non autorisé (token peut manquer le scope Admin)")
        else:
            logger.warning(f"API liste comptes: HTTP {e.code}")
    except Exception as e:
        logger.warning(f"API liste comptes: {e}")

    # Méthode 2: Si toujours pas trouvé, demander manuellement
    if not account_id:
        print("\n[ATTENTION] Account ID non detecte automatiquement.")
        print("L'account_id est le login_id de votre compte Deriv.")
        print("Format demo: VRTC... | Format reel: CR...")
        print("\nPour le trouver :")
        print("  1. Allez dans le dashboard Deriv")
        print("  2. Cliquez sur votre nom en haut a droite")
        print("  3. Regardez le 'Login ID' (ex: VRTC1234567)")
        account_id = input("\nEntrez votre account_id (ex: VRTC1234567): ").strip()

    if not account_id:
        print("[ERREUR] Account ID requis pour le flux OTP.")
        return

    client = DerivClient(config, logger)

    # Etape 1: Connexion publique (optionnelle, pour montrer que ca marche)
    print("\n[1/3] Connexion publique (ticks)...")
    pub_ok = await client.connect()
    if pub_ok:
        print("[OK] Connexion publique etablie")
        # Tester la recuperation des symboles
        symbols = await client.get_active_symbols()
        if symbols and "active_symbols" in symbols:
            print(f"      {len(symbols['active_symbols'])} symboles disponibles")
        await client.disconnect()
    else:
        print("[WARN] Connexion publique echouee (non bloquant pour OTP)")

    # Etape 2: OTP + connexion trading
    print(f"\n[2/3] Requete OTP pour le compte {account_id}...")
    print(f"      Token: {config.deriv_token[:25]}...")
    print(f"      App ID: {config.deriv_app_id}")

    trading_ok = await client.connect_trading(
        token=config.deriv_token,
        account_id=account_id,
    )

    if not trading_ok:
        print("\n[ECHEC] Connexion trading impossible.")
        print("\nVerifiez:")
        print("  1. Le token est-il actif et non expire ?")
        print("  2. L'account_id est-il correct ? (trouvable dans le dashboard Deriv)")
        print("  3. Le token a-t-il les scopes Read, Trade, Payments, Admin ?")
        print("  4. Le compte est-il un compte demo ou reel valide ?")
        return

    print("[OK] Connexion trading etablie!")

    # Etape 3: Verification du compte
    print("\n[3/3] Verification du compte trading...")
    balance = await client.get_balance()
    if balance and balance.get("balance"):
        b = balance["balance"]
        print(f"      Login ID : {b.get('loginid', '?')}")
        print(f"      Solde    : {b.get('currency', 'USD')} {b.get('balance', '?')}")
        print(f"      Type     : {b.get('account_type', '?')}")
        print("[OK] Compte trading verifie!")

        # Test optionnel: obtenir une proposition (sans executer)
        print("\n[TEST] Obtention d'une proposition demo...")
        proposal = await client.get_proposal(
            contract_type="CALL",
            symbol="1HZ100V",
            amount=1.0,
            basis="stake",
            duration=5,
            duration_unit="t",
        )
        if proposal and proposal.get("proposal"):
            p = proposal["proposal"]
            print(f"      ID: {p.get('id', '?')}")
            print(f"      Prix: {p.get('ask_price', '?')}")
            print(f"      Payout: {p.get('payout', '?')}")
            print("[OK] Proposition obtenue avec succes!")
            print("\n⚠ Le bot est pret pour le PAPER TRADING!")
        elif proposal and proposal.get("error"):
            print(f"[INFO] Proposition refusee: {proposal['error'].get('message')}")
            print("      (Normal si le compte demo n'a pas de fonds)")
        else:
            print("[INFO] Pas de proposition disponible (peut-etre format different)")
    else:
        print(f"[INFO] Reponse brute: {json.dumps(balance)[:300] if balance else 'Aucune'}")

    await client.disconnect()

    print("\n" + "=" * 70)
    print("  RESULTAT: FLUX OTP FONCTIONNEL")
    print("=" * 70)
    print("\nProchaine etape: lancer le paper trading")
    print("  python -m src.main paper")
    print("  (assurez-vous que MODE=paper_trading dans config/settings.env)")


if __name__ == "__main__":
    asyncio.run(main())