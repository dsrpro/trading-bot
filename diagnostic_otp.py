"""Diagnostic complet du flux OTP - teste chaque etape separement."""
import asyncio
import sys
import json
import ssl
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import load_config


def test_http_get(url, token, app_id, label):
    """Test simple HTTP GET avec le token."""
    print(f"\n[{label}] GET {url}")
    try:
        req = Request(url, headers={
            "Deriv-App-ID": app_id,
            "Authorization": f"Bearer {token}",
        })
        ctx = ssl.create_default_context()
        with urlopen(req, context=ctx, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            print(f"  Status: {resp.status}")
            print(f"  Response: {body[:500]}")
            return resp.status, body
    except HTTPError as e:
        print(f"  HTTP Error: {e.code} {e.reason}")
        body = e.read().decode("utf-8") if e.fp else ""
        print(f"  Body: {body[:300]}")
        return e.code, body
    except Exception as e:
        print(f"  Error: {e}")
        return None, str(e)


def test_http_post(url, token, app_id, label, body_data=None):
    """Test simple HTTP POST avec le token."""
    print(f"\n[{label}] POST {url}")
    try:
        data = json.dumps(body_data or {}).encode("utf-8")
        req = Request(url, data=data, headers={
            "Content-Type": "application/json",
            "Deriv-App-ID": app_id,
            "Authorization": f"Bearer {token}",
        }, method="POST")
        ctx = ssl.create_default_context()
        with urlopen(req, context=ctx, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            print(f"  Status: {resp.status}")
            print(f"  Response: {body[:500]}")
            return resp.status, body
    except HTTPError as e:
        print(f"  HTTP Error: {e.code} {e.reason}")
        body = e.read().decode("utf-8") if e.fp else ""
        print(f"  Body: {body[:300]}")
        return e.code, body
    except Exception as e:
        print(f"  Error: {e}")
        return None, str(e)


def main():
    config = load_config()

    token = config.deriv_token
    app_id = config.deriv_app_id
    account_id = "41207424"  # Ton login ID confirmé

    print("=" * 60)
    print("  DIAGNOSTIC OTP — TEST DE TOUS LES ENDPOINTS")
    print("=" * 60)
    print(f"  Token  : {token[:30]}...")
    print(f"  App ID : {app_id}")
    print(f"  Account: {account_id}")
    print()

    # Test 1: GET /accounts (liste des comptes)
    test_http_get(
        "https://api.derivws.com/trading/v1/options/accounts",
        token, app_id,
        "GET /accounts"
    )

    # Test 2: POST /accounts/{id}/otp (différentes variantes d'URL)
    urls_to_try = [
        f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp",
        f"https://api.deriv.com/trading/v1/options/accounts/{account_id}/otp",
        f"https://ws.derivws.com/trading/v1/options/accounts/{account_id}/otp",
    ]

    for url in urls_to_try:
        test_http_post(url, token, app_id, f"OTP {url.split('/')[-3]}://{url.split('/')[2]}")

    # Test 3: Essayer avec des headers alternatifs
    print("\n" + "=" * 60)
    print("  TEST HEADERS ALTERNATIFS")
    print("=" * 60)

    otp_url = f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp"

    # Variante: token dans le body
    print(f"\n[POST avec token dans le body] {otp_url}")
    try:
        data = json.dumps({"token": token}).encode("utf-8")
        req = Request(otp_url, data=data, headers={
            "Content-Type": "application/json",
            "Deriv-App-ID": app_id,
        }, method="POST")
        ctx = ssl.create_default_context()
        with urlopen(req, context=ctx, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            print(f"  Status: {resp.status}")
            print(f"  Response: {body[:500]}")
    except HTTPError as e:
        print(f"  HTTP Error: {e.code} {e.reason}")
        body = e.read().decode("utf-8") if e.fp else ""
        print(f"  Body: {body[:300]}")
    except Exception as e:
        print(f"  Error: {e}")

    # Variante: token dans le body avec clé 'api_token'
    print(f"\n[POST avec api_token dans le body] {otp_url}")
    try:
        data = json.dumps({"api_token": token}).encode("utf-8")
        req = Request(otp_url, data=data, headers={
            "Content-Type": "application/json",
            "Deriv-App-ID": app_id,
        }, method="POST")
        ctx = ssl.create_default_context()
        with urlopen(req, context=ctx, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            print(f"  Status: {resp.status}")
            print(f"  Response: {body[:500]}")
    except HTTPError as e:
        print(f"  HTTP Error: {e.code} {e.reason}")
        body = e.read().decode("utf-8") if e.fp else ""
        print(f"  Body: {body[:300]}")
    except Exception as e:
        print(f"  Error: {e}")

    # Test 4: Vérifier si l'App ID 1089 est encore valide
    print(f"\n[Test sans token - WebSocket public]")
    print("wss://api.derivws.com/trading/v1/options/ws/public?app_id=1089")
    print("→ Ceci a déjà été validé (89 symboles disponibles)")

    print("\n" + "=" * 60)
    print("  CONCLUSION")
    print("=" * 60)
    print("""
Si TOUTES les requetes HTTP retournent 401:
  → Le token est valide dans le dashboard MAIS pas pour ces endpoints API
  → Solution: Créer un NOUVEAU token avec Admin scope complet
  → Aller sur https://app.deriv.com/account/api-token
  → SUPPRIMER le token actuel
  → Créer un NOUVEAU token (tous les scopes cochés)
  → Le coller dans config/settings.env

Si certaines requetes passent:
  → L'URL qui fonctionne est la bonne
  → Je mettrai à jour deriv_client.py avec cette URL
""")


if __name__ == "__main__":
    main()