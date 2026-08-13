"""Client WebSocket asynchrone pour l'API Deriv v2.

Utilise les nouveaux endpoints:
    - Public (donnees de marche): wss://api.derivws.com/trading/v1/options/ws/public
    - Trading (apres OTP): POST HTTP puis WebSocket dynamique

Flux OTP complet:
    1. POST https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp
       Headers: Deriv-App-ID, Authorization: Bearer <token>
    2. Reponse: {"websocket_url": "wss://...", ...}
    3. Connexion WebSocket a l'URL dynamique
    4. Trading authentifie pret
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from typing import Any, Callable, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from src.config import Config


class DerivClient:
    """Client WebSocket pour l'API Deriv (nouvelle version).

    Responsabilites:
        - Connexion WebSocket securisee (wss)
        - Authentification via OTP (trading uniquement)
        - Streaming temps reel des ticks (endpoint public, sans auth)
        - Reconnexion automatique avec backoff exponentiel
    """

    # Nouveaux endpoints Deriv
    PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
    OTP_API_URL = "https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp"

    def __init__(self, config: Config, logger: Optional[logging.Logger] = None):
        self.config = config
        self.logger = logger or logging.getLogger("deriv_client")
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._req_id = 0
        self._connected = False
        self._running = False
        self._tick_callbacks: list[Callable[[dict], None]] = []
        self._message_callbacks: list[Callable[[dict], None]] = []
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._mode: str = "public"  # "public" ou "trading"

    @property
    def is_connected(self) -> bool:
        return self._connected

    def on_tick(self, callback: Callable[[dict], None]) -> None:
        """Enregistre un callback appele a chaque tick recu."""
        self._tick_callbacks.append(callback)

    def on_message(self, callback: Callable[[dict], None]) -> None:
        """Enregistre un callback appele pour chaque message recu."""
        self._message_callbacks.append(callback)

    async def connect(self) -> bool:
        """Etablit la connexion WebSocket (endpoint public).

        Pour les donnees de marche publiques, aucune authentification n'est necessaire.

        Returns:
            True si la connexion est reussie, False sinon.
        """
        self._running = True
        self._mode = "public"
        attempt = 0

        while attempt < self.config.reconnect_attempts and self._running:
            attempt += 1
            try:
                # Connexion au nouvel endpoint public
                url = f"{self.PUBLIC_WS_URL}?app_id={self.config.deriv_app_id}"
                self._ws = await websockets.connect(
                    url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,
                )
                self._req_id = 0
                self._connected = True
                self.logger.info(f"WebSocket public connecte (tentative {attempt})")

                # Demarrer la boucle de lecture
                read_task = asyncio.create_task(self._read_loop())
                return True  # Connexion OK, lecture en background

            except (ConnectionClosed, WebSocketException, OSError) as e:
                self._connected = False
                self.logger.warning(f"Connexion perdue (tentative {attempt}/{self.config.reconnect_attempts}): {e}")
                if attempt < self.config.reconnect_attempts:
                    delay = self.config.reconnect_delay_seconds * (2 ** (attempt - 1))
                    self.logger.info(f"Reconnexion dans {delay:.1f}s...")
                    await asyncio.sleep(delay)
            except Exception as e:
                self._connected = False
                self.logger.error(f"Erreur inattendue: {e}", exc_info=True)
                break

        self._connected = False
        self.logger.error("Impossible de se connecter apres toutes les tentatives")
        return False

    async def disconnect(self) -> None:
        """Ferme proprement la connexion WebSocket."""
        self._running = False
        self._connected = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        for future in self._pending_requests.values():
            if not future.done():
                future.set_exception(ConnectionError("Deconnecte"))
        self._pending_requests.clear()

    async def reconnect(self) -> bool:
        """Tente de reconnecter le client si la connexion est perdue."""
        self.logger.info("Reconnexion Deriv demandee")
        await self.disconnect()
        return await self.connect()

    # ── Trading Authentifié (OTP) ──────────────────────────────────

    def _request_otp_sync(self, token: str, account_id: str) -> Optional[dict]:
        """Etape 1: Requete HTTP POST pour obtenir l'URL WebSocket de trading.

        Appele de maniere synchrone (execute dans un thread separe par asyncio).

        Args:
            token: Token API Deriv (PAT).
            account_id: ID du compte Deriv (ex: "CR90000000").

        Returns:
            Dictionnaire avec 'websocket_url' ou None si echec.
        """
        url = self.OTP_API_URL.format(account_id=account_id)
        data = json.dumps({}).encode("utf-8")

        req = Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Deriv-App-ID": self.config.deriv_app_id,
                "Authorization": f"Bearer {token}",
            },
            method="POST",
        )

        try:
            ctx = ssl.create_default_context()
            with urlopen(req, context=ctx, timeout=15) as response:
                body = json.loads(response.read().decode("utf-8"))
                self.logger.info(f"OTP response recue: {json.dumps(body)[:300]}")
                return body
        except HTTPError as e:
            self.logger.error(f"OTP HTTP error {e.code}: {e.reason}")
            try:
                error_body = json.loads(e.read().decode("utf-8"))
                self.logger.error(f"OTP error body: {json.dumps(error_body)[:300]}")
            except Exception:
                pass
            return None
        except URLError as e:
            self.logger.error(f"OTP URL error: {e.reason}")
            return None
        except Exception as e:
            self.logger.error(f"OTP unexpected error: {e}")
            return None

    async def connect_trading(self, token: str, account_id: str) -> bool:
        """Etablit une connexion WebSocket de trading via OTP.

        Flux complet:
            1. POST HTTP → obtient websocket_url
            2. Connexion WebSocket a l'URL dynamique
            3. Pret pour get_balance, get_proposal, buy_contract

        Args:
            token: Token API Deriv (PAT).
            account_id: ID du compte Deriv (trouvable dans le dashboard).

        Returns:
            True si la connexion trading est etablie, False sinon.
        """
        self._running = True
        self._mode = "trading"

        # Étape 1: Requête OTP (exécutée dans un thread pour ne pas bloquer)
        self.logger.info(f"Etape 1/2: Requete OTP pour le compte {account_id}...")
        loop = asyncio.get_event_loop()
        otp_response = await loop.run_in_executor(
            None, self._request_otp_sync, token, account_id
        )

        if otp_response is None:
            self.logger.error("Echec de la requete OTP — verifiez le token et l'account_id")
            return False

        if otp_response.get("error"):
            err = otp_response["error"]
            self.logger.error(f"OTP erreur: {err.get('code', '')} - {err.get('message', '')}")
            return False

        # Nouvelle API v2: la reponse a la forme {"data": {"url": "wss://..."}}
        websocket_url = otp_response.get("websocket_url")
        if not websocket_url:
            data = otp_response.get("data", {})
            websocket_url = data.get("url") if isinstance(data, dict) else None
        if not websocket_url:
            self.logger.error(f"OTP reponse sans websocket_url: {json.dumps(otp_response)[:300]}")
            return False

        # Étape 2: Connexion WebSocket à l'URL dynamique
        self.logger.info(f"Etape 2/2: Connexion WebSocket trading...")
        attempt = 0
        while attempt < self.config.reconnect_attempts and self._running:
            attempt += 1
            try:
                self._ws = await websockets.connect(
                    websocket_url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,
                )
                self._req_id = 0
                self._connected = True
                self._mode = "trading"
                self.logger.info(f"WebSocket trading connecte! (tentative {attempt})")

                # Lancer la boucle de lecture
                asyncio.create_task(self._read_loop())
                return True

            except (ConnectionClosed, WebSocketException, OSError) as e:
                self._connected = False
                self.logger.warning(f"Connexion trading perdue (tentative {attempt}): {e}")
                if attempt < self.config.reconnect_attempts:
                    delay = self.config.reconnect_delay_seconds * (2 ** (attempt - 1))
                    self.logger.info(f"Reconnexion dans {delay:.1f}s...")
                    await asyncio.sleep(delay)
            except Exception as e:
                self._connected = False
                self.logger.error(f"Erreur connexion trading: {e}", exc_info=True)
                break

        self._connected = False
        self.logger.error("Impossible de se connecter au trading apres toutes les tentatives")
        return False

    async def subscribe_ticks(self, symbol: str) -> Optional[dict]:
        """Souscrit au flux de ticks pour un symbole (API Deriv v2).

        Utilise le format correct pour le nouvel endpoint public:
        {"ticks_history": "1HZ100V", "end": "latest", "subscribe": 1}

        Args:
            symbol: Symbole du marche (ex: "1HZ100V").

        Returns:
            Reponse de l'API ou None si deconnecte.
        """
        payload = {"ticks_history": symbol, "end": "latest", "subscribe": 1}
        return await self._send_request(payload)

    async def get_active_symbols(self) -> Optional[dict]:
        """Recupere la liste des symboles actifs.

        Returns:
            Reponse de l'API contenant la liste des symboles.
        """
        return await self._send_request({"active_symbols": "full"})

    async def get_candles(
        self,
        symbol: str,
        *,
        count: int = 500,
        granularity: int = 60,
        end: str = "latest",
    ) -> Optional[dict]:
        """Recupere les bougies OHLC historiques (API Deriv v2).

        Args:
            symbol: Symbole du marche (ex: "1HZ100V").
            count: Nombre de bougies a recuperer.
            granularity: Taille des bougies en secondes (60 = M1).
            end: Borne de fin ("latest" ou epoch).

        Returns:
            Reponse contenant la cle "candles", ou None si echec.
        """
        return await self._send_request(
            {
                "ticks_history": symbol,
                "style": "candles",
                "granularity": granularity,
                "count": count,
                "end": end,
            },
            timeout=30.0,
        )

    async def unsubscribe_ticks(self, symbol: str) -> Optional[dict]:
        """Se desabonne du flux de ticks."""
        return await self._send_request({"forget_all": "ticks"})

    async def get_balance(self) -> Optional[dict]:
        """Recupere le solde du compte (necessite auth trading)."""
        if self._mode != "trading":
            self.logger.warning("get_balance necessite le mode trading (OTP auth)")
            return None
        return await self._send_request({"balance": 1})

    async def get_proposal(self, contract_type: str, symbol: str, amount: float,
                           basis: str = "stake", duration: int = 1,
                           duration_unit: str = "t") -> Optional[dict]:
        """Obtient une proposition de contrat (necessite auth trading)."""
        if self._mode != "trading":
            self.logger.warning("get_proposal necessite le mode trading")
            return None
        return await self._send_request({
            "proposal": 1,
            "contract_type": contract_type,
            "underlying_symbol": symbol,
            "amount": amount,
            "basis": basis,
            "duration": duration,
            "duration_unit": duration_unit,
            "currency": "USD",
        })

    async def buy_contract(self, proposal_id: str, price: float) -> Optional[dict]:
        """Execute l'achat d'un contrat."""
        return await self._send_request({"buy": proposal_id, "price": price})

    def _next_req_id(self) -> int:
        self._req_id += 1
        return self._req_id

    async def _send_request(self, data: dict, timeout: float = 10.0) -> Optional[dict]:
        """Envoie une requete et attend la reponse.

        Args:
            data: Donnees de la requete (ajout automatique du req_id).
            timeout: Timeout en secondes.

        Returns:
            Dictionnaire de reponse ou None si timeout/erreur.
        """
        if not self._ws or not self._connected:
            self.logger.warning("Tentative d'envoi alors que deconnecte")
            return None

        req_id = self._next_req_id()
        data["req_id"] = req_id
        future: asyncio.Future[dict] = asyncio.get_event_loop().create_future()
        self._pending_requests[str(req_id)] = future

        try:
            payload = json.dumps(data)
            await self._ws.send(payload)
            self.logger.debug(f">> [req_id={req_id}] {json.dumps(data, default=str)[:200]}")
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout requete [req_id={req_id}]")
            return None
        except Exception as e:
            self.logger.error(f"Erreur envoi requete [req_id={req_id}]: {e}")
            return None
        finally:
            self._pending_requests.pop(str(req_id), None)

    async def _read_loop(self) -> None:
        """Boucle principale de lecture des messages WebSocket."""
        try:
            async for raw in self._ws:
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    self.logger.warning(f"Message non-JSON recu: {raw[:200]}")
                    continue

                self.logger.debug(f"<< {json.dumps(message, default=str)[:300]}")

                # Distribuer aux callbacks generiques
                for cb in self._message_callbacks:
                    try:
                        cb(message)
                    except Exception as e:
                        self.logger.error(f"Erreur callback message: {e}")

                # Associer la reponse a une requete en attente
                req_id = message.get("req_id")
                if req_id is not None:
                    future = self._pending_requests.get(str(req_id))
                    if future and not future.done():
                        future.set_result(message)

                # Traiter les ticks (format standard: {"tick": {...}})
                # Un seul dispatch: le champ "tick" est present sur tous les messages
                # de tick (y compris les updates msg_type="tick"), ce qui evite le
                # double appel du meme callback.
                tick = message.get("tick")
                if tick is not None:
                    for cb in self._tick_callbacks:
                        try:
                            cb(tick)
                        except Exception as e:
                            self.logger.error(f"Erreur callback tick: {e}")

                # Log des erreurs API
                if "error" in message:
                    self.logger.warning(
                        f"Erreur API: {message['error'].get('code', '')} - "
                        f"{message['error'].get('message', '')}"
                    )

        except ConnectionClosed as e:
            self._connected = False
            self.logger.info(f"Connexion fermee par le serveur: {e}")
        except Exception as e:
            self._connected = False
            self.logger.error(f"Erreur boucle de lecture: {e}", exc_info=True)