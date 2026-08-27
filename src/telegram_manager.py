"""Gestion Telegram : alertes sortantes + commandes distantes (polling).

Implemente uniquement avec la bibliotheque standard (urllib) pour ne pas
ajouter de dependance tierce. Deux rôles :

    1. Alertes sortantes : send_message() pour notifier les evenements
       (ouverture/cloture de trade, objectif atteint, erreurs, etc.).
    2. Commandes entrantes : polling long `getUpdates` qui repond aux
       commandes /status, /report, /kill, /resume, /stop, /help.

Configuration requise dans settings.env :
    TELEGRAM_BOT_TOKEN=123456:ABC...   (obtenu via @BotFather)
    TELEGRAM_CHAT_ID=123456789         (votre identifiant de discussion)
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import ssl
import time
from typing import Callable, Optional
import os
from urllib.request import Request, urlopen

from src.config import Config

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/{method}"


class TelegramManager:
    """Client Telegram minimaliste (alertes + commandes distantes)."""

    # Descriptions affichees dans le menu "/" du chat via setMyCommands.
    COMMAND_DESCRIPTIONS = {
        "/help": "Liste des commandes",
        "/start": "Demarrer le bot",
        "/run": "Demarrer sur un ou plusieurs symboles",
        "/markets": "Lister les marchés disponibles",
        "/status": "Etat actuel du bot",
        "/report": "Rapport de risque",
        "/kill": "Arret d'urgence (kill switch)",
        "/resume": "Desactive le kill switch",
        "/stop": "Arret propre du bot",
        "/choose": "Selectionner par index",
        "/reset": "Reinitialiser les compteurs du jour",
    }

    def __init__(
        self,
        config: Config,
        logger: Optional[logging.Logger] = None,
        chat_id: Optional[str] = None,
    ):
        self.config = config
        self.logger = logger or logging.getLogger("telegram_manager")
        self.token = config.telegram_bot_token
        self.chat_id = chat_id or config.telegram_chat_id

        # Respecter la variable d'environnement TELEGRAM_DISABLE
        if os.environ.get("TELEGRAM_DISABLE") == "1":
            self._enabled = False
        else:
            self._enabled = bool(self.token and self.chat_id)
        self._offset: Optional[int] = None
        self._running = False
        self._poll_task: Optional[asyncio.Task] = None
        self._command_handlers: dict[str, Callable] = {}
        self._startup_ts = int(time.time())
        # Count consecutive 409 Conflict responses from getUpdates
        self._conflict_count: int = 0
        self._poll_error_count: int = 0

    @property
    def enabled(self) -> bool:
        return self._enabled

    def register_command(self, command: str, handler: Callable) -> None:
        """Enregistre un gestionnaire de commande.

        Le gestionnaire peut etre synchrone ou async. S'il retourne une
        chaine, celle-ci est renvoyee par message Telegram.
        """
        self._command_handlers[command.lower()] = handler

    async def sync_bot_commands(self) -> bool:
        """Declare les commandes aupres de Telegram (setMyCommands).

        C'est ce qui rend les commandes visibles dans le menu "/" du chat.
        Sans cet appel, Telegram n'affiche aucune commande meme si le bot
        sait y repondre.
        """
        if not self._enabled:
            self.logger.debug("Telegram desactive — setMyCommands ignore")
            return False

        commands = []
        for cmd in self._command_handlers:
            name = cmd.lstrip("/").split("@")[0]
            description = self.COMMAND_DESCRIPTIONS.get(cmd, "")
            commands.append({"command": name, "description": description})

        if not commands:
            return False

        loop = asyncio.get_event_loop()
        # Log the payload we will send so we can debug live discrepancies
        try:
            self.logger.debug(f"setMyCommands payload: {json.dumps({'commands': commands})}")
        except Exception:
            pass
        result = await loop.run_in_executor(
            None,
            self._call_sync,
            "setMyCommands",
            {"commands": commands},
        )
        ok = bool(result and result.get("ok"))
        if ok:
            self.logger.info(
                f"Commandes Telegram enregistrees ({len(commands)}): "
                f"{', '.join(cmd.lstrip('/') for cmd in self._command_handlers)}"
            )
        else:
            self.logger.warning(
                f"Enregistrement des commandes Telegram echoue: "
                f"{json.dumps(result)[:200] if result else 'aucune reponse'}"
            )
            # If there is a detailed body, log it at debug for troubleshooting
            try:
                self.logger.debug(f"setMyCommands response full: {json.dumps(result, ensure_ascii=False)}")
            except Exception:
                pass
        return ok

    # ── Appels HTTP (bloquants, executes dans un thread) ─────────────

    def _call_sync(self, method: str, params: dict) -> Optional[dict]:
        url = TELEGRAM_API_BASE.format(token=self.token, method=method)
        body = json.dumps(params).encode("utf-8")
        req = Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            ctx = ssl.create_default_context()
            timeout = max(1.0, float(getattr(self.config, "telegram_request_timeout", 10.0)))
            with urlopen(req, context=ctx, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            # Try to capture HTTPError body if available and return parsed JSON
            try:
                body_reader = getattr(e, 'read', None)
                if callable(body_reader):
                    raw = e.read()
                    try:
                        text = raw.decode('utf-8')
                    except Exception:
                        text = str(raw)
                    try:
                        parsed = json.loads(text)
                        # Log and return the parsed error JSON for callers to react
                        self.logger.error(f"Telegram {method} HTTP error parsed: {parsed}")
                        return parsed
                    except Exception:
                        self.logger.error(f"Telegram {method} error: {e} -- body: {text}")
                        return None
            except Exception:
                pass

            self.logger.error(f"Telegram {method} error: {e}")
            return None

    async def send_message(self, text: str) -> bool:
        """Envoie un message Telegram (no-op si Telegram n'est pas configure)."""
        if not self._enabled:
            self.logger.debug("Telegram desactive — message non envoye")
            return False
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self._call_sync,
            "sendMessage",
            {"chat_id": self.chat_id, "text": text},
        )
        if result is not None and result.get("ok"):
            return True
        self.logger.warning(
            f"Envoi Telegram echoue: "
            f"{json.dumps(result)[:200] if result else 'aucune reponse'}"
        )
        return False

    # ── Polling des commandes ────────────────────────────────────────

    async def _poll_once(self) -> list[dict]:
        params: dict = {"timeout": 1, "allowed_updates": json.dumps(["message"]) }
        if self._offset is not None:
            params["offset"] = self._offset

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._call_sync, "getUpdates", params)

        # Handle Telegram HTTP-level errors returned as parsed JSON (e.g. 409 Conflict)
        if result and not result.get("ok") and result.get("error_code") == 409:
            # Consecutive conflict counter
            self._conflict_count += 1
            self._poll_error_count = 0
            max_backoff = getattr(self.config, 'telegram_backoff_max', 60)
            backoff = min(max_backoff, 2 ** self._conflict_count)
            self.logger.warning(
                f"Telegram getUpdates conflict (409) #{self._conflict_count}: {result.get('description')}; backoff={backoff}s"
            )
            # After N consecutive conflicts, alert via Telegram and stop polling to avoid spam
            threshold = getattr(self.config, 'telegram_conflict_threshold', 3)
            if self._conflict_count >= threshold:
                try:
                    await self.send_message(
                        "⚠️ Polling interrompu: plusieurs erreurs 409 (conflict). "
                        "Assurez-vous qu'aucune autre instance ne sonde ce bot (getUpdates) et relancez."
                    )
                except Exception:
                    self.logger.exception("Impossible d'envoyer l'alerte Telegram sur conflit 409")
                # Stop polling to avoid repeated noise
                self._running = False
                return []

            await asyncio.sleep(backoff)
            return []

        # Reset conflict counter on successful non-409 response
        if result and result.get("ok"):
            self._conflict_count = 0
            self._poll_error_count = 0

        if not result or not result.get("ok"):
            self._poll_error_count += 1
            max_backoff = getattr(self.config, 'telegram_backoff_max', 60)
            backoff = min(max_backoff, 2 ** min(self._poll_error_count, 6))
            self.logger.warning(
                f"Telegram getUpdates indisponible "
                f"(erreur reseau #{self._poll_error_count}); retry dans {backoff}s"
            )
            await asyncio.sleep(backoff)
            return []

        updates = result.get("result", [])
        for update in updates:
            update_id = update.get("update_id")
            if update_id is not None:
                self._offset = update_id + 1
        return updates

    async def poll_forever(self) -> None:
        """Boucle de polling longue. S'arrete via stop()."""
        self._running = True
        self.logger.info("Polling Telegram demarre")
        while self._running:
            try:
                updates = await self._poll_once()
                await self._handle_updates(updates)
            except Exception as e:
                self.logger.error(f"Erreur polling Telegram: {e}")
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(0.2)
        self.logger.info("Polling Telegram arrete")

    def start_polling(self) -> Optional[asyncio.Task]:
        """Demarre la boucle de polling en tache de fond et la retourne."""
        if not self._enabled:
            self.logger.warning("Telegram non configure — polling desactive")
            return None
        if self._poll_task is not None and not self._poll_task.done():
            return self._poll_task
        self._poll_task = asyncio.create_task(self.poll_forever())
        return self._poll_task

    def stop(self) -> None:
        """Arrete le polling."""
        self._running = False

    async def _handle_updates(self, updates: list[dict]) -> None:
        for update in updates:
            message = update.get("message") or update.get("edited_message")
            if not isinstance(message, dict):
                continue

            text = (message.get("text") or "").strip()
            if not text:
                continue

            # Ignore les messages Telegram déjà en file avant le démarrage
            # actuel du bot (ex. anciens /stop, /resume ou /run traités lors
            # d'un précédent lancement et encore présents dans la file).
            msg_date = message.get("date")
            if isinstance(msg_date, (int, float)) and int(msg_date) < self._startup_ts:
                self.logger.debug(
                    "Ignorer message Telegram stale: date=%s startup=%s text=%s",
                    msg_date,
                    self._startup_ts,
                    text,
                )
                continue

            first_token = text.split()[0].split("@")[0].lower()
            command = first_token if first_token.startswith("/") else f"/{first_token}"

            # Accepter les commandes avec ou sans slash, par exemple
            # "/run 1,2,3" ou "Run 1,2,3". Les messages ordinaires ne
            # sont pas traites, sauf si leur premier mot correspond a une
            # commande connue.
            if not first_token.startswith("/"):
                match = False
                for registered in self._command_handlers:
                    if registered.lower().lstrip("/") == first_token:
                        match = True
                        command = registered.lower()
                        break
                if not match:
                    continue

            # Securite : ne repondre qu'au chat autorise
            chat = message.get("chat", {}) if isinstance(message.get("chat"), dict) else {}
            incoming_chat = str(chat.get("id", ""))
            if incoming_chat != str(self.chat_id):
                self.logger.debug(f"Ignorer message d'un chat non autorise: chat_id={incoming_chat} attendu={self.chat_id} text={text}")
                continue

            handler = self._command_handlers.get(command)
            if handler is None:
                self.logger.debug(f"Commande non reconnue recu: {command} handlers={list(self._command_handlers.keys())}")
                await self.send_message(
                    "Commande inconnue. Envoyez /help pour la liste des commandes."
                )
                continue

            try:
                # Appeler le handler en tentant de lui passer le texte
                # s'il accepte au moins un parametre. Ceci permet d'avoir
                # des handlers du type `def handler(text: str)` pour parser
                # des arguments (ex: "/run EURUSD,1HZ75V"). Si le handler
                # n'accepte pas d'argument, on l'appelle sans parametre.
                try:
                    params_count = len(inspect.signature(handler).parameters)
                except Exception:
                    params_count = 0

                if params_count == 0:
                    result = handler()
                else:
                    result = handler(text)

                if asyncio.iscoroutine(result):
                    result = await result
                if isinstance(result, str) and result:
                    await self.send_message(result)
            except Exception as e:
                self.logger.error(f"Erreur commande {command}: {e}", exc_info=True)
                await self.send_message(f"Erreur lors de l'execution de {command}: {e}")
