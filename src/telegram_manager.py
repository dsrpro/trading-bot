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
import json
import logging
import ssl
from typing import Callable, Optional
from urllib.request import Request, urlopen

from src.config import Config

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/{method}"


class TelegramManager:
    """Client Telegram minimaliste (alertes + commandes distantes)."""

    # Descriptions affichees dans le menu "/" du chat via setMyCommands.
    COMMAND_DESCRIPTIONS = {
        "/help": "Liste des commandes",
        "/start": "Demarrer le bot",
        "/status": "Etat actuel du bot",
        "/report": "Rapport de risque",
        "/kill": "Arret d'urgence (kill switch)",
        "/resume": "Desactive le kill switch",
        "/stop": "Arret propre du bot",
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

        self._enabled = bool(self.token and self.chat_id)
        self._offset: Optional[int] = None
        self._running = False
        self._poll_task: Optional[asyncio.Task] = None
        self._command_handlers: dict[str, Callable] = {}

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
            with urlopen(req, context=ctx, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
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
        params: dict = {"timeout": 1, "allowed_updates": json.dumps(["message"])}
        if self._offset is not None:
            params["offset"] = self._offset

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._call_sync, "getUpdates", params)
        if not result or not result.get("ok"):
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
            if not text.startswith("/"):
                continue

            # Securite : ne repondre qu'au chat autorise
            chat = message.get("chat", {}) if isinstance(message.get("chat"), dict) else {}
            if str(chat.get("id", "")) != str(self.chat_id):
                continue

            command = text.split()[0].split("@")[0].lower()
            handler = self._command_handlers.get(command)
            if handler is None:
                await self.send_message(
                    "Commande inconnue. Envoyez /help pour la liste des commandes."
                )
                continue

            try:
                result = handler()
                if asyncio.iscoroutine(result):
                    result = await result
                if isinstance(result, str) and result:
                    await self.send_message(result)
            except Exception as e:
                self.logger.error(f"Erreur commande {command}: {e}", exc_info=True)
                await self.send_message(f"Erreur lors de l'execution de {command}: {e}")