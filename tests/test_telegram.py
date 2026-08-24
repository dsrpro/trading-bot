"""Tests unitaires du module TelegramManager (sans appel reseau reel)."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import Config
from src.telegram_manager import TelegramManager
from paper_trading_live import PaperTradingLive


def _config_with_telegram() -> Config:
    return Config(
        telegram_bot_token="123456:TESTTOKEN",
        telegram_chat_id="987654321",
    )


class TestTelegramManager:
    def test_disabled_when_no_config(self):
        cfg = Config()  # token et chat_id vides
        mgr = TelegramManager(cfg)
        assert not mgr.enabled

    def test_enabled_when_configured(self):
        mgr = TelegramManager(_config_with_telegram())
        assert mgr.enabled

    def test_register_and_unknown_command(self):
        mgr = TelegramManager(_config_with_telegram())
        sent = []

        async def fake_send(text: str) -> bool:
            sent.append(text)
            return True

        mgr._handle_updates = None  # on teste uniquement le dispatch ci-dessous
        # On teste la liste des commandes enregistrees
        mgr.register_command("/status", lambda: "OK")
        assert "/status" in mgr._command_handlers
        assert "/help" not in mgr._command_handlers


class TestTelegramHandleUpdates:
    @pytest.mark.asyncio
    async def test_dispatches_registered_command_and_filters_chat(self):
        mgr = TelegramManager(_config_with_telegram())
        replies = []

        async def fake_send(text: str) -> bool:
            replies.append(text)
            return True

        mgr.send_message = fake_send
        mgr.register_command("/status", lambda: "ETAT_OK")

        updates = [
            {
                "update_id": 1,
                "message": {
                    "chat": {"id": "987654321"},
                    "text": "/status",
                },
            },
            # Message d'un autre chat : doit etre ignore
            {
                "update_id": 2,
                "message": {
                    "chat": {"id": "111111111"},
                    "text": "/status",
                },
            },
            # Message vide : ignore
            {
                "update_id": 3,
                "message": {
                    "chat": {"id": "987654321"},
                    "text": "bonjour",
                },
            },
        ]

        await mgr._handle_updates(updates)

        # Une seule reponse : celle du bon chat
        assert replies == ["ETAT_OK"]

    @pytest.mark.asyncio
    async def test_unknown_command_replies_help(self):
        mgr = TelegramManager(_config_with_telegram())
        replies = []

        async def fake_send(text: str) -> bool:
            replies.append(text)
            return True

        mgr.send_message = fake_send

        updates = [
            {
                "update_id": 10,
                "message": {
                    "chat": {"id": "987654321"},
                    "text": "/inconnue",
                },
            },
        ]
        await mgr._handle_updates(updates)
        assert len(replies) == 1
        assert "Commande inconnue" in replies[0]

    @pytest.mark.asyncio
    async def test_command_with_bot_mention(self):
        mgr = TelegramManager(_config_with_telegram())
        replies = []

        async def fake_send(text: str) -> bool:
            replies.append(text)
            return True

        mgr.send_message = fake_send
        mgr.register_command("/status", lambda: "ETAT_OK")

        updates = [
            {
                "update_id": 20,
                "message": {
                    "chat": {"id": "987654321"},
                    "text": "/status@MonBot",
                },
            },
        ]
        await mgr._handle_updates(updates)
        assert replies == ["ETAT_OK"]

    @pytest.mark.asyncio
    async def test_command_without_leading_slash_is_accepted(self):
        mgr = TelegramManager(_config_with_telegram())
        replies = []

        async def fake_send(text: str) -> bool:
            replies.append(text)
            return True

        mgr.send_message = fake_send
        mgr.register_command("/run", lambda text: f"RUN:{text}")

        updates = [
            {
                "update_id": 30,
                "message": {
                    "chat": {"id": "987654321"},
                    "text": "Run 6, 9,7,8,11,17",
                },
            },
        ]
        await mgr._handle_updates(updates)
        assert replies == ["RUN:Run 6, 9,7,8,11,17"]

    @pytest.mark.asyncio
    async def test_stale_telegram_messages_are_ignored_on_startup(self):
        mgr = TelegramManager(_config_with_telegram())
        replies = []

        async def fake_send(text: str) -> bool:
            replies.append(text)
            return True

        mgr.send_message = fake_send
        mgr.register_command("/stop", lambda: "STOPPED")

        old_ts = 1700000000
        updates = [
            {
                "update_id": 50,
                "message": {
                    "chat": {"id": "987654321"},
                    "date": old_ts,
                    "text": "/stop",
                },
            },
        ]

        await mgr._handle_updates(updates)
        assert replies == []


class TestBotPauseResume:
    def test_stop_then_resume_keeps_process_running(self):
        cfg = Config(telegram_bot_token="123456:TESTTOKEN", telegram_chat_id="987654321")
        engine = PaperTradingLive(cfg, "STPRNG")

        assert engine._running is False
        assert engine._paused is False

        engine._tg_stop()
        assert engine._running is True
        assert engine._paused is True

        engine._tg_resume()
        assert engine._running is True
        assert engine._paused is False


class TestTelegramSendMessage:
    @pytest.mark.asyncio
    async def test_send_message_noop_when_disabled(self):
        mgr = TelegramManager(Config())
        assert await mgr.send_message("x") is False

    @pytest.mark.asyncio
    async def test_send_message_success(self):
        mgr = TelegramManager(_config_with_telegram())
        calls = []

        def fake_call_sync(method, params):
            calls.append((method, params))
            return {"ok": True, "result": {}}

        mgr._call_sync = fake_call_sync
        assert await mgr.send_message("bonjour") is True
        assert calls[0][0] == "sendMessage"
        assert calls[0][1]["chat_id"] == "987654321"
        assert calls[0][1]["text"] == "bonjour"

    @pytest.mark.asyncio
    async def test_send_message_failure(self):
        mgr = TelegramManager(_config_with_telegram())

        def fake_call_sync(method, params):
            return {"ok": False, "description": "Forbidden"}

        mgr._call_sync = fake_call_sync
        assert await mgr.send_message("x") is False