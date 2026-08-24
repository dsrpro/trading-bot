from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import List, Dict, Optional, Tuple

RUNTIME_DIR = Path(__file__).resolve().parent.parent / "runtime"
RUNTIME_FILE = RUNTIME_DIR / "instances.json"
EVENTS_FILE = RUNTIME_DIR / "events.jsonl"


def _ensure_dir() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def _read_all() -> List[Dict]:
    _ensure_dir()
    if not RUNTIME_FILE.exists():
        return []
    try:
        with open(RUNTIME_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _write_all(data: List[Dict]) -> None:
    _ensure_dir()
    tmp = RUNTIME_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, RUNTIME_FILE)


def register_instance(symbol: str, pid: int | None = None) -> None:
    pid = pid or os.getpid()
    now = int(time.time())
    data = _read_all()
    # remove any existing entry for this pid
    data = [d for d in data if int(d.get("pid", 0)) != int(pid)]
    data.append({
        "pid": int(pid),
        "symbol": str(symbol),
        "start_ts": now,
        # état de trading (mis à jour par update_instance)
        "positions": [],
        "pnl": 0.0,
        "win_rate": 0.0,
        "trades": 0,
        "capital": 0.0,
    })
    _write_all(data)


def unregister_instance(pid: int | None = None) -> None:
    pid = pid or os.getpid()
    data = _read_all()
    data = [d for d in data if int(d.get("pid", 0)) != int(pid)]
    _write_all(data)


def update_instance(pid: int, **fields) -> None:
    """Met à jour l'état d'une instance enregistrée (positions, pnl, etc.)."""
    data = _read_all()
    updated = False
    for d in data:
        if int(d.get("pid", 0)) == int(pid):
            d.update(fields)
            updated = True
            break
    if not updated:
        # L'instance n'existe pas encore : la créer pour ne pas perdre l'état.
        entry = {"pid": int(pid), "start_ts": int(time.time())}
        entry.update(fields)
        entry.setdefault("symbol", fields.get("symbol", "?"))
        data.append(entry)
    _write_all(data)


def list_instances() -> List[Dict]:
    """Retourne toutes les instances avec leur état courant (positions, P&L...)."""
    return _read_all()


# ── File d'événements partagée (monitoring multi-processus) ─────────
#
# Chaque instance de trading (parent ET enfants lancés via /run) écrit ses
# événements (ouverture/clôture de trade, erreurs...) dans ce fichier JSONL
# partagé. L'instance "mère" (la seule avec Telegram actif) possède un
# forwarder qui lit ce fichier et relaie les messages vers Telegram.
# Ceci permet de remonter les trades de TOUS les indices sur un seul chat,
# sans conflit de polling getUpdates (une seule boucle Telegram).

def append_event(event: dict) -> None:
    """Ajoute un événement à la file partagée.

    Args:
        event: dict avec au minimum 'type' et 'message'. On lui ajoute
               automatiquement un 'id' unique et un 'ts' (timestamp).
    """
    _ensure_dir()
    event = dict(event)
    event.setdefault("id", uuid.uuid4().hex)
    event.setdefault("ts", int(time.time()))
    try:
        with open(EVENTS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass


def read_new_events(offset: int) -> Tuple[List[Dict], int]:
    """Lit les événements depuis un offset en octets.

    Args:
        offset: position (en octets) de lecture.

    Returns:
        (evénements lus, nouvel offset). Si le fichier a été réinitialisé
        (nouvel offset > taille), on repart à 0.
    """
    _ensure_dir()
    if not EVENTS_FILE.exists():
        return [], offset
    try:
        size = EVENTS_FILE.stat().st_size
        if offset > size:
            offset = 0
        with open(EVENTS_FILE, "r", encoding="utf-8") as f:
            f.seek(offset)
            lines = f.readlines()
            new_offset = f.tell()
    except Exception:
        return [], offset

    events: List[Dict] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except Exception:
            continue
    return events, new_offset