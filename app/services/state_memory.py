"""
Operaciones puras sobre el dict de estado en memoria.

Este modulo espeja funciones de reademail.py (lineas ~361-424):
today_yyyymmdd, state_get_processed_set, state_add_processed,
state_has_replied, state_mark_replied.

No hace I/O: no lee ni escribe archivos. Todas las funciones que
mutan estado lo hacen in-place sobre el dict recibido, igual que en
reademail.py. Todavia no esta conectado a reademail.py.
"""

from datetime import datetime
from typing import Dict, Set

PROCESSED_CACHE_LIMIT_DEFAULT = 3000


def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def state_get_processed_set(state: Dict) -> Set[str]:
    arr = state.get("processed_message_ids") or []
    return {str(x) for x in arr if x is not None}


def state_add_processed(
    state: Dict,
    message_id: str,
    processed_cache_limit: int = PROCESSED_CACHE_LIMIT_DEFAULT,
) -> None:
    arr = state.get("processed_message_ids") or []
    if not isinstance(arr, list):
        arr = []
    mid = str(message_id)
    if mid not in arr:
        arr.append(mid)
    if len(arr) > processed_cache_limit:
        arr = arr[-processed_cache_limit:]
    state["processed_message_ids"] = arr


def state_has_replied(state: Dict, message_id: str) -> bool:
    arr = state.get("replied_message_ids") or []
    return str(message_id) in {str(x) for x in arr}


def state_mark_replied(
    state: Dict,
    message_id: str,
    processed_cache_limit: int = PROCESSED_CACHE_LIMIT_DEFAULT,
) -> None:
    arr = state.get("replied_message_ids") or []
    if not isinstance(arr, list):
        arr = []
    mid = str(message_id)
    if mid not in arr:
        arr.append(mid)
    if len(arr) > processed_cache_limit:
        arr = arr[-processed_cache_limit:]
    state["replied_message_ids"] = arr
