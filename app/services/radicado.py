"""
Generacion pura de numero de radicado.

Este modulo espeja get_or_create_radicado de reademail.py
(lineas ~427-458). No hace I/O; muta el dict `state` recibido
in-place, igual que el original. Todavia no esta conectado a
reademail.py.
"""

from typing import Dict

from app.services.state_memory import today_yyyymmdd

RADICADO_PREFIX_DEFAULT = "RAD"
RADICADO_PAD_DEFAULT = 6
RADICADO_RESET_DAILY_DEFAULT = True
RADICADO_MAP_LIMIT_DEFAULT = 10000


def get_or_create_radicado(
    message_id: str,
    state: Dict,
    radicado_prefix: str = RADICADO_PREFIX_DEFAULT,
    radicado_pad: int = RADICADO_PAD_DEFAULT,
    radicado_reset_daily: bool = RADICADO_RESET_DAILY_DEFAULT,
    radicado_map_limit: int = RADICADO_MAP_LIMIT_DEFAULT,
) -> str:
    mappings = state.get("message_radicados") or {}
    if not isinstance(mappings, dict):
        mappings = {}

    mid = str(message_id)
    if mid in mappings:
        return mappings[mid]

    today = today_yyyymmdd()
    last_date = str(state.get("radicado_date") or "")
    counter = int(state.get("radicado_counter") or 0)

    if radicado_reset_daily and last_date != today:
        counter = 0

    counter += 1
    state["radicado_counter"] = counter
    state["radicado_date"] = today

    if radicado_reset_daily:
        radicado = f"{radicado_prefix}-{today}-{counter:0{radicado_pad}d}"
    else:
        radicado = f"{radicado_prefix}-{counter:0{radicado_pad}d}"

    mappings[mid] = radicado
    if len(mappings) > radicado_map_limit:
        keys = list(mappings.keys())[-radicado_map_limit:]
        mappings = {k: mappings[k] for k in keys}

    state["message_radicados"] = mappings
    return radicado
