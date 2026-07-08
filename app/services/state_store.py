"""
Lectura y escritura segura de estado con rutas inyectables.

Este modulo espeja _state_file_for_account, load_state, save_state de
reademail.py (lineas ~368-390). A diferencia del original, no depende
de globals STATE_FILE/ACCOUNTS_DIR: recibe las rutas como parametros.

Esto permite:
- Pruebas con tmp_path sin tocar accounts/ real.
- Eventual inyeccion en reademail.py sin romper su interfaz actual.

Todavia no esta conectado a reademail.py.
"""

import json
import os
from pathlib import Path
from typing import Dict, Optional, Union


def state_file_for_account(
    account_email: Optional[str],
    state_file: Union[str, Path],
    accounts_dir: Union[str, Path],
) -> str:
    """
    Resuelve la ruta del archivo JSON de estado para una cuenta.

    Si account_email es None/falsy, devuelve state_file.
    Si no, devuelve accounts_dir/<account_email>/gmail_watch_state.json.
    """
    if not account_email:
        return str(state_file)
    return os.path.join(str(accounts_dir), str(account_email), "gmail_watch_state.json")


def load_state(
    account_email: Optional[str],
    state_file: Union[str, Path],
    accounts_dir: Union[str, Path],
) -> Dict:
    """
    Carga el dict de estado desde JSON.

    Devuelve {} si:
    - El archivo no existe.
    - El JSON es invalido.
    - El contenido no es un dict.
    """
    path = state_file_for_account(account_email, state_file, accounts_dir)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(
    state: Dict,
    account_email: Optional[str],
    state_file: Union[str, Path],
    accounts_dir: Union[str, Path],
) -> None:
    """
    Guarda el dict de estado como JSON.

    Crea directorios intermedios si no existen.
    """
    path = state_file_for_account(account_email, state_file, accounts_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
