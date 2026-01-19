# =========================
# IMPORTS (HERRAMIENTAS)
# =========================

import base64
# Gmail API entrega cuerpos y adjuntos codificados en base64 (base64url).
# Necesitamos base64 para decodificar y convertir eso en texto legible.

import os
import os.path
# os y os.path sirven para:
# - leer variables de entorno (CLIENT_SHEET_ID, etc.)
# - verificar si existe token.json (para no autenticar cada vez)

import re
# re se usa para "normalizar" texto (quitar símbolos, espacios, etc.)
# Esto ayuda a detectar clientes aunque el correo tenga caracteres raros.

from typing import Dict, List, Optional
# Solo es para dejar claro el tipo de datos (no es obligatorio, pero ayuda a entender).

# =========================
# GOOGLE AUTH + CLIENTES API
# =========================

from google.auth.transport.requests import Request
# Request se usa para refrescar el token cuando expira (renovar credenciales).

from google.auth.exceptions import RefreshError
# RefreshError nos permite detectar cuando el refresh_token deja de servir (scopes inválidos, etc.).

from google.oauth2.credentials import Credentials
# Credentials representa tus credenciales OAuth (access_token y refresh_token).

from google_auth_oauthlib.flow import InstalledAppFlow
# InstalledAppFlow: flujo OAuth para apps locales (abre navegador para autorizar).

from googleapiclient.discovery import build
# build crea el "cliente" (service) para llamar a Gmail API y Sheets API.

from googleapiclient.errors import HttpError
# HttpError captura errores específicos cuando una llamada a la API falla.

# =========================
# SCOPES (PERMISOS)
# =========================

# Pedimos permisos de lectura tanto para Gmail como para Sheets porque:
# - Queremos leer correos (Gmail)
# - Queremos leer la tabla de clientes (Sheets)
# Esto define lo que tu token podrá hacer.
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# =========================
# FILTROS DE GMAIL
# =========================

# Solo queremos correos no leídos; se puede sobreescribir con env GMAIL_QUERY si necesitas otro filtro.
GMAIL_QUERY = os.environ.get("GMAIL_QUERY", "is:unread")

# Limitar el número de mensajes para evitar procesar demasiados correos.
MAX_MESSAGES = int(os.environ.get("MAX_EMAILS", "10"))

# =========================
# CONFIG DE LA HOJA DE CLIENTES (CATÁLOGO)
# =========================

# Aquí defines de dónde sacar la lista de clientes y su tipo.
# Se puede configurar sin tocar código usando variables de entorno:
# - CLIENT_SHEET_ID: ID de la hoja
# - CLIENT_SHEET_RANGE: rango dentro de la hoja (ej: "Clientes!A:C")
#
# Si NO pones variables de entorno, usa valores por defecto.
SHEET_ID = os.environ.get("CLIENT_SHEET_ID", "14x7UflRW7P9qIHy65biueQUQjn03WBhV7T6l454VUmQ")
SHEET_RANGE = os.environ.get("CLIENT_SHEET_RANGE", "Clientes!A:B")


def _normalize_text(value: str) -> str:
    """
    Normaliza texto para comparaciones simples.
    Ejemplo:
    "Cliente S.A. (Bogotá)" -> "clientesabogota"

    ¿Por qué sirve?
    - Los correos pueden traer nombres con puntos, espacios, tildes, guiones, etc.
    - Si normalizamos, comparar se vuelve más fácil y menos frágil.
    """
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _decode_body(data: Optional[str]) -> str:
    """
    Decodifica texto base64url que devuelve Gmail API.

    Gmail API muchas veces te da el contenido en payload.body.data
    y eso viene codificado (no es texto normal). Aquí lo convertimos.
    """
    if not data:
        return ""
    try:
        decoded_bytes = base64.urlsafe_b64decode(data)
        # decode utf-8 con errors ignore para que no se caiga si hay caracteres raros
        return decoded_bytes.decode("utf-8", errors="ignore")
    except Exception:
        # Si algo falla (data corrupta o no es base64 válido), devolvemos vacío
        return ""


def extract_plain_text(payload: Dict) -> str:
    """
    Obtiene el texto plano del cuerpo del correo.

    ¿Por qué esto existe?
    - Un correo puede venir como "multipart", es decir, con varias partes:
      - text/plain (texto)
      - text/html (HTML)
      - adjuntos
      - partes anidadas
    - Gmail a veces no pone el texto en un solo lugar; puede estar "enterrado"
      en payload.parts[]. Entonces recorremos recursivamente.

    Estrategia:
    1) Si el payload tiene body.data directo, lo decodificamos.
    2) Si no, buscamos dentro de parts:
       - si encontramos text/plain, lo decodificamos y devolvemos.
       - si no, seguimos bajando en niveles (recursión).
    """

    if not payload:
        return ""

    # Caso 1: el cuerpo viene directo en payload.body.data
    body = payload.get("body", {})
    data = body.get("data")
    if data:
        return _decode_body(data)

    # Caso 2: el cuerpo está en partes (multipart)
    for part in payload.get("parts", []) or []:
        mime_type = part.get("mimeType", "")

        # Si esta parte es texto plano, la devolvemos
        if mime_type == "text/plain":
            return _decode_body(part.get("body", {}).get("data"))

        # Si no es text/plain, puede ser multipart que contiene otras partes
        nested = extract_plain_text(part)
        if nested:
            return nested

    # Si no encontramos texto plano
    return ""


def load_client_catalog(sheets_service) -> List[Dict[str, Optional[str]]]:
    """
    Lee la hoja de cálculo y devuelve una lista de clientes conocidos.

    ¿Qué devuelve?
    Una lista de diccionarios, por ejemplo:
    [
      {"name": "Cliente A", "normalized": "clientea", "payment_type": "contado"},
      {"name": "Cliente B", "normalized": "clienteb", "payment_type": "credito"}
    ]

    ¿Por qué lo hacemos?
    - Para detectar automáticamente quién es el cliente cuando llega un correo
    - Para saber si ese cliente es contado o crédito (según tu tabla)
    """

    # Si no configuraste el SHEET_ID real, no podemos leer catálogo.
    if SHEET_ID == "TU_SHEET_ID_AQUI":
        print(
            "Configura CLIENT_SHEET_ID/CLIENT_SHEET_RANGE para habilitar la detección de clientes."
        )
        return []

    # Llamada a Google Sheets API: lee el rango definido
    try:
        result = (
            sheets_service.spreadsheets()
            .values()
            .get(spreadsheetId=SHEET_ID, range=SHEET_RANGE)
            .execute()
        )
    except HttpError as error:
        decoded_content = ""
        raw_content = getattr(error, "content", b"")
        if isinstance(raw_content, bytes):
            decoded_content = raw_content.decode("utf-8", errors="ignore")
        else:
            decoded_content = str(raw_content)

        if "SERVICE_DISABLED" in decoded_content or "sheets.googleapis.com" in decoded_content:
            print(
                "No se pudo leer la hoja porque la Google Sheets API está deshabilitada en esta "
                "cuenta. Actívala en https://console.developers.google.com/apis/api/sheets.googleapis.com/overview "
                "y vuelve a ejecutar el script. Continuaré sin catálogo."
            )
            return []

        # Otros errores de Sheets se envían hacia arriba para que main() los muestre.
        raise

    values = result.get("values", [])
    catalog: List[Dict[str, Optional[str]]] = []

    # Iteramos cada fila (row) del sheet
    for row in values:
        if not row:
            continue

        # Columna A: nombre del cliente
        name = row[0].strip()

        # Saltamos filas vacías o el encabezado "Cliente"
        if not name or name.lower() == "cliente":
            continue

        # Columna B: tipo de pago (contado/credito)
        payment_type = row[1].strip().lower() if len(row) > 1 else None

        catalog.append(
            {
                "name": name,
                "normalized": _normalize_text(name),
                # Solo aceptamos "contado" o "credito"; si viene otra cosa queda None
                "payment_type": payment_type if payment_type in {"contado", "credito"} else None,
            }
        )

    return catalog


def find_client(text: str, catalog: List[Dict[str, Optional[str]]]) -> Optional[Dict[str, Optional[str]]]:
    """
    Busca si algún cliente del catálogo aparece en el texto del correo.

    Estrategia:
    - Normalizamos el texto del correo
    - Normalizamos cada cliente
    - Si el nombre normalizado está incluido dentro del texto normalizado, lo detectamos

    Ejemplo:
    texto: "Hola, envío documentos de Cliente A..."
    normalized_text contiene "clientea" -> detecta Cliente A
    """

    normalized_text = _normalize_text(text)
    for client in catalog:
        if client["normalized"] and client["normalized"] in normalized_text:
            return client
    return None


def determine_payment_type(text: str) -> Optional[str]:
    """
    Identifica palabras clave para clasificar el correo como contado o crédito.

    Esto sirve como "plan B" cuando:
    - el cliente no está en catálogo, o
    - el catálogo no tiene payment_type definido,
    - pero el correo menciona "contado" o "crédito".

    Nota: esto es heurístico (no perfecto).
    """

    lowered = text.lower()
    if "contado" in lowered:
        return "contado"
    if "credito" in lowered or "crédito" in lowered:
        return "credito"
    return None


def get_header(payload: Dict, name: str) -> str:
    """
    Obtiene un header (Subject, From, etc.) del mensaje.

    Gmail guarda headers como lista:
    payload["headers"] = [{"name": "Subject", "value": "..."}, ...]

    Este método busca el header por nombre.
    """

    target = name.lower()
    for header in payload.get("headers", []):
        if header.get("name", "").lower() == target:
            return header.get("value", "")
    return ""


def main():
    """
    Flujo completo del script:

    1) Autenticarse (OAuth):
       - Si existe token.json: lo usa (no pide login)
       - Si expira y hay refresh_token: refresca
       - Si no: abre navegador para autorizar y crea token.json

    2) Crear servicios:
       - Gmail API service (gmail v1)
       - Sheets API service (sheets v4)

    3) Cargar catálogo de clientes desde Sheets (si está configurado)

    4) Listar mensajes de INBOX
       - Nota: list() devuelve IDs, no el contenido completo.

    5) Por cada mensaje:
       - get() en format="full" para traer payload completo
       - extraer Subject (header)
       - extraer texto plano del body (recursivo)
       - juntar subject + body + snippet en un texto "searchable"
       - buscar cliente en catálogo
       - determinar contado/crédito por catálogo o keywords
       - imprimir el resultado
    """

    # =========================
    # AUTENTICACIÓN OAUTH
    # =========================

    creds = None

    # Si token.json existe, lo cargamos para evitar login cada vez
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # Si no hay creds o son inválidas, nos autenticamos
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                # Renovación automática (sin navegador)
                creds.refresh(Request())
            except RefreshError as refresh_error:
                # Token caducó o perdió permisos (invalid_scope). Limpiamos y forzamos login.
                print(
                    "No se pudo refrescar el token (probable cambio de permisos). "
                    "Eliminando token.json para re-autenticar..."
                )
                try:
                    os.remove("token.json")
                except FileNotFoundError:
                    pass
                creds = None
        if not creds or not creds.valid:
            # Primer login, sin refresh token o refresh fallido: abrir navegador
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        # Guardamos para próximas ejecuciones
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        # =========================
        # CREAR CLIENTES (SERVICES)
        # =========================

        # Cliente Gmail API
        gmail_service = build("gmail", "v1", credentials=creds)

        # Cliente Sheets API (para leer catálogo de clientes)
        sheets_service = build("sheets", "v4", credentials=creds)

        # Cargar catálogo desde Sheets
        client_catalog = load_client_catalog(sheets_service)

        # =========================
        # LISTAR MENSAJES (solo IDs)
        # =========================

        results = (
            gmail_service.users()
            .messages()
            .list(
                userId="me",
                labelIds=["INBOX"],
                q=GMAIL_QUERY,
                maxResults=max(1, min(MAX_MESSAGES, 500)),
            )
            .execute()
        )

        # list() normalmente devuelve [{"id": "...", "threadId": "..."}]
        messages = results.get("messages", [])[:MAX_MESSAGES]

        if not messages:
            print("No se encontraron mensajes con el filtro actual.")
            return

        # =========================
        # PROCESAR MENSAJES
        # =========================

        print("Messages:")
        for message in messages:
            # =========================
            # OBTENER MENSAJE COMPLETO
            # =========================

            msg = (
                gmail_service.users()
                .messages()
                .get(userId="me", id=message["id"], format="full")
                .execute()
            )

            # msg incluye:
            # - payload (headers, parts, etc.)
            # - snippet (texto corto de vista previa)
            payload = msg.get("payload", {})
            snippet = msg.get("snippet", "")

            # Sacamos el Subject real desde headers
            subject = get_header(payload, "Subject")

            # Sacamos texto plano del cuerpo (si existe)
            body_text = extract_plain_text(payload)

            # Construimos un texto combinado para buscar cliente/tipo
            # (porque a veces el nombre aparece en subject, a veces en body o snippet)
            searchable_text = f"{subject}\n{body_text}\n{snippet}"

            # =========================
            # DETECCIÓN DE CLIENTE
            # =========================

            # Si tenemos catálogo, intentamos detectar cliente por coincidencia
            client = find_client(searchable_text, client_catalog) if client_catalog else None

            # payment_type preferido: el del catálogo
            payment_type = client.get("payment_type") if client else None

            # si no hay tipo en catálogo o no detectó cliente, intentamos por keywords
            if not payment_type:
                payment_type = determine_payment_type(searchable_text)

            # =========================
            # SALIDA (PRINT)
            # =========================

            print(f"  Message ID: {message['id']}")

            # Si Subject está vacío, usamos snippet como fallback para mostrar algo
            print(f"  Subject: {subject or snippet}")

            if client:
                print(f"  Cliente detectado: {client['name']}")
            else:
                print("  Cliente detectado: no identificado")

            if payment_type:
                print(f"  Tipo detectado: {payment_type}")
            else:
                print("  Tipo detectado: indeterminado")

            print("-" * 40)

    except HttpError as error:
        # Si Gmail/Sheets devuelven error de API, cae aquí
        print(f"An error occurred: {error}")


# Punto de entrada típico en Python:
# si ejecutas este archivo directamente, corre main().
if __name__ == "__main__":
    main()
