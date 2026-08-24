# ReadMail — Manual técnico

> Arquitectura, configuración, operación y estado del refactor.
> Actualizado: 12 de agosto de 2026 · Rama de trabajo: `refactor/arquitectura-base`

## 1. Arquitectura

```
Portal web (WordPress)  ──escribe──►  Google Sheets + Google Drive
                                              │
Gmail ──watch──► Pub/Sub ──pull──►  ReadMail (Ubuntu, systemd)
                                              │
                          ┌───────────────────┼───────────────────┐
                     Etiquetas Gmail    Respuestas/reenvíos   Alertas WhatsApp
                                              │
                                    Estado local (JSON por cuenta)
```

- **Runtime productivo:** `reademail.py` (monolito, ~3.400 líneas). Es el entrypoint.
- **Arquitectura modular en transición:** paquetes bajo `app/`. Extracciones puras con pruebas; la mayoría **aún no conectadas** al monolito.
- **Excepción:** `app/services/alternate_recipient.py` sí está conectado y en uso.
- Python 3.12 en CI. No hay API HTTP: es un proceso listener de larga duración.

## 2. Regla de oro al modificar

Antes de tocar código, determinar el objetivo:

- **Cambiar producción hoy** → la fuente de verdad es `reademail.py`.
- **Trabajar en la refactorización** → modificar el módulo puro bajo `app/` y sus pruebas, sin asumir que está conectado.
- **Conectar un módulo** → tarea explícita de integración con pruebas de regresión.

No editar simultáneamente el monolito y su copia modular salvo que la tarea lo exija. Muchos tests congelan textos, patrones y defaults exactos: un cambio cosmético puede ser un cambio de negocio.

## 3. Flujo de `process_message()`

1. Carga estado de la cuenta; evita reprocesar mensajes ya procesados/respondidos.
2. Genera o reutiliza el radicado (`RAD-YYYYMMDD-NNNNNN`).
3. **Detección de rebotes** (antes del filtro de adjuntos): mueve la factura original y el rebote a revisión manual, alerta por WhatsApp.
4. **Filtro de antigüedad**: más de `MAX_DIAS_ANTIGUEDAD` → revisión manual (se ignora en `MODO_PRUEBAS`).
5. Extrae remitente; si no hay, termina.
6. Si `ONLY_WITH_ATTACHMENTS` y no hay adjuntos, termina.
7. Descarga adjuntos, abre ZIP y ZIP anidados, unifica PDF/XML/imágenes.
8. **Ruta administrativa**: NIT del catálogo, o NIT/nombre exacto en el asunto contra Administrativas/CajaMenor.
9. **Nota de crédito**: asunto/cuerpo, nombre de PDF, texto de PDF.
10. Sin PDF → revisión manual.
11. **Clasificación**: con XML → factura electrónica; sin XML → cuenta de cobro.
12. Validaciones según tipo.
13. Rechazo o aprobación, con etiquetado y respuesta.
14. Marca `replied` y `processed` **aunque el envío falle** (evita duplicados; congelado por tests).

## 4. Reglas de negocio activas

### Ruta administrativa
- Pestañas: `Administrativas`, `CajaMenor`. Títulos resueltos tolerando espacios, mayúsculas, acentos y caracteres invisibles.
- Rango `A:B`, con detección de NIT/nombre **por contenido** (no por posición).
- NIT normalizado a dígitos; los de 10 dígitos se indexan completo y sin posible DV.
- Coincidencia por nombre: palabra completa, siglas puntuadas equivalentes (`S.A.S` = `SAS`), solo en el **asunto**.
- `AUTO_FILL_NIT_ENABLED` apagado por defecto; si se activa solo escribe en filas con la celda A vacía y respeta `MODO_PRUEBAS`/`DRY_RUN`.

### Entidades registradas (Clientes / Terceros) — cargado, aún no conectado
- Rango `A:M`. Esquema: A=ID, B=Nit, C=Nombre, D=Correo, E=Estado, J=carpeta Drive, K=RUT, L=Cámara, M=Certificación bancaria.
- Detección por encabezado con **fallback por campo** (evita el 0 silencioso).
- Solo carga entidades con Estado en `ACTIVE_VALUES` (o vacío).
- `registered_docs` indexado en todas las formas del NIT y fusionado por campo (una fila vacía no borra IDs de otra).

### Factura electrónica
Defaults actuales: `MIN_PDF_FACTURA_ELECTRONICA=3`, `MIN_XML_FACTURA_ELECTRONICA=1`, orden de compra, cliente identificado y OK de compras.

> **Cambio de negocio acordado (pendiente de implementar):** el rechazo debe quedar únicamente por falta de **orden de compra** u **OK de compras**. El mínimo de PDF y el "cliente no identificado" dejan de ser motivos de rechazo (este último pasa a revisión manual). En `MODO_PRUEBAS`, en vez de rechazar se reenvía a `COMPRAS_EMAIL`.

### Cuenta de cobro
Paquete de cinco: cuenta de cobro, cédula, RUT, certificado bancario, orden de compra. Clasificación por nombre de archivo y texto extraído; empate de puntaje = desconocido. Tolerancia `completo_con_desconocido` si hay ≥4 requeridos y exactamente un archivo no identificado.

### Extracción de PDF
Usa `pypdf` (fallback `PyPDF2`). **No usa OCR.** PDFs escaneados sin capa de texto fallan en orden, cliente, nota de crédito y OK de compras. El script aparte `clasificador_docs.py` sí usa OCR pero no forma parte del listener.

## 5. Seguridad de ZIP

Procesamiento en memoria. Anidación ≤2 niveles; ZIP ≤25 MiB; ≤250 entradas; ≤150 MiB descomprimido; ≤25 MiB por archivo. Rechaza cifrados y rutas inseguras (`../`, absolutas). Omite `__MACOSX`, AppleDouble y `.DS_Store`.

## 6. Destinatario alterno de rechazos

Con `ALT_RECIPIENT_ENABLED=true`, los rechazos a proveedores tecnológicos/no-reply resuelven destinatario en orden:

1. Correo del proveedor en `AccountingSupplierParty` del XML (maneja el CDATA del AttachedDocument; ignora firmas digitales).
2. Correo de contacto del catálogo según NIT del asunto DIAN.
3. `ALT_FALLBACK_EMAIL`.

Se bloquea el dominio interno en las fuentes 1 y 2 (anti-loop); el fallback interno sí se permite y además mueve el mensaje a revisión manual. Adjuntos >20 MiB no se reenvían: se envía solo texto con aviso. El reenvío incluye el mensaje original citado.

## 7. Detección de rebotes

Detecta DSN por remitente (`mailer-daemon`, `postmaster`), asunto de fallo, o `multipart/report`. Extrae el radicado (`RAD-\d{8}-\d{6}`) y el destinatario fallido, localiza el correo original y lo mueve de RECHAZADOS a REVISIÓN MANUAL. Alerta por WhatsApp con cuenta + radicado + correo fallido, con cooldown por rebote.

## 8. Configuración (`.env`)

### Obligatorias
`GCP_PROJECT_ID`, `PUBSUB_SUBSCRIPTION`, `PUBSUB_TOPIC_FULL`

### Gmail y estado
`GMAIL_LABEL_IDS`, `GMAIL_WATCH_STATE_FILE`, `GMAIL_ACCOUNTS`, `ACCOUNTS_DIR`, `MODO_PRUEBAS`, `ETIQUETA_PRUEBAS`

### Sheets
`CLIENT_SHEET_ID`, `CLIENT_SHEET_RANGE`, `CLIENT_LOOKUP_RANGE`, `ACTIVE_VALUES`, `AUTO_FILL_NIT_ENABLED`

### Etiquetas
`LABEL_ADMIN_NAME`, `LABEL_REVIEW_NAME`, `LABEL_NOTE_CREDIT_NAME`, `LABEL_APPROVED_NAME`, `LABEL_REJECTED_NAME`

### Validaciones y ZIP
`MIN_PDF_FACTURA_ELECTRONICA`, `MIN_XML_FACTURA_ELECTRONICA`, `MIN_PDF_CUENTA_COBRO`, `MAX_ZIP_BYTES`, `MAX_ZIP_FILES`, `MAX_ZIP_TOTAL_UNCOMPRESSED`, `MAX_ZIP_SINGLE_FILE`, `MAX_ZIP_NESTING`

### Flags operativos
`ARCHIVE_*`, `ALT_RECIPIENT_ENABLED`, `ALT_FALLBACK_EMAIL`, `LIMITE_ANTIGUEDAD_ENABLED`, `MAX_DIAS_ANTIGUEDAD`, `WHATSAPP_*`, `TOKEN_ALERT_EMAIL`, `INTERACTIVE_AUTH`, `TOKEN_ALERT_COOLDOWN_HOURS`, `COMPRAS_EMAIL`

> `DRY_RUN` en `app/config/settings.py` es seguro por defecto, pero ese módulo **no está conectado**. En `reademail.py` su default es `false` y hoy solo protege el auto-fill de NIT: **no es un modo global sin efectos secundarios**.

## 9. Permisos (OAuth)

```
gmail.modify · gmail.readonly · gmail.send · spreadsheets
```

> ⚠️ El scope de Sheets pasó de `readonly` a **escritura** (`spreadsheets`) para el auto-fill de NIT. **Este cambio exige reautorizar todas las cuentas antes de desplegar**, o el programa fallará al autenticar.

## 10. Operación

### Local (Windows)
```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
Copy-Item .env.example .env    # y completar valores

.\.venv\Scripts\python.exe reademail.py --authorize-account correo@dominio.com
.\.venv\Scripts\python.exe reademail.py
.\.venv\Scripts\python.exe -m pytest -q
```

### Producción (Ubuntu)
- Ruta: `/opt/readmail.com` · usuario `admincentury` · unidad `deploy/readmail.service`

```bash
sudo systemctl status readmail
sudo journalctl -u readmail -f
sudo systemctl restart readmail
```

### Rollback
```bash
sudo systemctl stop readmail
git checkout v1.0-estable
sudo systemctl start readmail
```
El estado local es compatible hacia atrás; los mensajes ya respondidos no se re-responden.

## 11. Estado y idempotencia

Archivos: `gmail_watch_state.json` (cuenta única) o `accounts/<correo>/gmail_watch_state.json`.

Claves: `processed_message_ids`, `replied_message_ids`, `message_radicados`, `last_history_id`, `watch_expiration_ms`, `radicado_counter`, `radicado_date`, `token_alert_sent_at`.

Caches limitados a `PROCESSED_CACHE_LIMIT`. Radicado con reinicio diario, reutilizado para el mismo `message_id`.

## 12. Seguridad de datos

Nunca subir, mostrar ni loguear: `.env`, `credentials.json`, `token.json`, `accounts/`, `gmail_watch_state.json`, teléfonos, API keys o correos privados. Todos están en `.gitignore`. Pre-commit incluye chequeo de secretos (Gitleaks).

**En tests:** prohibidos los datos reales de terceros (NIT, razón social, correos). Usar siempre valores ficticios.

## 13. Estado del refactor

**Conectado y en producción:** filtros previos, ruta administrativa por hojas, notas de crédito, clasificación por XML, validaciones, rebotes, antigüedad, destinatario alterno, alertas WhatsApp.

**Construido pero NO conectado:**
- `app/services/email_routing.py` — router puro por validación en hojas (Administrativas/CajaMenor → admin; Clientes/Terceros → FE; ninguna → revisión manual).
- `load_registered_entities` / `registered_docs` — lectura de Clientes/Terceros con IDs de Drive.
- Resto de módulos en `app/` (espejos del monolito con pruebas).

**Pendiente inmediato:** aplicar el cambio de negocio de factura electrónica (§4) y conectar el router.

## 14. Deuda técnica conocida

1. Duplicación entre `app/` y `reademail.py`: pueden divergir.
2. Router no conectado: producción usa decisiones distintas a `email_routing.py`.
3. `DRY_RUN` inconsistente entre el módulo de settings y el monolito.
4. Sin OCR en el listener: PDFs escaneados generan rechazos falsos.
5. Configuración cargada en el import de `reademail.py`: acopla los tests.
6. Manejo amplio de excepciones (`except Exception` + `print`) en varias rutas.
7. Cobertura de CI limitada a `app/` aunque varios tests ejercitan `reademail.py`.
8. Artefactos versionados (`.pyc`, `readmail-deploy.tar.gz`) que conviene limpiar.

## 15. Protocolo de trabajo

1. Leer el archivo objetivo y sus tests antes de cambiar nada.
2. Un cambio a la vez, con pruebas verdes.
3. Revisión independiente antes de cada push (checklist estricto si toca `reademail.py`).
4. Preservar textos de correo, regex, nombres de etiquetas y defaults exactos salvo instrucción expresa.
5. Una sola etiqueta de estado final por mensaje.
6. Conservar idempotencia: nunca responder dos veces.
7. Sin I/O real en tests.
8. No hacer commit, push, despliegue ni OAuth sin autorización explícita.
