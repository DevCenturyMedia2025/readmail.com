# Configuracion de Google API para readmail.com

Esta guia prepara Google Cloud para `reademail.py`.

El script usa:

- Gmail API: leer correos, modificar etiquetas y responder en el mismo hilo.
- Google Sheets API: leer el catalogo/lista blanca de clientes.
- Pub/Sub: recibir eventos de Gmail cuando llegan cambios al INBOX.
- OAuth Desktop app: generar `credentials.json` y `token.json`.

Documentacion oficial:

- Gmail push notifications: https://developers.google.com/workspace/gmail/api/guides/push
- Gmail `users.watch`: https://developers.google.com/workspace/gmail/api/reference/rest/v1/users/watch
- Sheets Python quickstart: https://developers.google.com/workspace/sheets/api/quickstart/python
- Pub/Sub topic: https://cloud.google.com/pubsub/docs/create-topic
- Pub/Sub subscription: https://cloud.google.com/pubsub/docs/create-subscription

## 1. Crear o elegir proyecto en Google Cloud

1. Entra a https://console.cloud.google.com/
2. Arriba, selecciona o crea un proyecto.
3. Guarda el `Project ID`, no solo el nombre visible.

Ese valor va en `.env`:

```env
GCP_PROJECT_ID=tu-project-id
```

## 2. Activar APIs

En el proyecto, ve a **APIs & Services > Library** y activa:

1. Gmail API
2. Google Sheets API
3. Cloud Pub/Sub API

## 3. Configurar pantalla OAuth

Ve a **Google Auth Platform**.

1. Entra a **Branding**.
2. Si sale `Get Started`, inicia la configuracion.
3. Tipo de usuario:
   - Usa `Internal` si el dominio de Google Workspace lo permite.
   - Usa `External` si no aparece `Internal`.
4. Llena:
   - App name: `readmail.com`
   - User support email: tu correo administrador
   - Developer contact information: tu correo administrador
5. En **Audience**, si el app esta en modo Testing, agrega como test users los correos Gmail que vas a autorizar.
6. En **Data Access / Scopes**, agrega estos scopes:

```text
https://www.googleapis.com/auth/gmail.modify
https://www.googleapis.com/auth/gmail.readonly
https://www.googleapis.com/auth/gmail.send
https://www.googleapis.com/auth/spreadsheets
```

Son los mismos scopes definidos en `reademail.py`.

## 4. Crear OAuth Client ID

Ve a **Google Auth Platform > Clients**.

1. Click en **Create Client**.
2. Application type: **Desktop app**.
3. Name: `readmail-desktop`.
4. Click en **Create**.
5. Descarga el JSON.
6. Renombra el archivo a:

```text
credentials.json
```

Para modo de una sola cuenta, va en:

```text
/opt/readmail.com/credentials.json
```

Para modo multi-cuenta, puedes usar el mismo `credentials.json` en el root o copiar uno por cuenta:

```text
/opt/readmail.com/accounts/correo@dominio.com/credentials.json
```

## 5. Crear topic de Pub/Sub

En Google Cloud Console ve a **Pub/Sub > Topics**.

1. Click **Create topic**.
2. Topic ID sugerido:

```text
gmail-watch
```

3. Crea el topic.

El topic completo queda asi:

```text
projects/tu-project-id/topics/gmail-watch
```

Ese valor va en `.env`:

```env
PUBSUB_TOPIC_FULL=projects/tu-project-id/topics/gmail-watch
```

## 6. Dar permiso a Gmail para publicar en el topic

Este paso es obligatorio para que `users.watch` funcione.

En **Pub/Sub > Topics > gmail-watch > Permissions**, agrega este principal:

```text
gmail-api-push@system.gserviceaccount.com
```

Rol:

```text
Pub/Sub Publisher
```

Sin este permiso, Gmail no puede publicar eventos en el topic.

## 7. Crear subscription pull

En **Pub/Sub > Subscriptions**:

1. Click **Create subscription**.
2. Subscription ID sugerido:

```text
gmail-watch-sub
```

3. Topic: `gmail-watch`.
4. Delivery type: **Pull**.
5. Ack deadline: puedes dejar default o subirlo. El script luego intenta extenderlo con `PUBSUB_ACK_DEADLINE_SECONDS`.
6. Crear.

Ese valor va en `.env`:

```env
PUBSUB_SUBSCRIPTION=gmail-watch-sub
```

## 8. Configurar Google Sheet de clientes

1. Abre tu Google Sheet.
2. Copia el ID de la URL.

Ejemplo:

```text
https://docs.google.com/spreadsheets/d/ESTE_ES_EL_ID/edit
```

En `.env`:

```env
CLIENT_SHEET_ID=ESTE_ES_EL_ID
CLIENT_SHEET_RANGE=Clientes!A:Z
CLIENT_LOOKUP_RANGE=Clientes!A:Z
```

El correo que autorizas por OAuth debe tener permiso de lectura sobre esa hoja.

## 9. Completar `.env`

En el servidor:

```bash
cd /opt/readmail.com
cp .env.example .env
nano .env
```

Minimo necesario:

```env
GCP_PROJECT_ID=tu-project-id
PUBSUB_TOPIC_FULL=projects/tu-project-id/topics/gmail-watch
PUBSUB_SUBSCRIPTION=gmail-watch-sub
GMAIL_LABEL_IDS=INBOX

CLIENT_SHEET_ID=tu-sheet-id
CLIENT_SHEET_RANGE=Clientes!A:Z
CLIENT_LOOKUP_RANGE=Clientes!A:Z
```

Para multi-cuenta:

```env
GMAIL_ACCOUNTS=correo1@dominio.com,correo2@dominio.com
ACCOUNTS_DIR=accounts
```

## 10. Autorizar Gmail

### Opcion A: una sola cuenta

En el servidor:

```bash
cd /opt/readmail.com
. .venv/bin/activate
python reademail.py
```

El script abrira flujo OAuth si no existe `token.json`.

### Opcion B: multi-cuenta

Autoriza cada cuenta:

```bash
cd /opt/readmail.com
. .venv/bin/activate
python reademail.py --authorize-account correo@dominio.com
```

Esto guarda el token en:

```text
accounts/correo@dominio.com/token.json
```

Si el servidor no tiene navegador, autoriza en tu PC y luego sube el `token.json` al servidor.

## 11. Probar antes de activar systemd

```bash
cd /opt/readmail.com
. .venv/bin/activate
python reademail.py
```

Debes ver algo parecido a:

```text
Autenticado como: correo@dominio.com
Catalogo/lista blanca cargado: ...
Watch activo...
Escuchando Pub/Sub...
```

## 12. Activar como servicio

```bash
sudo cp /opt/readmail.com/deploy/readmail.service /etc/systemd/system/readmail.service
sudo systemctl daemon-reload
sudo systemctl enable readmail
sudo systemctl start readmail
journalctl -u readmail -f
```

## Checklist rapido

- APIs activas: Gmail API, Google Sheets API, Pub/Sub API.
- OAuth Client ID tipo Desktop app descargado como `credentials.json`.
- Scopes coinciden con `reademail.py`.
- Topic creado: `gmail-watch`.
- Gmail tiene permiso `Pub/Sub Publisher` sobre el topic.
- Subscription pull creada: `gmail-watch-sub`.
- `.env` tiene `GCP_PROJECT_ID`, `PUBSUB_TOPIC_FULL`, `PUBSUB_SUBSCRIPTION`, `CLIENT_SHEET_ID`.
- Cuenta Gmail autorizada y con acceso al Sheet.
- Servicio systemd activo.
