# Deploy en Ubuntu Server 22.04

Servidor objetivo:

- Ubuntu Server 22.04 LTS
- App path sugerido: `/opt/readmail.com`
- Usuario de servicio sugerido: `admincentury`
- Puerto SSH: `22`

No subas `.venv`, `__pycache__`, `.env`, `token.json`, `credentials.json`, `accounts/` ni `gmail_watch_state.json` al repo.

## 1. Preparar paquetes del sistema

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git tesseract-ocr tesseract-ocr-spa
```

## 2. Subir el proyecto

Opcion con Git:

```bash
sudo mkdir -p /opt/readmail.com
sudo chown -R admincentury:admincentury /opt/readmail.com
cd /opt/readmail.com
git clone <URL_DEL_REPO> .
```

Opcion con `scp` desde Windows:

```powershell
scp -r .\* admincentury@10.15.1.27:/opt/readmail.com/
```

## 3. Crear entorno e instalar dependencias

```bash
cd /opt/readmail.com
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 4. Configurar variables y credenciales

```bash
cp .env.example .env
nano .env
```

Coloca en `.env` los valores reales de Google Cloud, Pub/Sub, Sheets y Gmail. Copia `credentials.json` al root del proyecto o a cada carpeta de cuenta si usas multi-cuenta.

Para autorizar una cuenta en el servidor:

```bash
cd /opt/readmail.com
. .venv/bin/activate
python reademail.py --authorize-account correo@dominio.com
```

Si el servidor no tiene navegador, genera el token localmente y sube el `token.json` correspondiente. Para multi-cuenta, cada cuenta vive en:

```text
/opt/readmail.com/accounts/<correo>/token.json
/opt/readmail.com/accounts/<correo>/credentials.json
```

## 5. Instalar como servicio systemd

```bash
sudo cp /opt/readmail.com/deploy/readmail.service /etc/systemd/system/readmail.service
sudo systemctl daemon-reload
sudo systemctl enable readmail
sudo systemctl start readmail
```

Ver estado y logs:

```bash
sudo systemctl status readmail
journalctl -u readmail -f
```

Reiniciar despues de cambios:

```bash
sudo systemctl restart readmail
```

## 6. Seguridad minima

- Cambia la contrasena compartida por chat/captura.
- Usa llave SSH y deshabilita login por password cuando ya tengas acceso estable.
- Mantén `.env`, `credentials.json`, `token.json` y `accounts/` fuera de Git.
- Revisa permisos:

```bash
chmod 600 /opt/readmail.com/.env
chmod 600 /opt/readmail.com/credentials.json 2>/dev/null || true
find /opt/readmail.com/accounts -type f -name '*.json' -exec chmod 600 {} \; 2>/dev/null || true
```
