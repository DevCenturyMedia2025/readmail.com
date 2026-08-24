# ReadMail — Checklist hasta la prueba de 28 días

> Marcar cada paso al completarlo. No saltar el orden: hay dependencias.
> Actualizado: 12 de agosto de 2026

---

## FASE 1 — Ordenar el repositorio

- [ ] **1.1 Subir los commits pendientes**
  ```powershell
  git push origin refactor/arquitectura-base
  ```
  Verificar CI verde en GitHub Actions.

- [ ] **1.2 Commitear la documentación nueva**
  ```powershell
  git add docs/01_QUE_HACE_EL_SISTEMA.md docs/02_MANUAL_USUARIO.md docs/03_MANUAL_TECNICO.md docs/04_FLUJO_OBJETIVO.md docs/05_CHECKLIST_DESPLIEGUE.md
  git commit -m "docs: documentacion del sistema, flujo objetivo y checklist"
  ```

- [ ] **1.3 Eliminar documentación duplicada y obsoleta**
  ```powershell
  git rm docs/manual_tecnico.md docs/manual_usuario.md manual_tecnico.md
  git commit -m "docs: eliminar manuales duplicados y desactualizados"
  ```
  (Verificar antes que no contengan información que no esté ya en los nuevos.)

- [ ] **1.4 Verificar que no hay secretos rastreados**
  ```powershell
  git ls-files | Select-String -Pattern "credential|token|\.env$|accounts/"
  ```
  → debe salir vacío (salvo `.env.example`).

---

## FASE 2 — Completar el código

- [ ] **2.1 A4/A5 — rechazo solo por orden/OK + reenvío a Compras en modo pruebas**
  Es lo que va a operar durante los 28 días. Sin esto, la prueba no refleja el comportamiento deseado.
  - [ ] Codex implementa
  - [ ] Claude Code revisa → APROBADO
  - [ ] Push + CI verde
  - [ ] `COMPRAS_EMAIL` documentado en `.env.example`

- [ ] **2.2 (Opcional) Arreglar la extracción del cliente**
  En el formato de orden de compra actual devuelve el NIT en vez del nombre. Puede provocar muchas revisiones manuales durante la prueba.

- [ ] **2.3 (Opcional) Fuga del rótulo `"Orden de compra:"` vacío**

---

## FASE 3 — Docker (opcional, si se decide contenerizar)

- [ ] **3.1 Dockerfile + .dockerignore + docker-compose.yml**
- [ ] **3.2 Revisión: ningún secreto entra a la imagen**
- [ ] **3.3 Build local exitoso**
- [ ] **3.4 Documentar en `docs/06_DESPLIEGUE_DOCKER.md`**

---

## FASE 4 — Preparar el servidor

- [ ] **4.1 Respaldo del estado actual del servidor**
  ```bash
  sudo systemctl stop readmail
  sudo cp -r /opt/readmail.com /opt/readmail.com.backup-$(date +%Y%m%d)
  ```

- [ ] **4.2 Confirmar el punto de retorno**
  ```bash
  cd /opt/readmail.com && git tag | grep v1.0-estable
  ```
  → debe existir. Es el rollback.

- [ ] **4.3 Reautorizar las 3 cuentas** ⚠️ **OBLIGATORIO**
  El scope de Sheets pasó a escritura. Sin esto el programa NO arranca.
  Desde el PC (requiere navegador):
  ```powershell
  .\.venv\Scripts\python.exe reademail.py --authorize-account facturacion@century-media.net
  .\.venv\Scripts\python.exe reademail.py --authorize-account facturacion@eliteagencia.com
  .\.venv\Scripts\python.exe reademail.py --authorize-account facturacion@newsapiens.com
  ```
  - [ ] Verificar que cada una diga `✅ Cuenta autorizada correctamente: <el correo correcto>`
  - [ ] Copiar los `token.json` resultantes al servidor (`/opt/readmail.com/accounts/<correo>/`)

- [ ] **4.4 Actualizar el `.env` del servidor** con las variables nuevas:
  ```
  MODO_PRUEBAS=true
  ETIQUETA_PRUEBAS=pruebas
  COMPRAS_EMAIL=<correo de Compras>
  ALT_RECIPIENT_ENABLED=true
  ALT_FALLBACK_EMAIL=<buzón interno>
  WHATSAPP_ALERT_ENABLED=true
  WHATSAPP_PHONE=<número>
  WHATSAPP_APIKEY=<key>
  TOKEN_ALERT_EMAIL=<correo admin>
  INTERACTIVE_AUTH=false
  AUTO_FILL_NIT_ENABLED=false
  LIMITE_ANTIGUEDAD_ENABLED=true
  MAX_DIAS_ANTIGUEDAD=5
  ```

- [ ] **4.5 Crear la etiqueta "pruebas"** en la cuenta que se va a vigilar (si no existe).

---

## FASE 5 — Desplegar

- [ ] **5.1 Traer el código**
  ```bash
  cd /opt/readmail.com
  git fetch --all
  git checkout refactor/arquitectura-base   # o el tag de release
  pip install -r requirements.txt
  ```

- [ ] **5.2 Arrancar**
  ```bash
  sudo systemctl restart readmail
  sudo journalctl -u readmail -f
  ```

- [ ] **5.3 Verificar en el arranque** (los 5 puntos):
  - [ ] `🧪 MODO PRUEBAS ACTIVO: ... Bandeja de entrada IGNORADA.`
  - [ ] `✅ Autenticado como: <la cuenta correcta>`
  - [ ] `📄 Administrativas: X NIT, Y nombres` con X > 0
  - [ ] `📄 CajaMenor / Clientes / Terceros` con números > 0
  - [ ] `👂 Escuchando Pub/Sub`

- [ ] **5.4 Prueba de humo**: etiquetar un correo de prueba y confirmar que se procesa.

---

## FASE 6 — Los 28 días

- [ ] **6.1** Definir quién etiqueta los correos de prueba y con qué criterio.
- [ ] **6.2** Revisar los logs a diario los primeros 3 días, luego 2 veces por semana.
- [ ] **6.3** Llevar registro de hallazgos: qué se clasificó mal y por qué.
- [ ] **6.4** Entrenar a los proveedores: documentos en **PDF con texto seleccionable**, nombrados de forma clara (`orden de compra.pdf`, `ok compras.pdf`), con la frase **"OK de compras"**.
- [ ] **6.5** Revisar los reenvíos a Compras: ¿son razonables o hay ruido?
- [ ] **6.6** Contar los `[SIMULACIÓN] Entidad no registrada` de tipo FE → decide si conectar A1.

---

## ROLLBACK (si algo sale mal)

```bash
sudo systemctl stop readmail
cd /opt/readmail.com
git checkout v1.0-estable
sudo systemctl start readmail
sudo journalctl -u readmail -f
```
El estado local es compatible hacia atrás. Los correos ya respondidos no se re-responden.

⚠️ **Ojo:** tras el rollback, los tokens reautorizados con el scope de escritura siguen funcionando (tienen más permisos de los que la versión vieja pide). No hay que revertir la autorización.

---

## Riesgos conocidos a vigilar durante la prueba

| Riesgo | Señal | Acción |
|---|---|---|
| El watch de century-media quedó apuntando a "pruebas" | Producción no procesa INBOX | Se corrige al arrancar el servicio con el `.env` correcto |
| Extracción de cliente rota en el formato de orden | Muchas facturas a REVISIÓN MANUAL por "cliente no identificado" | Arreglar la extracción (Fase 2.2) |
| PDFs escaneados sin texto | Rechazos falsos por "falta OK/orden" | Entrenamiento de proveedores |
| Reenvíos masivos a Compras | Compras recibe un correo por factura | Revisar el detector del OK |
