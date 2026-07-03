# Manual Técnico — Sistema de Facturación BTL (`reademail.py`)

## ¿Qué hace este programa?

Es un **robot de correo** que vigila tres cuentas de Gmail de forma continua y, cada vez que llega un correo con adjuntos de facturación, lo analiza automáticamente, lo clasifica y le responde al remitente con el resultado.

---

## Cuentas monitoreadas

| Cuenta |
|---|
| facturacion@newsapiens.com |
| facturacion@century-media.net |
| facturacion@eliteagencia.com |

---

## Flujo general (paso a paso)

```
Correo nuevo llega
        │
        ▼
1. ¿Tiene archivos adjuntos?
   └─ No → IGNORADO (no se procesa)
        │
        ▼
2. ¿El correo/asunto menciona un NIT o nombre que está en la lista blanca del Google Sheet?
   └─ Sí → Etiqueta: ADMINISTRATIVA → archiva → fin
        │
        ▼
3. ¿El texto del correo menciona "nota de crédito"?
   └─ Sí → Etiqueta: NOTA DE CRÉDITO → archiva → fin
        │
        ▼
4. Se descargan adjuntos y se abren ZIPs (incluye ZIPs dentro de ZIPs)
        │
        ▼
5. ¿Hay al menos 1 PDF?
   └─ No → Etiqueta: REVISIÓN MANUAL → fin
        │
        ▼
6. ¿El nombre o contenido de algún PDF dice "nota de crédito"?
   └─ Sí → Etiqueta: NOTA DE CRÉDITO → archiva → fin
        │
        ▼
7. ¿Hay XML adjunto?
   ├─ Sí → tipo = FACTURA ELECTRÓNICA
   └─ No  → tipo = CUENTA DE COBRO
        │
        ▼
8. Validaciones según tipo:

   FACTURA ELECTRÓNICA:
   ├─ Mínimo 3 PDF + 1 XML
   ├─ Debe existir una orden de compra (en nombre o texto del PDF)
   ├─ Se debe identificar el cliente (buscando en el catálogo del Sheet)
   └─ Debe haber "OK de compras" dentro del texto de algún PDF

   CUENTA DE COBRO:
   └─ Debe tener los 5 documentos requeridos:
      • Cuenta de cobro
      • Cédula
      • RUT
      • Certificado bancario
      • Orden de compra
        │
        ▼
9. ¿Pasó todas las validaciones?
   ├─ Sí → Etiqueta: APROBADOS → responde al remitente con confirmación
   └─ No → Etiqueta: RECHAZADOS → responde al remitente con motivos de rechazo
```

---

## Etiquetas que aplica en Gmail

| Etiqueta | Color | Cuándo se aplica |
|---|---|---|
| `ADMINISTRATIVA` | Azul | El NIT o nombre del remitente está en la lista blanca |
| `NOTA DE CRÉDITO` | Morado | El correo o los PDFs mencionan "nota de crédito" |
| `REVISIÓN MANUAL` | Amarillo | No hay PDFs en los adjuntos |
| `APROBADOS` | Verde | Pasó todas las validaciones |
| `RECHAZADOS` | Rojo | Falló alguna validación |

---

## Respuestas automáticas al remitente

### Correo de APROBADO
```
Asunto: APROBADO - facturacion recibida correctamente (ID: RAD-20260701-000001)

Confirmamos que tu correo fue recibido y validado correctamente.

ID interno: RAD-20260701-000001
Cliente: [nombre del cliente]
Clasificación: FACTURA ELECTRÓNICA / CUENTA DE COBRO
PDF detectados: 4
XML detectados: 1
```

### Correo de RECHAZADO
```
Asunto: RECHAZADO - facturacion no radicada (ID: RAD-20260701-000001)

Recibimos tu correo, pero no fue posible radicarlo.

ID interno: RAD-20260701-000001
Cliente identificado: [nombre o "No identificado"]
Clasificación detectada: FACTURA ELECTRÓNICA

Se identificaron archivos incompletos. Agradecemos revisar y confirmar
que la documentación esté completa antes de realizar el envío.
```

---

## Validación de documentos (Cuenta de Cobro)

El programa identifica cada PDF por su nombre o contenido y verifica que estén presentes los 5 tipos obligatorios:

| Tipo de documento | Cómo lo detecta |
|---|---|
| **Cuenta de cobro** | Palabras clave: "cuenta cobro", "cuenta de cobro", "cta" |
| **Cédula** | Palabras clave: "cedula de ciudadania", "documento de identidad" |
| **RUT** | Palabras clave: "registro unico tributario", "dian", "nit" |
| **Certificado bancario** | Palabras clave: "certificado bancario", "banco", "cuenta de ahorros" |
| **Orden de compra** | Palabras clave: "orden de compra", "orden no", "subtotal", "autorizado por" |

---

## Validación de documentos (Factura Electrónica)

| Validación | Requisito mínimo |
|---|---|
| Cantidad de PDFs | 3 o más |
| Cantidad de XMLs | 1 o más |
| Orden de compra | Detectada en nombre o texto de algún PDF |
| Cliente identificado | Nombre del cliente encontrado en el catálogo (Google Sheet) |
| OK de compras | Algún PDF contiene frases como: "ok compras", "visto bueno compras", "aprobado compras" |

---

## Manejo de archivos ZIP

- Soporta ZIPs adjuntos directamente al correo
- Soporta ZIPs dentro de ZIPs (hasta 2 niveles de anidamiento)
- Protecciones de seguridad:
  - Tamaño máximo ZIP: 25 MB
  - Máximo de archivos dentro del ZIP: 250
  - Tamaño total descomprimido máximo: 150 MB
  - No procesa ZIPs con contraseña
  - Bloquea rutas de escape tipo `../`

---

## Google Sheet (catálogo de clientes)

- **ID del Sheet:** `1GiU1YI4qZ1v1QbsXlFPNZdJTIzPiCPhcMGk3QAHa0IQ`
- **Rango:** `Clientes!A:Z`
- Columnas esperadas: `Cliente`, `NIT`, `Estado`
- El catálogo se refresca automáticamente cada vez que llega un correo nuevo
- Clientes con estado `activo`, `si`, `1` o `true` son considerados activos

---

## Sistema de radicado

Cada correo recibe un número de radicado único con el formato:

```
RAD-YYYYMMDD-000001
```

- Se reinicia a `000001` cada día
- Se guarda en un archivo de estado por cuenta para no repetir radicados

---

## Archivos de estado por cuenta

Cada cuenta tiene su propia carpeta dentro de `accounts/`:

```
accounts/
├── facturacion@newsapiens.com/
│   ├── token.json              ← credenciales OAuth
│   └── gmail_watch_state.json  ← historial procesado y radicados
├── facturacion@century-media.net/
│   └── ...
└── facturacion@eliteagencia.com/
    └── ...
```

---

## Cómo ejecutarlo

```powershell
cd "c:\Users\Arodriguez\Documents\GitHub\readmail.com"
.\.venv\Scripts\python.exe reademail.py
```

### Para autorizar una cuenta nueva:
```powershell
.\.venv\Scripts\python.exe reademail.py --authorize-account nueva@correo.com
```

---

## Tecnologías utilizadas

| Tecnología | Para qué se usa |
|---|---|
| Google Gmail API | Leer, etiquetar y responder correos |
| Google Pub/Sub | Notificaciones en tiempo real de correos nuevos |
| Google Sheets API | Catálogo de clientes y lista blanca de NITs |
| pypdf / PyPDF2 | Extraer texto de PDFs |
| Python-dotenv | Leer variables de entorno desde `.env` |
| OAuth2 (Google) | Autenticación segura por cuenta |

---

## Variables de entorno principales (`.env`)

| Variable | Valor actual | Descripción |
|---|---|---|
| `GCP_PROJECT_ID` | `rising-abacus-495520-u7` | Proyecto de Google Cloud |
| `PUBSUB_SUBSCRIPTION` | `gmail-watch-sub` | Suscripción Pub/Sub |
| `GMAIL_ACCOUNTS` | 3 cuentas | Cuentas a monitorear |
| `CLIENT_SHEET_ID` | ID del Sheet | Catálogo de clientes |
| `MIN_PDF_FACTURA_ELECTRONICA` | `3` | PDFs mínimos para factura |
| `MIN_PDF_CUENTA_COBRO` | `4` | PDFs mínimos para cuenta de cobro |
| `ARCHIVE_APPROVED` | `true` | Archiva correos aprobados |
| `ARCHIVE_REJECTED` | `true` | Archiva correos rechazados |
