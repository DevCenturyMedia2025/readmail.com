# ReadMail — Flujo objetivo (especificación)

> Esta es la **especificación de referencia**: cómo debe comportarse el sistema.
> Sirve para auditar el código contra ella. Lo que no esté aquí, no debe estar en el código.
> Actualizado: 12 de agosto de 2026

## Orden de evaluación

Cada correo se evalúa en este orden exacto. El primer filtro que se cumple decide y termina el flujo.

| # | Filtro | Condición | Resultado |
|---|---|---|---|
| 0 | **Dedupe** | El mensaje ya fue procesado o respondido | Se ignora (sin efectos) |
| 1 | **Rebote** | Remitente `mailer-daemon`/`postmaster`, asunto de fallo, o `multipart/report` | REVISIÓN MANUAL (el rebote y la factura original) + alerta WhatsApp |
| 2 | **Antigüedad** | Más de `MAX_DIAS_ANTIGUEDAD` días (ignorado en `MODO_PRUEBAS`) | REVISIÓN MANUAL |
| 3 | **Sin remitente** | No se puede extraer el correo del remitente | Se descarta, sin respuesta |
| 4 | **Sin adjuntos** | `ONLY_WITH_ATTACHMENTS=true` y no hay adjuntos | Se ignora |
| 5 | **Administrativa** | NIT o nombre del asunto está en `Administrativas` o `CajaMenor` | ADMINISTRATIVA |
| 6 | **Nota de crédito o débito** | Texto en asunto/cuerpo, nombre de PDF o texto de PDF | NOTA DE CRÉDITO |
| 7 | **Sin PDF** | Ningún PDF entre los adjuntos (tras abrir ZIP) | REVISIÓN MANUAL |
| 8 | **Tipo** | ¿Trae XML? | Sí → §A · No → §B |

---

## §A — Factura electrónica (trae XML)

| # | Condición | Resultado |
|---|---|---|
| A1 | La entidad **no está** en `Clientes` ni `Terceros` | REVISIÓN MANUAL, sin responder |
| A2 | No se identifica el cliente | REVISIÓN MANUAL, sin responder |
| A3 | Trae los documentos PDF de **orden de compra** y **OK de compras** | APROBADO + respuesta al proveedor |
| A4 | Falta orden y/o OK — **modo real** | RECHAZADO + respuesta indicando qué falta |
| A5 | Falta orden y/o OK — **modo pruebas** | Reenviar a `COMPRAS_EMAIL` con adjuntos; sin responder al proveedor; sin etiqueta de estado; el correo queda en bandeja **sin leer** |

**No son motivos de rechazo:** cantidad mínima de PDF, ni ausencia de otros documentos.

---

## §B — Cuenta de cobro (no trae XML)

| # | Condición | Resultado |
|---|---|---|
| B1 | **Ningún PDF se declara "cuenta de cobro"** | REVISIÓN MANUAL, sin responder *(último filtro)* |
| B2 | Paquete completo: cuenta de cobro, cédula, RUT, certificado bancario, orden de compra | APROBADO + respuesta |
| B3 | Paquete incompleto | RECHAZADO + respuesta indicando qué falta |

**La regla de entidad registrada (A1) NO aplica a cuentas de cobro**, porque llegan sin NIT ni nombre en el asunto.

---

## Reglas transversales

1. **Una sola etiqueta de estado por correo.** Nunca dos a la vez.
2. **Idempotencia.** Un correo nunca se responde dos veces. Se marca como procesado/respondido aunque el envío falle.
3. **Sin respuesta al remitente** en: REVISIÓN MANUAL, ADMINISTRATIVA, NOTA DE CRÉDITO y reenvío a Compras.
4. **Desvío de rechazos** (`ALT_RECIPIENT_ENABLED`): si el remitente es no-reply o proveedor tecnológico, el rechazo se envía al correo del emisor del XML → contacto del catálogo → buzón interno. Nunca al dominio propio.
5. **Alertas WhatsApp:** solo errores técnicos (token vencido, fallo del loop, configuración faltante, rebote). Nunca por aprobaciones ni rechazos.
6. **Modo pruebas:** solo procesa correos con la etiqueta configurada; ignora la bandeja de entrada.

---

## Definiciones de detección (precisión requerida)

| Concepto | Debe detectar | NO debe detectar |
|---|---|---|
| **Nota de crédito** | "nota de crédito", "nota credito", "credit note" | — |
| **Nota de débito** | "nota de débito", "nota debito", "debit note" | — |
| **Orden de compra** | Adjunto cuyo nombre indica "orden de compra", "orden", "OC-123" u "O.C.", o cuyo encabezado declara que es una orden | "orden de servicio", "orden de trabajo", "ordenador" o una mención narrativa dentro de la factura |
| **OK de compras** | Adjunto cuyo nombre o texto contiene "ok compras", "ok de compras", "aprobado compras", "aprobado por compras", "aprobado de compras", "aprobación de compras", "aprobación compras", "visto bueno compras", "visto bueno de compras", "vobo compras", "vobo de compras", "autorizado por compras" o "aprobada compras". También: "aprobado para radicar", "autorizado para radicar", "cuenta con visto bueno", "recibida a satisfacción" y "visto bueno para radicación". | Frases donde la aprobación está negada o pendiente. La negación se evalúa en la **oración completa**, tanto antes como después del término. |
| **Entidad administrativa** | NIT exacto o nombre como frase completa, en el **asunto** | Coincidencias parciales dentro de otra palabra o de otro NIT |

La orden y el OK se validan por **presencia del documento adjunto**, no por una
mención en el texto de la factura. Un archivo con nombre genérico y sin texto
legible no puede identificarse; por eso los adjuntos escaneados deben tener un
nombre descriptivo.

---

## Fugas conocidas a corregir

| # | Fuga | Severidad | Estado |
|---|---|---|---|
| F1 | Una mención de orden dentro de la factura podía contar como documento adjunto. | 🔴 Alta | ✅ Corregida: se valida presencia por nombre o encabezado del PDF |
| F2 | La nota de débito no se detecta; sigue al flujo de factura. | 🟠 Media | Pendiente |
| F3 | "pendiente ok compras" se cuenta como OK aprobado. | 🟠 Media | ✅ Corregida: la negación se evalúa dentro de la misma frase |
| F4 | La entidad administrativa solo se detecta por el asunto: si el asunto no trae NIT ni nombre, se escapa al flujo de factura. | 🟡 Baja | Por evaluar |
| F5 | Una cuenta de cobro de una entidad desconocida puede aprobarse (A1 no aplica a §B). | 🟡 Baja | Decisión tomada |

---

## Estado de implementación

| Sección | Estado |
|---|---|
| Filtros 0-4, 6-8 | ✅ Implementado |
| Filtro 5 (administrativa) | ✅ Implementado |
| §A1 (entidad no registrada) | ⚠️ Programado en `email_routing.py`, **no conectado** |
| §A2, A3 | ✅ Implementado |
| §A4, A5 (rechazo solo por orden/OK + reenvío a Compras) | ❌ Pendiente |
| §B1 (último filtro de cuenta de cobro) | ✅ Implementado |
| §B2, B3 | ✅ Implementado |
| Fugas F1 y F3 | ✅ Corregidas |
| Fuga F2 | ❌ Pendiente |
