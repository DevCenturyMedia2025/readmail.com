# ReadMail — Flujo objetivo

> Este documento describe **el comportamiento real del sistema**, verificado
> ejecutando el código con escenarios sintéticos. Donde el código y la
> especificación original diferían, se documentó el código: las diferencias
> resultaron ser decisiones correctas y quedan explicadas en su sección.
> Sirve como referencia de negocio y como base para auditar cambios futuros.
> Actualizado: 24 de agosto de 2026

## Orden de evaluación

Cada correo se evalúa en este orden exacto. El primer filtro que se cumple decide y termina el flujo.

| # | Filtro | Condición | Resultado |
|---|---|---|---|
| 0 | **Dedupe** | El mensaje ya fue procesado o respondido | Se ignora (sin efectos) |
| 1 | **Rebote** | Remitente `mailer-daemon`/`postmaster`, asunto de fallo, o `multipart/report` | REVISIÓN MANUAL (el rebote y la factura original) + alerta WhatsApp |
| 2 | **Antigüedad** | Más de `MAX_DIAS_ANTIGUEDAD` días — **no aplica en `MODO_PRUEBAS`** (ver nota abajo) | REVISIÓN MANUAL |
| 3 | **Sin remitente** | No se puede extraer el correo del remitente | Se descarta, sin respuesta |
| 4 | **Sin adjuntos** | `ONLY_WITH_ATTACHMENTS=true` y no hay adjuntos | Se ignora |
| 5 | **Administrativa** | NIT o nombre del asunto está en `Administrativas` o `CajaMenor` | ADMINISTRATIVA |
| 6a | **Nota de crédito/débito — por texto del correo** | Texto en asunto, cuerpo o snippet | NOTA DE CRÉDITO |
| 7 | **Tipo** | ¿Trae XML? | Sí → §A · No → §B |
| 8 | **Paquete ilegible** *(solo §A)* | Hubo errores al abrir un ZIP **y** el correo trae XML | REVISIÓN MANUAL, sin responder |
| 9 | **Sin PDF** | Ningún PDF entre los adjuntos (tras abrir ZIP) | REVISIÓN MANUAL |
| 10a | **Nota de crédito/débito — por nombre de PDF** | El nombre de algún PDF declara la nota | NOTA DE CRÉDITO |
| 10b | **Nota de crédito/débito — por texto de PDF** | El texto extraído de algún PDF declara la nota | NOTA DE CRÉDITO |

### Nota sobre el filtro 2 (antigüedad) y `MODO_PRUEBAS`

El filtro de antigüedad **queda desactivado mientras `MODO_PRUEBAS=true`**. Es
deliberado: en pruebas se etiquetan a propósito correos viejos ya archivados
para ejercitar el flujo, y el filtro los desviaría a REVISIÓN MANUAL antes de
llegar a la lógica que se quiere probar. En modo real el filtro sí aplica.

### Nota sobre las tres barreras de nota de crédito/débito

La detección **no ocurre en un solo punto del flujo**. Está partida en tres
barreras y solo la primera se evalúa antes de abrir el paquete:

| Barrera | Fuente | Posición |
|---|---|---|
| 6a | Asunto, cuerpo y snippet del correo | **Antes** de determinar el tipo, del chequeo de ZIP y del chequeo de PDF |
| 10a | Nombre de los PDF | **Después** del chequeo de ZIP y de PDF |
| 10b | Texto extraído de los PDF | **Después** del chequeo de ZIP y de PDF |

**Consecuencia:** si el ZIP viene ilegible y la nota **solo** puede reconocerse
por el PDF (no por el texto del correo), el filtro 8 corta antes y el correo
termina en **REVISIÓN MANUAL**, no en NOTA DE CRÉDITO. Es deliberado: si el
paquete no se pudo abrir, el conjunto de PDF disponible es incompleto y no es
base confiable para clasificar. Una persona revisa el caso.

---

## §A — Factura electrónica (trae XML)

| # | Condición | Resultado |
|---|---|---|
| A1 | La entidad **no está** en `Clientes` ni `Terceros` | ⚠️ **No conectado.** Hoy solo escribe un log `[SIMULACIÓN]` y el correo sigue al flujo normal. La ruta prevista es REVISIÓN MANUAL, sin responder |
| A2 | No se identifica el cliente | REVISIÓN MANUAL, sin responder |
| A3 | Trae los documentos PDF de **orden de compra** y **OK de compras** | APROBADO + respuesta al proveedor |
| A4 | Falta orden y/o OK — **modo real** | RECHAZADO + respuesta indicando qué falta |
| A5 | Falta orden y/o OK — **modo pruebas** (requiere `MODO_PRUEBAS=true` **y** `COMPRAS_EMAIL` definido) | Reenviar a `COMPRAS_EMAIL` con adjuntos y cita del original; sin responder al proveedor; sin etiqueta de estado; el correo queda en bandeja **sin leer** |

**No son motivos de rechazo:** cantidad mínima de PDF, ni ausencia de otros documentos.

**El ZIP ilegible NO es motivo de rechazo en §A.** Se resuelve antes, en el
filtro 8, enviando el correo a REVISIÓN MANUAL sin responder. El proveedor
nunca recibe un motivo falso del tipo "no enviaste la orden" cuando en realidad
sí la envió dentro de un ZIP que no se pudo abrir. El detalle del error queda
en el log.

### ⚠️ Condición obligatoria de A5

> **A5 requiere las dos condiciones a la vez: `MODO_PRUEBAS=true` y `COMPRAS_EMAIL` con un valor.**
>
> **Si `COMPRAS_EMAIL` está vacío, el reenvío NUNCA ocurre y la factura se
> rechaza normalmente, respondiéndole al proveedor.** El sistema registra un
> `WARNING` en el log y cae a la ruta A4. No hay error de arranque ni alerta:
> el único rastro es esa línea de log, así que conviene verificar la variable
> antes de dar por activo el modo pruebas.

Si el reenvío a Compras falla, el correo queda marcado como procesado (no se
reintenta) y se emite una alerta de WhatsApp. La alerta tiene un cooldown
compartido por área, no por factura: varios fallos seguidos dentro de la
ventana de cooldown generan una sola notificación.

---

## §B — Cuenta de cobro (no trae XML)

| # | Condición | Resultado |
|---|---|---|
| B1 | **Ningún PDF se declara "cuenta de cobro"** | REVISIÓN MANUAL, sin responder *(último filtro)* |
| B2 | Paquete completo: cuenta de cobro, cédula, RUT, certificado bancario, orden de compra | APROBADO + respuesta |
| B3 | Paquete incompleto | RECHAZADO + respuesta indicando qué falta |
| B4 | **ZIP ilegible** | RECHAZADO + respuesta; el error del ZIP se suma a los motivos |

**La regla de entidad registrada (A1) NO aplica a cuentas de cobro**, porque llegan sin NIT ni nombre en el asunto.

### El ZIP ilegible se trata distinto en §A y en §B

Es una asimetría **deliberada**, no un descuido:

| Rama | ZIP ilegible | Se responde al proveedor |
|---|---|---|
| §A factura electrónica | REVISIÓN MANUAL (filtro 8, antes de evaluar nada más) | No |
| §B cuenta de cobro | RECHAZADO, con el error del ZIP como motivo | Sí |

La razón es de quién puede resolver el problema. En §A la aprobación depende de
documentos internos (orden y OK de compras) que el proveedor no controla, así
que pedirle que reenvíe no sirve de nada: quien debe mirar el caso es alguien
de la casa. En §B, en cambio, el paquete entero lo arma el proveedor, y un ZIP
dañado es exactamente el tipo de problema que él puede corregir reenviando el
archivo. Por eso ahí sí se le responde, y el error del ZIP viaja en la lista de
motivos junto con los documentos faltantes.

Consecuencia práctica: una cuenta de cobro **con los 5 documentos completos**
pero con un ZIP dañado se **RECHAZA**, no se aprueba.

---

## Reglas transversales

1. **Una sola etiqueta de estado por correo.** Cada etiquetado añade la etiqueta nueva y quita todas las demás, así que el correo nunca queda con dos estados a la vez. En el desvío de rechazos a buzón interno se etiqueta dos veces (primero RECHAZADOS, luego REVISIÓN MANUAL); el estado final es uno solo.
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
| **OK de compras** | Adjunto cuyo nombre o texto contiene "ok compras", "ok de compras", "aprobado compras", "aprobado por compras", "aprobado de compras", "aprobación de compras", "aprobación compras", "visto bueno compras", "visto bueno de compras", "vobo compras", "vobo de compras", "autorizado por compras" o "aprobada compras". También: "aprobado para radicar", "autorizado para radicar", "cuenta con visto bueno", "recibida a satisfacción" y "visto bueno para radicación". | Una negación anterior dentro de la misma cláusula, sin cruzar `.`, `!`, `?`, `;`, `:`, salto de línea, "pero" o "aunque". Después del término solo vetan estados pendientes inmediatos y sin coma intermedia. |
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
| F2 | La nota de débito no se detecta; sigue al flujo de factura. | 🟠 Media | ✅ Corregida en `5551789` |
| F3 | "pendiente ok compras" se cuenta como OK aprobado. | 🟠 Media | ✅ Corregida: la negación se evalúa dentro de la misma frase |
| F4 | La entidad administrativa solo se detecta por el asunto. | 🟡 Baja | Abierta: El asunto trae NIT en los correos de plataformas DIAN, pero el filtro 5 aplica a todos los correos; una entidad administrativa con asunto libre y NIT solo en el cuerpo se escapa (verificado). |
| F5 | Una cuenta de cobro de una entidad desconocida puede aprobarse (A1 no aplica a §B). | 🟡 Baja | Decisión tomada |

---

## Estado de implementación

Verificado ejecutando el código con escenarios sintéticos el 24 de agosto de 2026.

| Sección | Estado |
|---|---|
| Filtro 0 (dedupe) | ✅ Implementado |
| Filtro 1 (rebote) | ✅ Implementado |
| Filtro 2 (antigüedad) | ✅ Implementado — inactivo mientras `MODO_PRUEBAS=true`, por diseño |
| Filtros 3, 4 (sin remitente, sin adjuntos) | ✅ Implementado |
| Filtro 5 (administrativa) | ✅ Implementado |
| Filtro 6a (nota por texto del correo) | ✅ Implementado |
| Filtro 7 (tipo por presencia de XML) | ✅ Implementado |
| Filtro 8 (paquete ilegible, solo §A) | ✅ Implementado |
| Filtro 9 (sin PDF) | ✅ Implementado |
| Filtros 10a, 10b (nota por nombre y texto de PDF) | ✅ Implementado |
| §A1 (entidad no registrada) | ⚠️ **No conectado.** Solo emite un log `[SIMULACIÓN]`; la ruta no cambia |
| §A2 (cliente no identificado → revisión manual) | ✅ Implementado |
| §A3 (orden + OK → aprobado) | ✅ Implementado |
| §A4 (rechazo solo por orden/OK) | ✅ Implementado |
| §A5 (reenvío a Compras) | ✅ Implementado — **inactivo hasta definir `COMPRAS_EMAIL`** |
| §B1 (último filtro de cuenta de cobro) | ✅ Implementado |
| §B2, B3 | ✅ Implementado |
| §B4 (ZIP ilegible como motivo) | ✅ Implementado |
| Reglas transversales 1-6 | ✅ Implementadas |
| Fugas F1, F2, F3 | ✅ Corregidas |
| Fuga F4 | 🟡 Abierta |
| Fuga F5 | Decisión tomada, sin acción |

### Configuración requerida fuera del repositorio

| Variable | Efecto si falta |
|---|---|
| `COMPRAS_EMAIL` | §A5 nunca se ejecuta; toda factura sin orden u OK se rechaza y se le responde al proveedor, incluso en modo pruebas |
