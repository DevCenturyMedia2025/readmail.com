# ReadMail — Flujo objetivo

> Este documento describe **el comportamiento real del sistema**, verificado
> ejecutando el código con escenarios sintéticos. Donde el código y la
> especificación original diferían, se documentó el código: las diferencias
> resultaron ser decisiones correctas y quedan explicadas en su sección.
> Sirve como referencia de negocio y como base para auditar cambios futuros.
> Actualizado: 28 de agosto de 2026

## Orden de evaluación

Cada correo se evalúa en este orden exacto. El primer filtro que se cumple decide y termina el flujo.

| # | Filtro | Condición | Resultado |
|---|---|---|---|
| 0 | **Dedupe** | El mensaje ya fue procesado o respondido | Se ignora (sin efectos) |
| 1 | **Rebote** | Remitente `mailer-daemon`/`postmaster`, asunto de fallo, o `multipart/report` | REVISIÓN MANUAL (el rebote y la factura original) + alerta WhatsApp |
| 2 | **Antigüedad** | `LIMITE_ANTIGUEDAD_ENABLED=true` **y** más de `MAX_DIAS_ANTIGUEDAD` días — **no aplica en `MODO_PRUEBAS`** (ver nota abajo) | REVISIÓN MANUAL |
| 3 | **Sin remitente** | No se puede extraer el correo del remitente | Se descarta, sin respuesta |
| 4 | **Sin adjuntos** | `ONLY_WITH_ATTACHMENTS=true` y no hay adjuntos | Se ignora |
| 5 | **Administrativa** | NIT o nombre del asunto está en `Administrativas` o `CajaMenor` | ADMINISTRATIVA |
| 6a | **Nota de crédito — por texto del correo** | Texto en asunto, cuerpo o snippet | NOTA DE CRÉDITO |
| 6b | **Nota de débito — por texto del correo** | Texto en asunto, cuerpo o snippet | NOTA DE DÉBITO |
| 7 | **Tipo** | ¿Trae XML? | Sí → §A · No → §B |
| 8 | **Paquete ilegible** *(solo §A)* | Hubo errores al abrir un ZIP **y** el correo trae XML | REVISIÓN MANUAL, sin responder |
| 9 | **Sin PDF** | Ningún PDF entre los adjuntos (tras abrir ZIP). **Las imágenes no cuentan** (ver nota abajo) | REVISIÓN MANUAL |
| 10a | **Nota de crédito — por nombre de PDF** | El nombre de algún PDF declara la nota | NOTA DE CRÉDITO |
| 10b | **Nota de débito — por nombre de PDF** | El nombre de algún PDF declara la nota | NOTA DE DÉBITO |
| 10c | **Nota de crédito — por texto de PDF** | El texto extraído de algún PDF declara la nota | NOTA DE CRÉDITO |
| 10d | **Nota de débito — por texto de PDF** | El texto extraído de algún PDF declara la nota | NOTA DE DÉBITO |

### Nota sobre el filtro 2 (antigüedad): dos interruptores, no uno

El filtro solo actúa si se cumplen **las dos** condiciones de configuración:

| Variable | Efecto | Por qué existe |
|---|---|---|
| `LIMITE_ANTIGUEDAD_ENABLED` (por defecto `true`) | Si es `false`, el filtro no actúa **nunca**, ni siquiera en modo real | Es el interruptor de operación. Permite apagar el control para una repesca deliberada —vaciar un atraso, reprocesar un lote que quedó sin atender— sin tocar `MODO_PRUEBAS` ni el resto del flujo |
| `MODO_PRUEBAS` | Si es `true`, el filtro no actúa | En pruebas se etiquetan a propósito correos viejos ya archivados para ejercitar el flujo, y el filtro los desviaría a REVISIÓN MANUAL antes de llegar a la lógica que se quiere probar |

Son independientes y cualquiera de los dos basta para desactivar el filtro. En
operación normal (`LIMITE_ANTIGUEDAD_ENABLED=true`, `MODO_PRUEBAS=false`) el
filtro sí aplica.

**Cuidado al apagar `LIMITE_ANTIGUEDAD_ENABLED`:** con el filtro apagado, un
correo de hace meses se procesa y se responde como si fuera de hoy. El
proveedor recibe un rechazo o una aprobación por algo que quizá ya resolvió por
otra vía. Es un interruptor para usar a conciencia y volver a encender.

### Nota sobre el filtro 9: cuenta PDF, no imágenes

El filtro exige **al menos un PDF**. Las imágenes adjuntas (fotos, capturas,
escaneos guardados como JPG o PNG) no satisfacen el filtro: un correo cuyos
únicos adjuntos son imágenes va **siempre a REVISIÓN MANUAL**, aunque una de
esas imágenes fuera una foto legible de una cuenta de cobro.

Es deliberado. El PDF es el acuse de que el proveedor envió un documento y no
una foto de un documento: trae texto extraíble, conserva el nombre del archivo
como pista de clasificación y no depende de que la foto esté enfocada, derecha
y completa. Un correo que llega solo con imágenes es, casi siempre, un envío
improvisado desde un teléfono, y ese es justamente el caso en que conviene que
mire una persona antes de aprobar o rechazar.

**Consecuencia sobre el clasificador:** el clasificador de documentos sí admite
imágenes para el tipo `cuenta_cobro` (`allow_images`), y de hecho se le pasan
`pdfs + images` en §B. Pero el filtro 9 corta antes, así que esa capacidad
**solo se alcanza cuando el correo trae además al menos un PDF**. Una imagen
nunca puede ser el único documento de un paquete aprobado. No es un defecto: es
el filtro 9 haciendo de piso mínimo de calidad documental.

### Nota sobre las tres barreras de nota de crédito/débito

La detección **no ocurre en un solo punto del flujo**. Está partida en tres
barreras y solo la primera se evalúa antes de abrir el paquete:

| Barrera | Fuente | Posición |
|---|---|---|
| 6a, 6b | Asunto, cuerpo y snippet del correo | **Antes** de determinar el tipo, del chequeo de ZIP y del chequeo de PDF |
| 10a, 10b | Nombre de los PDF | **Después** del chequeo de ZIP y de PDF |
| 10c, 10d | Texto extraído de los PDF | **Después** del chequeo de ZIP y de PDF |

**Crédito y débito son estados separados**, cada uno con su etiqueta. En cada
barrera se evalúa primero el crédito y luego el débito: un correo que declarara
las dos cosas —caso raro— se archiva como **nota de crédito**, por ser el caso
frecuente. Ninguna de las dos responde al remitente.

**Consecuencia:** si el ZIP viene ilegible y la nota **solo** puede reconocerse
por el PDF (no por el texto del correo), el filtro 8 corta antes y el correo
termina en **REVISIÓN MANUAL**, no en su etiqueta de nota. Es deliberado: si el
paquete no se pudo abrir, el conjunto de PDF disponible es incompleto y no es
base confiable para clasificar. Una persona revisa el caso.

---

## §A — Factura electrónica (trae XML)

> **Los números no son el orden de evaluación.** Primero se decide orden y OK
> (A3/A4/A5); el cliente (A2) solo se mira si no hubo motivo de rechazo. Ver
> "El orden real de evaluación en §A", abajo.

| # | Condición | Resultado |
|---|---|---|
| A1 | La entidad **no está** en `Clientes` ni `Terceros` | ⚠️ **No conectado.** Hoy solo escribe un log `[SIMULACIÓN]` y el correo sigue al flujo normal. La ruta prevista es REVISIÓN MANUAL, sin responder |
| A3 | Trae los documentos PDF de **orden de compra** y **OK de compras** | APROBADO + respuesta al proveedor |
| A4 | Falta orden y/o OK — **modo real** | RECHAZADO + respuesta indicando qué falta |
| A5 | Falta orden y/o OK — **modo pruebas** (requiere `MODO_PRUEBAS=true` **y** `COMPRAS_EMAIL` definido) | Reenviar a `COMPRAS_EMAIL` con adjuntos y cita del original, en **conversación aparte**; sin responder al proveedor; la factura queda en **REVISIÓN MANUAL** y **sin leer** |
| A2 | No se identifica el cliente — **solo si no hubo motivo de rechazo**, es decir, orden y OK están presentes | REVISIÓN MANUAL, sin responder |

**No son motivos de rechazo:** cantidad mínima de PDF, ni ausencia de otros documentos.

### El orden real de evaluación en §A

La numeración A1–A5 es histórica y **no** refleja el orden en que el código
decide. El orden real es:

| Paso | Se evalúa | Si se cumple |
|---|---|---|
| 1 | ¿Están la orden de compra y el OK de compras? | **No** → A4 (rechazo con respuesta) o A5 (reenvío a Compras, en modo pruebas). Fin |
| 2 | ¿Se pudo leer el nombre del cliente? | **No** → A2 (REVISIÓN MANUAL, sin responder). Fin |
| 3 | — | A3 (APROBADO + respuesta) |

**Por qué la orden se evalúa antes que el cliente.** El nombre del cliente se
lee **de la propia orden de compra**. Si la orden no llegó, no hay de dónde
leerlo: A2 se cumpliría siempre que se cumple A4, y el correo terminaría en
REVISIÓN MANUAL en vez de rechazarse. Eso tendría dos efectos malos:

1. **El proveedor no se enteraría de que le falta la orden.** REVISIÓN MANUAL
   no responde. El proveedor quedaría esperando indefinidamente por algo que
   podía corregir él mismo en cinco minutos, y alguien de la casa tendría que
   escribirle a mano.
2. **A5 sería inalcanzable.** El reenvío a Compras existe precisamente para
   resolver la falta de orden u OK. Si el correo se desviara antes a REVISIÓN
   MANUAL por no poder leer el cliente, nunca llegaría al área que puede
   aportar el documento que falta.

Dicho de otro modo: **"no puedo leer el cliente" no es un diagnóstico útil
cuando la causa es "no llegó la orden".** El motivo accionable es la falta del
documento, y ese es el que se le comunica.

**Consecuencia verificada:** una factura sin orden, sin OK y con cliente
ilegible sale **RECHAZADA con respuesta al proveedor**, no a REVISIÓN MANUAL.
A2 queda reservado para el caso en que sí llegaron los dos documentos pero el
nombre del cliente no se pudo extraer de la orden — ahí sí el problema es
nuestro, es un caso raro que merece ojos humanos, y no se le responde al
proveedor porque él no tiene nada que corregir.

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

### El circuito de vuelta: qué pasa cuando Compras contesta

El reenvío sale como **conversación aparte**, no dentro del hilo del proveedor.
Es deliberado: si fuera en el mismo hilo, un "responder a todos" desde Compras
le enviaría al proveedor la discusión interna sobre su propia factura.

Al enviarlo se guarda en el estado el vínculo entre el hilo creado y la factura
original (`compras_forwards`), y la factura queda en **REVISIÓN MANUAL** desde
ese momento: está detenida esperando un documento interno, y así se ve en la
bandeja en vez de quedar sin marca.

### Qué se le dice a Compras

El cuerpo del reenvío lo arma `build_compras_request_text` e instruye de forma
explícita: **responder a ese mismo correo**, adjuntar el soporte en PDF con
texto seleccionable, y **no crear un correo nuevo**, con la consecuencia dicha
sin rodeos —un correo empezado desde cero no queda vinculado a nada y la
radicación se queda detenida—. Como el reconocimiento por radicado existe, el
texto también ofrece la salida: si tienen que usar otro correo, que conserven el
identificador en el asunto. El mensaje identifica además el tipo de documento
(factura electrónica o cuenta de cobro), el proveedor y qué falta.

Cuando llega la respuesta de Compras, se reconoce **antes** de entrar al flujo
de facturas, justo después del filtro de rebotes:

| Señal | Uso |
|---|---|
| El hilo del mensaje coincide con uno rastreado | Señal principal, fiable |
| El radicado aparece en el asunto | Respaldo, para cuando alguien reenvía a mano y se pierde el hilo |

Reconocida la respuesta: se etiqueta **REVISIÓN MANUAL** la respuesta de
Compras, se confirma **REVISIÓN MANUAL** en la factura original, y **no se
responde a nadie**. La conversación de Compras contiene ya los adjuntos
originales (viajaron en el reenvío) más el documento que faltaba, así que una
persona radica con el paquete completo a la vista.

**No hay aprobación automática.** Es deliberado: reabrir una factura ya marcada
como procesada tocaría la garantía de que nunca se responde dos veces. Sin este
reconocimiento, además, una respuesta de Compras que trajera el paquete entero
se habría tratado como factura nueva, aprobándose con un radicado distinto y
enviándole la confirmación a Compras en vez de al proveedor.

Si el reenvío a Compras falla, el correo queda marcado como procesado (no se
reintenta) y se emite una alerta de WhatsApp. La alerta tiene un cooldown
compartido por área, no por factura: varios fallos seguidos dentro de la
ventana de cooldown generan una sola notificación.

---

## §B — Cuenta de cobro (no trae XML)

| # | Condición | Resultado |
|---|---|---|
| B1 | **Ningún PDF se declara "cuenta de cobro"** | REVISIÓN MANUAL, sin responder *(último filtro)* |
| B2 | Paquete completo: cuenta de cobro, cédula, RUT, certificado bancario, orden de compra **y OK de compras** | APROBADO + respuesta |
| B3 | Paquete incompleto | RECHAZADO + respuesta indicando qué falta |
| B4 | **ZIP ilegible** | RECHAZADO + respuesta; el error del ZIP se suma a los motivos |
| B5 | Falta **únicamente** el OK de compras — **modo pruebas** (requiere `MODO_PRUEBAS=true` **y** `COMPRAS_EMAIL` definido) | Reenviar a `COMPRAS_EMAIL`, igual que A5: sin responder al proveedor, la factura queda en **REVISIÓN MANUAL** y **sin leer** |

**La regla de entidad registrada (A1) NO aplica a cuentas de cobro**, porque llegan sin NIT ni nombre en el asunto.

### El OK de compras también se exige en §B

La orden de compra y el OK de compras son obligatorios en **las dos ramas**: son
los documentos que autorizan el pago, y esa autorización no depende de si el
proveedor factura electrónicamente o por cuenta de cobro.

**El OK se detecta con `detect_ok_compras`, el mismo detector estricto de §A**,
no con el clasificador del paquete. Es deliberado, por dos razones:

1. **Un solo criterio.** El clasificador general reconoce el tipo
   `aprobado_compras` con una regla mucho más laxa ("aprobado" más una señal de
   apoyo), lo que aceptaría como visto bueno cualquier documento que mencione
   una aprobación. `detect_ok_compras` exige las fórmulas acordadas y descarta
   negaciones como "pendiente ok compras".
2. **Un mismo archivo puede aportar la orden y el OK.** El clasificador asigna
   **un solo tipo por archivo**: una orden de compra ya firmada por Compras se
   clasificaría como orden, y el OK saldría como faltante pese a estar ahí. Al
   evaluarlo con un detector aparte, ese caso —frecuente— se aprueba bien.

El OK se busca sobre `pdfs + images`, igual que el resto del paquete de §B.

### B5 — El reenvío a Compras también cubre la cuenta de cobro

Cuando **lo único que falta es el OK de compras**, la cuenta de cobro se reenvía
a Compras en modo pruebas, exactamente como A5. La razón es la misma que en §A:
el OK es un documento **interno**, que el proveedor no puede emitir ni
conseguir. Rechazárselo a él sería pedirle algo que no está en su mano; el
reenvío se lo pide a quien sí puede firmarlo.

**El reenvío cubre el OK y nada más.** Si además falta cualquier otro documento
—orden de compra, cédula, RUT, certificado bancario— o el ZIP viene dañado, la
cuenta de cobro se **rechaza también en modo pruebas**, con respuesta al
proveedor. Esa parte del paquete la arma él, así que el rechazo sí es
accionable. En el código la condición es literal: se reenvía solo si la lista de
motivos es exactamente `[MISSING_OK_COMPRAS_MESSAGE]`.

| Qué falta en la cuenta de cobro | Modo pruebas (con `COMPRAS_EMAIL`) | Modo real |
|---|---|---|
| Solo el OK de compras | Reenvío a Compras, sin responder, sin etiqueta, sin leer | RECHAZADO + respuesta |
| Solo la orden de compra | RECHAZADO + respuesta | RECHAZADO + respuesta |
| El OK y algo más | RECHAZADO + respuesta | RECHAZADO + respuesta |
| El OK, y además el ZIP dañado | RECHAZADO + respuesta | RECHAZADO + respuesta |

Si `COMPRAS_EMAIL` está vacío, aplica la misma advertencia de A5: el reenvío no
ocurre, queda un `WARNING` en el log y la cuenta de cobro se rechaza
respondiéndole al proveedor.

### La excepción del documento no identificado

Si falta **un** documento obligatorio, llega **exactamente un** archivo que el
sistema no logra clasificar y se reconocieron los otros cuatro, el paquete se
da por **completo con un documento no identificado** y se aprueba. La idea es
que un archivo mal nombrado suele ser justamente el que falta.

**Esa excepción solo cubre cédula, RUT y certificado bancario.** Nunca cubre la
orden de compra —queda fuera de la lista `UNKNOWN_COVERABLE_DOCS`— ni el OK de
compras, que ni siquiera pasa por el clasificador. Los soportes del proveedor
admiten el beneficio de la duda; los documentos que autorizan el pago, no.

Consecuencia verificada: una cuenta de cobro sin orden y con un archivo no
identificado se **RECHAZA**. Antes se aprobaba.

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
3. **Sin respuesta al remitente** en: REVISIÓN MANUAL, ADMINISTRATIVA, NOTA DE CRÉDITO, NOTA DE DÉBITO y reenvío a Compras.
4. **Desvío de rechazos** (`ALT_RECIPIENT_ENABLED`): si el remitente es no-reply o proveedor tecnológico, el rechazo se envía al correo del emisor del XML → contacto del catálogo → buzón interno. Nunca al dominio propio.
5. **Alertas WhatsApp:** solo errores técnicos (token vencido, fallo del loop, configuración faltante, rebote). Nunca por aprobaciones ni rechazos.
6. **Modo pruebas:** solo procesa correos con la etiqueta configurada; ignora la bandeja de entrada.

---

## Definiciones de detección (precisión requerida)

| Concepto | Debe detectar | NO debe detectar |
|---|---|---|
| **Nota de crédito** | "nota de crédito", "nota credito", "notas de credito", "credit note(s)" | "débito automático", "tarjeta débito", "nota interna" |
| **Nota de débito** | "nota de débito", "nota debito", "notas de debito", "debit note(s)" | "débito automático", "tarjeta débito", "nota interna" |
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
| F6 | Una cuenta de cobro sin OK de compras se aprobaba, y un documento no identificado podía tapar la falta de la orden. | 🟠 Media | ✅ Corregida: el OK es obligatorio en §B y la excepción solo cubre cédula, RUT y certificado bancario |

---

## Estado de implementación

Verificado ejecutando el código con escenarios sintéticos el 28 de agosto de 2026.

| Sección | Estado |
|---|---|
| Filtro 0 (dedupe) | ✅ Implementado |
| Filtro 1 (rebote) | ✅ Implementado |
| Filtro 2 (antigüedad) | ✅ Implementado — inactivo si `LIMITE_ANTIGUEDAD_ENABLED=false` **o** `MODO_PRUEBAS=true`, por diseño |
| Filtros 3, 4 (sin remitente, sin adjuntos) | ✅ Implementado |
| Filtro 5 (administrativa) | ✅ Implementado |
| Filtros 6a, 6b (nota por texto del correo) | ✅ Implementado |
| Filtro 7 (tipo por presencia de XML) | ✅ Implementado |
| Filtro 8 (paquete ilegible, solo §A) | ✅ Implementado |
| Filtro 9 (sin PDF) | ✅ Implementado — exige PDF; las imágenes no cuentan, por diseño |
| Filtros 10a-10d (nota por nombre y texto de PDF) | ✅ Implementado |
| §A1 (entidad no registrada) | ⚠️ **No conectado.** Solo emite un log `[SIMULACIÓN]`; la ruta no cambia |
| §A2 (cliente no identificado → revisión manual) | ✅ Implementado — se evalúa **después** de orden y OK, por diseño |
| §A3 (orden + OK → aprobado) | ✅ Implementado |
| §A4 (rechazo solo por orden/OK) | ✅ Implementado |
| §A5 (reenvío a Compras) | ✅ Implementado — **inactivo hasta definir `COMPRAS_EMAIL`** |
| Circuito de vuelta (respuesta de Compras → REVISIÓN MANUAL) | ✅ Implementado — por hilo, con el radicado del asunto como respaldo |
| §B1 (último filtro de cuenta de cobro) | ✅ Implementado |
| §B2, B3 | ✅ Implementado — el paquete exige además el **OK de compras**, con el detector estricto de §A |
| §B5 (reenvío a Compras por falta del OK) | ✅ Implementado — solo si el OK es lo único que falta; **inactivo hasta definir `COMPRAS_EMAIL`** |
| §B4 (ZIP ilegible como motivo) | ✅ Implementado |
| Reglas transversales 1-6 | ✅ Implementadas |
| Fugas F1, F2, F3 | ✅ Corregidas |
| Fuga F4 | 🟡 Abierta |
| Fuga F5 | Decisión tomada, sin acción |

### Configuración requerida fuera del repositorio

| Variable | Efecto si falta |
|---|---|
| `COMPRAS_EMAIL` | §A5 y §B5 nunca se ejecutan; toda factura sin orden u OK, y toda cuenta de cobro sin OK, se rechazan respondiéndole al proveedor, incluso en modo pruebas |
