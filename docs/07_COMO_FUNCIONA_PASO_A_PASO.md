# Cómo funciona el sistema de radicación, paso a paso

Documento para dirección y contabilidad. Sin lenguaje técnico.
Describe lo que el programa **hace hoy**, verificado ejecutándolo.
Actualizado: 26 de agosto de 2026.

---

## 1. Qué hace, en tres frases

El programa vigila el buzón de facturación y revisa cada correo que llega de los proveedores.
Mira los documentos adjuntos, decide si la facturación está completa y le pone al correo una etiqueta de color en Gmail que dice en qué estado quedó.
Cuando corresponde, le responde al proveedor en el mismo hilo diciéndole si quedó radicado o qué le faltó.

---

## 2. El recorrido de un correo, paso a paso

Cada correo pasa por estas preguntas **en este orden**. La primera pregunta cuya respuesta obliga a una decisión cierra el caso: el correo ya no sigue avanzando.

1. **¿Ya habíamos revisado este correo antes?**
   El programa lleva una lista de los correos que ya trabajó.
   - Sí → no hace nada. Es la garantía de que nunca responde dos veces.
   - No → sigue.

2. **¿Es un aviso de que un correo nuestro no pudo entregarse?**
   Es el mensaje automático que devuelve el servidor cuando una dirección no existe.
   - Sí → marca **REVISIÓN MANUAL** ese aviso y también la factura original a la que se refiere, y envía una alerta por WhatsApp al responsable técnico. No le responde a nadie.
   - No → sigue.

3. **¿El correo es muy viejo?**
   Hay un límite de días configurado (hoy: cinco).
   - Sí → **REVISIÓN MANUAL**, sin responder.
   - No → sigue.

   *Nota:* mientras el sistema está en modo de pruebas esta pregunta se salta a propósito, porque en pruebas se usan correos antiguos ya archivados. Además existe un interruptor aparte que permite apagar este control incluso fuera de pruebas.

4. **¿Se puede saber quién envió el correo?**
   - No → se descarta en silencio, sin etiqueta y sin respuesta.
   - Sí → sigue.

5. **¿Trae archivos adjuntos?**
   - No → se ignora, sin etiqueta y sin respuesta. (Este control se puede apagar.)
   - Sí → sigue.

6. **¿Es un correo de una entidad administrativa?**
   Se compara el número de identificación tributaria o el nombre que aparece **en el asunto** contra dos listas internas (entidades administrativas y caja menor).
   - Sí → **ADMINISTRATIVA**. No se responde. Lo revisa contabilidad.
   - No → sigue.

7. **¿El correo dice que trae una nota de crédito o una nota de débito?**
   Se lee el asunto, el cuerpo y el resumen del correo.
   - Dice nota de crédito → **NOTA DE CRÉDITO**, sin responder.
   - Dice nota de débito → **NOTA DE DÉBITO**, sin responder.
   - Si un correo dijera las dos cosas, queda archivado como nota de crédito.
   - No dice ninguna → sigue.

8. **¿Qué clase de facturación es?**
   Si entre los adjuntos viene el archivo electrónico oficial de la factura, es una **factura electrónica** y va por el camino A. Si no viene, se trata como **cuenta de cobro** y va por el camino B.

9. **(Solo camino A) ¿Alguno de los archivos comprimidos venía dañado?**
   - Sí → **REVISIÓN MANUAL**, sin responder. No se le reclama nada al proveedor, porque el problema no se resuelve pidiéndole que reenvíe: alguien de la casa debe mirarlo.
   - No → sigue.

10. **¿Llegó al menos un documento en formato PDF?**
    - No → **REVISIÓN MANUAL**, sin responder.
    - Sí → sigue.

11. **¿Alguno de los documentos es una nota de crédito o de débito?**
    Ahora ya se pueden abrir los documentos, así que se mira primero el nombre del archivo y después su contenido.
    - Es nota de crédito → **NOTA DE CRÉDITO**, sin responder.
    - Es nota de débito → **NOTA DE DÉBITO**, sin responder.
    - Ninguna de las dos → sigue por su camino.

### Camino A — Factura electrónica

12. **¿La empresa que factura está en nuestras listas de clientes y terceros?**
    Hoy esta pregunta **solo se anota en el registro interno, no cambia nada**. El correo sigue su curso normal aunque la empresa no esté registrada. Está previsto que en el futuro estos casos vayan a **REVISIÓN MANUAL**, pero eso todavía no está activo.

13. **¿Vienen los dos documentos internos que autorizan el pago: la orden de compra y el visto bueno del área de compras?**
    Se exige que lleguen como documentos adjuntos. No basta con que la factura los mencione en el texto.
    - **Vienen los dos** → **APROBADOS** y se le responde al proveedor confirmando la radicación. Con una excepción: si la orden llegó pero en ella no se puede leer el nombre del cliente, el correo va a **REVISIÓN MANUAL** y no se responde.
    - **Falta alguno** → **RECHAZADOS** y se le responde al proveedor diciéndole exactamente qué faltó. Esto ocurre así aunque tampoco se haya podido leer el nombre del cliente: cuando falta la orden, el rechazo tiene prioridad.
    - **Falta alguno y el sistema está en modo de pruebas** con un buzón de compras configurado → el correo se reenvía completo al área de compras pidiendo el documento faltante, **no se le responde al proveedor**, no se le pone etiqueta de estado y el correo queda marcado como no leído en la bandeja. Si el reenvío falla, se avisa por WhatsApp al responsable técnico.

    *Importante:* si el buzón de compras no está configurado, el reenvío **nunca ocurre** y la factura se rechaza normalmente, respondiéndole al proveedor. El único rastro de esa situación queda en el registro interno del programa.

### Camino B — Cuenta de cobro

14. **¿Alguno de los documentos se identifica a sí mismo como cuenta de cobro?**
    - No → **REVISIÓN MANUAL**, sin responder.
    - Sí → sigue.

15. **¿Está el paquete completo?**
    Deben venir los cinco documentos: cuenta de cobro, cédula, RUT, certificado bancario y orden de compra.
    - **Completo y sin archivos dañados** → **APROBADOS** y se responde al proveedor.
    - **Incompleto** → **RECHAZADOS** y se responde diciendo qué documento falta.
    - **Completo pero con un archivo comprimido dañado** → también **RECHAZADOS**, y se le pide al proveedor que reenvíe. Aquí sí se le reclama, porque el paquete lo arma él y el archivo dañado sí lo puede corregir.

### Cuando hay que responder un rechazo a un remitente automático

Muchas facturas electrónicas llegan desde direcciones que no aceptan respuestas. En esos casos, si la función está activada, el rechazo se envía en cascada: primero al correo del proveedor que viene dentro del archivo electrónico de la factura; si no, al contacto que figura en nuestra lista de clientes; y si tampoco, a un buzón interno. Nunca se envía a una dirección de la propia empresa, salvo ese buzón interno de gestión manual. Cuando termina en el buzón interno, el correo además queda en **REVISIÓN MANUAL**. Si la función está desactivada, sencillamente no se responde nada.

---

## 3. Las etiquetas de Gmail

| Etiqueta | Qué significa | Qué debe hacer usted |
|---|---|---|
| **APROBADOS** | La facturación llegó completa. El proveedor ya recibió la confirmación. | Continuar con el proceso contable normal. |
| **RECHAZADOS** | Faltó algo. El proveedor ya recibió el correo diciéndole qué faltó. | Nada de inmediato. Esperar el reenvío del proveedor. |
| **REVISIÓN MANUAL** | El programa no pudo decidir con seguridad y prefirió no equivocarse. **Al proveedor no se le respondió nada.** | Abrir el correo, revisarlo a mano y resolverlo. Esta es la bandeja que hay que mirar todos los días. |
| **ADMINISTRATIVA** | Es un correo de una entidad administrativa o de caja menor, no una factura de proveedor. No se responde. | Darle el trámite administrativo que corresponda. |
| **NOTA DE CRÉDITO** | El correo trae una nota de crédito, no una factura para radicar. No se responde. | Aplicarla contablemente. |
| **NOTA DE DÉBITO** | El correo trae una nota de débito. No se responde. | Aplicarla contablemente. |
| **Sin etiqueta** | El correo se descartó (sin adjuntos, sin remitente legible) o fue reenviado al área de compras en modo de pruebas. | Si aparece como no leído en la bandeja, es un reenvío a compras en curso. |

Un correo **nunca queda con dos etiquetas de estado al mismo tiempo**: cada vez que se pone una, se quitan las demás.

---

## 4. Qué debe enviar el proveedor

Son dos casos distintos y no se mezclan.

**Si factura electrónicamente**, debe enviar en el mismo correo:

- El archivo electrónico oficial de su factura. Es lo que le indica al programa que se trata de una factura electrónica.
- La **orden de compra** que nosotros le emitimos.
- El **visto bueno del área de compras**.

Los dos últimos son los que autorizan el pago. Sin ellos no hay forma de saber si la compra fue aprobada internamente, y por eso la factura se rechaza. No sirve que la factura los mencione en su texto: tienen que llegar como archivos.

**Si trabaja por cuenta de cobro** (personas naturales, contratistas), debe enviar cinco documentos:

- La cuenta de cobro.
- La cédula.
- El RUT.
- El certificado bancario.
- La orden de compra.

Los tres del medio son los que permiten pagarle correctamente y cumplir con las obligaciones tributarias. Faltando cualquiera, el paquete se rechaza.

---

## 5. De dónde saca la información

El programa consulta unas hojas de cálculo compartidas de la empresa:

- **La lista de clientes** y **la lista de terceros**: para reconocer a qué cliente pertenece cada factura y ponerle siempre el mismo nombre.
- **La lista de entidades administrativas** y **la de caja menor**: para separar esos correos del flujo de facturas.

Además lee la información que viene dentro de los propios documentos que envía el proveedor.

**Qué pasa si esas listas están incompletas:**

- Si un cliente no está en la lista, la factura **no se frena**: si el nombre se puede leer en la orden de compra, se usa tal como aparece allí y la factura sigue su curso. Solo se detiene si no hay ningún nombre legible.
- Si una entidad administrativa no está en la lista, sus correos entran al flujo normal de facturas y muy probablemente terminen rechazados.
- Hoy hay un caso conocido sin resolver: una entidad administrativa se reconoce **solo por el asunto del correo**. Si su identificación aparece únicamente dentro del cuerpo del mensaje, el programa no la reconoce y el correo sigue como si fuera una factura de proveedor.

---

## 6. Qué NO puede hacer el programa

- **No lee documentos escaneados como foto.** Si un PDF es la imagen de una hoja y no tiene texto seleccionable, el programa no puede saber qué documento es. Si además el nombre del archivo es genérico, lo dará por ausente y rechazará la factura. Por eso los escaneos deben tener un nombre descriptivo.
- **No abre correos que llegan sin archivos adjuntos.** Si el proveedor pone un enlace de descarga en vez de adjuntar, el correo se ignora.
- **No procesa correos cuyos únicos adjuntos son fotos.** Si todo llega en formato de imagen y ningún archivo es un PDF, el correo va a REVISIÓN MANUAL.
- **No repara archivos comprimidos dañados.**
- **No verifica valores, ni cifras, ni impuestos, ni fechas de vencimiento.** No compara el monto de la factura contra el de la orden de compra. Solo verifica que los documentos requeridos estén presentes.
- **No decide si el gasto está bien hecho.** Su único criterio de aprobación es documental.
- **Todavía no bloquea a proveedores no registrados.** Está previsto, pero hoy solo lo anota internamente y el correo sigue su curso normal.

---

## 7. Preguntas frecuentes

**¿Puede aprobar algo que no debía?**
Sí, en un caso: si llegan documentos con los nombres correctos pero el contenido no corresponde. El programa verifica que los documentos *estén*, no que su contenido sea correcto. También aprueba cuentas de cobro de personas que no están en ninguna de nuestras listas, porque esos correos llegan sin identificación en el asunto. Es una decisión tomada a conciencia, no un error.

**¿Puede rechazar algo bueno?**
Sí. El caso típico es un documento escaneado como foto y con nombre genérico: el programa no puede identificarlo y lo da por faltante. Por eso todo rechazo puede apelarse: el proveedor simplemente reenvía con nombres de archivo claros.

**¿Qué pasa si el programa se apaga?**
Los correos se quedan quietos en la bandeja, sin etiqueta y sin respuesta. No se pierde nada. Cuando vuelve a encenderse, retoma desde donde iba. Los correos que quedaron atrás más de cinco días irán a REVISIÓN MANUAL en vez de procesarse automáticamente, para que una persona los mire.

**¿Responde dos veces?**
No. Lleva un registro de a qué correos ya respondió y no vuelve a hacerlo. Y el registro se actualiza **aunque el envío falle**, precisamente para que un problema de conexión nunca genere una segunda respuesta.

**¿Cuándo reconoce un proveedor nuevo?**
De inmediato, sin que nadie lo dé de alta. El programa no valida al proveedor que envía: valida los documentos que llegan. Lo que sí consulta en las listas es el **cliente** al que se le factura, y aun así, si el cliente no está en la lista pero su nombre se lee en la orden de compra, la factura pasa igual.

**¿Qué debo hacer con REVISIÓN MANUAL?**
Es la única bandeja que exige atención diaria. Todo lo que el programa no pudo decidir termina ahí, y el proveedor **no ha recibido ninguna respuesta**, así que está esperando.

**¿Recibimos alertas por WhatsApp de facturas rechazadas?**
No. Las alertas por WhatsApp son solo para problemas técnicos del programa: que se venza el permiso de acceso al correo, que falle el arranque por configuración, que un correo nuestro rebote o que falle un reenvío al área de compras. Nunca por aprobaciones ni por rechazos.
