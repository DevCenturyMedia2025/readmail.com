# ReadMail — Manual de usuario

> Cómo trabajar con el sistema en el día a día.
> Actualizado: 12 de agosto de 2026

## 1. Las etiquetas de Gmail

Cada correo procesado queda con **una sola** etiqueta de estado:

| Etiqueta | Significa | Qué hacer |
|---|---|---|
| **APROBADOS** | Factura recibida y validada | Nada. Sigue el proceso interno de radicación. |
| **RECHAZADOS** | Le faltó documentación al proveedor | Nada. Ya se le respondió pidiendo que complete. |
| **ADMINISTRATIVA** | Entidad de las listas Administrativas/CajaMenor | Se archiva por tipo, no se valida. |
| **NOTA DE CRÉDITO** | Nota de crédito | Gestión contable manual. |
| **NOTA DE DÉBITO** | Nota de débito | Gestión contable manual. |
| **REVISIÓN MANUAL** | El programa no pudo resolverlo solo | **Requiere que alguien lo mire.** Ver sección 4. |

## 2. El portal web (registro de entidades)

**Quién lo usa:** Compras.

**Para qué:** registrar clientes y proveedores, y subir sus documentos base.

**Qué se puede subir:** RUT, cámara de comercio y certificación bancaria. Se guardan en Google Drive y quedan enlazados al registro.

**Por qué importa:** el programa solo reconoce entidades que estén registradas. Un proveedor sin registrar hace que sus facturas caigan en revisión manual.

### Registrar una entidad nueva

1. Entrar al portal.
2. Elegir el tipo de registro (Clientes o Terceros/proveedores).
3. Llenar NIT, nombre y correo.
4. Subir los documentos disponibles.
5. Guardar. El registro queda ACTIVO.

El programa lo detecta con el siguiente correo que procese — no hay que reiniciar nada.

## 3. Qué debe enviar el proveedor

### Factura electrónica
- Factura en PDF
- Archivo XML
- Orden de compra
- OK de compras (visto bueno)

### Cuenta de cobro
- Cuenta de cobro
- Cédula
- RUT
- Certificado bancario
- Orden de compra

> **Importante para el entrenamiento de proveedores:** los documentos deben ir en **PDF digital**, no como fotografía ni escaneo de imagen. El programa lee el texto del documento; si es una imagen, no puede leerlo y lo dará por faltante.

## 4. Qué hacer con REVISIÓN MANUAL

> **Frecuencia de revisión: cada 3 días.** Definido con Contabilidad.
> Es importante no dejarla más tiempo: ahí caen los rebotes (proveedores que
> nunca recibieron su rechazo) y las entidades nuevas sin registrar. Cada día
> que pasa es un día que el proveedor no se entera de nada.

Un correo llega ahí por alguna de estas razones:

| Motivo | Acción sugerida |
|---|---|
| La entidad no está registrada *(previsto, aún no activo)* | Registrarla en el portal y reprocesar el correo |
| Es un rebote (la respuesta no llegó al proveedor) | Buscar el correo correcto y reenviar a mano |
| Tiene más de 5 días | Verificar si aún procede radicarla |
| No trae PDF | Pedirle al remitente que envíe la documentación |
| No se identificó el cliente | Verificar que el nombre/NIT esté bien escrito en las listas |

## 5. Modo pruebas (etiqueta "pruebas")

Sirve para probar el sistema sin tocar el flujo real.

**Cómo funciona:** el programa en modo pruebas **ignora la bandeja de entrada** y solo procesa los correos que tengan la etiqueta **pruebas** puesta a mano.

**Cómo probar un correo:**
1. Verificar que el programa esté corriendo en modo pruebas (al arrancar muestra un aviso de MODO PRUEBAS ACTIVO).
2. Abrir el correo en Gmail.
3. Ponerle la etiqueta **pruebas**.
4. Observar la consola del programa.

**Diferencia de comportamiento en pruebas:** cuando a una factura electrónica le falta la orden o el OK de compras, en vez de rechazar al proveedor, el correo se **reenvía a Compras** pidiendo el archivo, y queda en la bandeja sin leer.

**Ojo:** un correo que el programa ya procesó no se vuelve a procesar aunque le quiten y pongan la etiqueta. Para probar de nuevo, usar un correo distinto.

## 6. Alertas por WhatsApp

El sistema envía WhatsApp **solo cuando hay un error técnico**, no por rechazos ni aprobaciones (eso es funcionamiento normal). Los avisos que pueden llegar:

- Token de una cuenta vencido (requiere reactivación)
- Error en el proceso de escucha de correos
- Falta configuración para arrancar
- Un rechazo rebotó y no le llegó al proveedor

Si llega una alerta de **token vencido**, hay que reautorizar esa cuenta (ver manual técnico).

## 7. Preguntas rápidas

**Agregué un proveedor y su factura igual cayó en revisión manual.**
Puede que la factura haya llegado antes de que el programa recargara las listas. Con el siguiente correo ya queda registrado.

**El proveedor dice que mandó todo pero se rechazó.**
Lo más probable es que los documentos vengan escaneados como imagen. Verificar que sean PDF digital.

**¿Puedo cambiar los textos de los correos de respuesta?**
Sí, pero es un cambio de código. Solicitarlo al responsable técnico.
