# ReadMail — Qué hace el sistema

> Documento para dirección y contabilidad. Sin tecnicismos.
> Actualizado: 12 de agosto de 2026

## En una frase

ReadMail es un programa que vigila el correo de facturación las 24 horas, clasifica cada factura que llega, verifica que traiga los documentos requeridos, y responde automáticamente aprobando o rechazando.

## Qué problema resuelve

Antes, cada correo de facturación había que abrirlo, revisar los adjuntos, verificar de quién venía, comprobar que estuviera completo y responder a mano. Con cientos de correos al mes, eso consume horas y se presta a errores.

Ahora el programa hace ese trabajo repetitivo y deja para las personas solo los casos que de verdad necesitan criterio.

## Qué decide con cada correo

El programa evalúa cada correo en este orden:

| Situación | Qué hace |
|---|---|
| Es un rebote de correo (una respuesta que no llegó) | Revisión manual + aviso |
| Tiene más de 5 días de antigüedad | Revisión manual |
| La entidad está en las listas **Administrativas** o **CajaMenor** | Etiqueta ADMINISTRATIVA |
| Es nota de crédito o de débito | Etiqueta NOTA DE CRÉDITO |
| No trae ningún PDF | Revisión manual |
| **Trae XML** → es factura electrónica | Ver abajo |
| **No trae XML** → es cuenta de cobro | Ver abajo |

### Factura electrónica

- No se identifica el cliente → **Revisión manual**
- Trae orden de compra y OK de compras → **APROBADO**
- Le falta la orden o el OK:
  - En operación real → **RECHAZADO** (se le pide al proveedor)
  - En modo pruebas → se **reenvía a Compras** para gestión interna

### Cuenta de cobro

Se exige el paquete completo: cuenta de cobro, cédula, RUT, certificado bancario y orden de compra.
- Completo → **APROBADO**
- Incompleto → **RECHAZADO**

## De dónde saca la información

El programa no inventa nada. Consulta cuatro listas en una hoja de Google:

- **Administrativas** y **CajaMenor** — entidades cuyos correos se archivan sin validar factura.
- **Clientes** y **Terceros** — clientes y proveedores registrados.

Esas listas se alimentan desde el **portal web**, donde Compras registra a cada proveedor y sube su RUT, cámara de comercio y certificación bancaria (que quedan guardados en Google Drive).

**Regla prevista (aún NO activa):** si una entidad no está en ninguna lista, su correo debería ir a revisión manual. Esta regla está diseñada y programada, pero **todavía no está conectada**: hoy una entidad no registrada sigue el flujo normal y puede llegar a aprobarse. Ver "Limitaciones actuales".

El programa nunca crea registros por su cuenta.

## Limitaciones actuales (a la fecha de este documento)

1. **El control de "entidad no registrada" no está operando.** Una factura de un proveedor que no esté en ninguna lista se valida igual que las demás: si trae la documentación completa, se aprueba y se le responde automáticamente. Conectar este control es el siguiente paso del proyecto.
2. **La lista Terceros aún no se usa.** Se lee, pero ninguna decisión depende de ella todavía.
3. **No hay lectura de documentos escaneados.** Un PDF que sea una foto o imagen no puede leerse y se dará por faltante.

## Qué depende del equipo

1. **Mantener las listas al día.** Un proveedor nuevo que no se registre en el portal hará que sus facturas caigan en revisión manual.
2. **Entrenar a los proveedores.** Deben enviar la factura, el XML, la orden de compra y el OK de compras. Y en **PDF digital, no como foto o escaneo** — el programa lee texto, no imágenes.
3. **Revisar la etiqueta REVISIÓN MANUAL.** Ahí caen los casos que el programa no puede resolver solo.

## Preguntas frecuentes

**¿Puede aprobar algo que no debía?**
Solo aprueba si encuentra todos los documentos requeridos. Si algo falta, no aprueba.

**¿Puede rechazar algo que estaba bien?**
Sí, en un caso: si los documentos vienen escaneados como imagen, el programa no puede leerlos y los da por faltantes. Por eso es importante que lleguen en PDF digital.

**¿Qué pasa si el programa se cae o el servidor se apaga?**
No se pierde ningún correo. Los mensajes quedan en Gmail y se procesan cuando el programa vuelve.

**¿Responde dos veces el mismo correo?**
No. El programa lleva memoria de lo que ya respondió.

**¿Cuándo se entera de un proveedor nuevo?**
Las listas se recargan con cada correo que llega. Lo que se registre en el portal queda disponible enseguida.

**¿Tiene acceso a otros archivos de la empresa?**
No. Solo lee la hoja de cálculo de las listas y el correo de facturación. No accede al resto de Drive ni a otros buzones.
