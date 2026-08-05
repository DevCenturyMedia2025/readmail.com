"""
Ruteo de correos de ReadMail (funcion pura, sin conexion al monolito).

Como todos los correos llegan "como si fueran factura electronica", la ruta
NO se decide por el XML, sino VALIDANDO la entidad (por NIT o por nombre)
contra las hojas de Google Sheets:

  - En hoja Administrativas o CajaMenor  -> etiqueta administrativa
  - Registrada en Clientes o Terceros    -> flujo de factura electronica
  - No esta en ninguna hoja (entidad nueva) -> revision manual
  - Informacion insuficiente/contradictoria -> revision manual

Estas senales (is_administrativa, is_caja_menor, is_registered_entity) se
calculan aguas arriba en el lookup de Sheets. Esta funcion NO hace I/O, NO
crea clientes/proveedores y NO importa reademail.py. La traduccion
ruta -> etiqueta real (LABEL_ADMIN_NAME / LABEL_REVIEW_NAME) se hara en el
orquestador al conectar, en fase posterior.

Conserva la distincion interna ADMINISTRATIVA vs CAJA_MENOR: ambas comparten
el mismo grupo de etiqueta (ADMIN), pero la ruta las diferencia.

Precedencia (espeja el process_message actual: administrativa se evalua
ANTES que factura electronica):
  1. Informacion insuficiente/contradictoria -> REVISION_MANUAL
  2. En Administrativas/CajaMenor            -> ADMINISTRATIVA / CAJA_MENOR (grupo ADMIN)
  3. Registrada en Clientes/Terceros         -> FACTURA_ELECTRONICA (grupo FE)
  4. No esta en ninguna hoja (nueva)         -> REVISION_MANUAL
"""
from dataclasses import dataclass

# Rutas (distincion interna de negocio)
ROUTE_FACTURA_ELECTRONICA = "FACTURA_ELECTRONICA"
ROUTE_ADMINISTRATIVA = "ADMINISTRATIVA"
ROUTE_CAJA_MENOR = "CAJA_MENOR"
ROUTE_REVISION_MANUAL = "REVISION_MANUAL"

# Grupos de etiqueta de Gmail (bucket -> etiqueta real se resuelve al conectar)
LABEL_GROUP_FE = "FE"          # sigue el flujo FE; no aplica etiqueta administrativa
LABEL_GROUP_ADMIN = "ADMIN"    # -> LABEL_ADMIN_NAME
LABEL_GROUP_REVIEW = "REVIEW"  # -> LABEL_REVIEW_NAME


@dataclass(frozen=True)
class EmailRoute:
    route: str        # distincion interna: ver ROUTE_*
    label_group: str  # bucket de etiqueta: ver LABEL_GROUP_*


def classify_email_route(
    *,
    is_administrativa: bool = False,
    is_caja_menor: bool = False,
    is_registered_entity: bool = False,
    has_sufficient_info: bool = True,
) -> EmailRoute:
    """Devuelve la ruta y el grupo de etiqueta para un correo.

    Parametros (calculados aguas arriba en el lookup de Sheets; sin I/O aqui):
      is_administrativa: la entidad esta en la hoja Administrativas.
      is_caja_menor: la entidad esta en la hoja CajaMenor.
      is_registered_entity: la entidad esta en Clientes o Terceros
        (validada por NIT o por nombre).
      has_sufficient_info: False si la informacion es insuficiente o
        contradictoria; fuerza REVISION_MANUAL.
    """
    if not has_sufficient_info:
        return EmailRoute(ROUTE_REVISION_MANUAL, LABEL_GROUP_REVIEW)

    if is_administrativa or is_caja_menor:
        # Ambas comparten etiqueta (grupo ADMIN); la distincion queda en 'route'.
        if is_caja_menor and not is_administrativa:
            return EmailRoute(ROUTE_CAJA_MENOR, LABEL_GROUP_ADMIN)
        return EmailRoute(ROUTE_ADMINISTRATIVA, LABEL_GROUP_ADMIN)

    if is_registered_entity:
        return EmailRoute(ROUTE_FACTURA_ELECTRONICA, LABEL_GROUP_FE)

    # No esta en ninguna hoja: entidad nueva. No se crea nada; va a revision.
    return EmailRoute(ROUTE_REVISION_MANUAL, LABEL_GROUP_REVIEW)
