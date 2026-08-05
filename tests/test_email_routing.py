"""Pruebas de la funcion pura classify_email_route (sin I/O, sin monolito)."""
from app.services.email_routing import (
    classify_email_route,
    ROUTE_FACTURA_ELECTRONICA,
    ROUTE_ADMINISTRATIVA,
    ROUTE_CAJA_MENOR,
    ROUTE_REVISION_MANUAL,
    LABEL_GROUP_FE,
    LABEL_GROUP_ADMIN,
    LABEL_GROUP_REVIEW,
)


def test_administrativa_va_a_grupo_admin():
    r = classify_email_route(is_administrativa=True)
    assert r.route == ROUTE_ADMINISTRATIVA
    assert r.label_group == LABEL_GROUP_ADMIN


def test_caja_menor_conserva_distincion_pero_misma_etiqueta():
    r = classify_email_route(is_caja_menor=True)
    assert r.route == ROUTE_CAJA_MENOR          # distincion interna
    assert r.label_group == LABEL_GROUP_ADMIN   # misma etiqueta que administrativa


def test_admin_tiene_precedencia_sobre_caja():
    r = classify_email_route(is_administrativa=True, is_caja_menor=True)
    assert r.route == ROUTE_ADMINISTRATIVA
    assert r.label_group == LABEL_GROUP_ADMIN


def test_registrada_en_clientes_o_terceros_va_a_factura_electronica():
    r = classify_email_route(is_registered_entity=True)
    assert r.route == ROUTE_FACTURA_ELECTRONICA
    assert r.label_group == LABEL_GROUP_FE


def test_admin_tiene_precedencia_sobre_registrada():
    r = classify_email_route(is_administrativa=True, is_registered_entity=True)
    assert r.route == ROUTE_ADMINISTRATIVA
    assert r.label_group == LABEL_GROUP_ADMIN


def test_entidad_nueva_no_en_ninguna_hoja_va_a_revision_manual():
    r = classify_email_route()  # todo en False: no esta en ninguna hoja
    assert r.route == ROUTE_REVISION_MANUAL
    assert r.label_group == LABEL_GROUP_REVIEW


def test_info_insuficiente_fuerza_revision_manual():
    # aunque este registrada, si la info es insuficiente/contradictoria -> revision
    r = classify_email_route(is_registered_entity=True, has_sufficient_info=False)
    assert r.route == ROUTE_REVISION_MANUAL
    assert r.label_group == LABEL_GROUP_REVIEW
