from reademail import build_forward_body


def test_build_forward_body_incluye_rechazo_y_correo_original():
    result = build_forward_body(
        rejection_text="La factura fue rechazada por falta de orden de compra.",
        from_header="Proveedor Ejemplo <proveedor@example.com>",
        fecha="Tue, 21 Jul 2026 10:30:00 -0500",
        asunto="Factura FV-123",
        para="facturas@example.com",
        body_text="Buenos días, adjunto la factura solicitada.",
    )

    assert result.startswith("La factura fue rechazada por falta de orden de compra.")
    assert "---------- Mensaje original ----------" in result
    assert "De: Proveedor Ejemplo <proveedor@example.com>" in result
    assert "Fecha: Tue, 21 Jul 2026 10:30:00 -0500" in result
    assert "Asunto: Factura FV-123" in result
    assert "Para: facturas@example.com" in result
    assert "Buenos días, adjunto la factura solicitada." in result


def test_build_forward_body_sin_texto_incluye_aviso():
    result = build_forward_body(
        rejection_text="Rechazo de prueba",
        from_header="proveedor@example.com",
        fecha="Tue, 21 Jul 2026 10:30:00 -0500",
        asunto="Factura sin cuerpo",
        para="facturas@example.com",
        body_text="",
    )

    assert "(el correo original no tenía texto)" in result
