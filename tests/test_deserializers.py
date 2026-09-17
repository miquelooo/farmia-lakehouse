"""Tests del wire format Confluent (expr puro, sin Spark)."""
from __future__ import annotations

from farmia_ingest.readers.deserializers import wire_format_strip_expr


def test_wire_format_strip_default_column():
    # Salta los 5 bytes de cabecera: empieza en el byte 6 y toma (longitud - 5) bytes.
    assert wire_format_strip_expr() == "substring(value, 6, length(value)-5)"


def test_wire_format_strip_custom_column():
    assert wire_format_strip_expr("payload") == "substring(payload, 6, length(payload)-5)"
