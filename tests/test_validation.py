"""Tests de validación de configuración (JSON Schema). Python puro, sin Spark."""
from __future__ import annotations

import pytest

from farmia_ingest.config.validation import validate_dataset_dict
from farmia_ingest.core.errors import ValidationError


def _valid_batch() -> dict:
    return {
        "dataset_id": "ecommerce_sales_orders",
        "source_system": "ecommerce",
        "mode": "batch",
        "format": "json",
        "target": {"schema": "bronze_ecommerce", "table": "sales_orders"},
    }


def test_valid_batch_passes():
    validate_dataset_dict(_valid_batch(), source="test")  # no lanza


def test_batch_requires_format():
    raw = _valid_batch()
    del raw["format"]
    with pytest.raises(ValidationError):
        validate_dataset_dict(raw, source="test")


def test_unknown_format_rejected():
    raw = _valid_batch()
    raw["format"] = "orc"
    with pytest.raises(ValidationError):
        validate_dataset_dict(raw, source="test")


def test_dataset_id_pattern_enforced():
    raw = _valid_batch()
    raw["dataset_id"] = "Ventas Online"  # espacios/mayúsculas no permitidos
    with pytest.raises(ValidationError):
        validate_dataset_dict(raw, source="test")


def test_streaming_needs_topic_or_pattern():
    raw = {
        "dataset_id": "iot_sensor_readings",
        "source_system": "iot",
        "mode": "streaming",
        "target": {"schema": "bronze_iot", "table": "sensor_readings"},
    }
    with pytest.raises(ValidationError):
        validate_dataset_dict(raw, source="test")  # ni topic ni subscribe_pattern


def test_streaming_singleplex_ok():
    raw = {
        "dataset_id": "iot_sensor_readings",
        "source_system": "iot",
        "mode": "streaming",
        "format": "json",
        "topic": "farmia.iot.sensor-readings",
        "target": {"schema": "bronze_iot", "table": "sensor_readings"},
    }
    validate_dataset_dict(raw, source="test")  # no lanza


def test_additional_properties_rejected():
    raw = _valid_batch()
    raw["formato"] = "json"  # typo: propiedad no declarada
    with pytest.raises(ValidationError):
        validate_dataset_dict(raw, source="test")
