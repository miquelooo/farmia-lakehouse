"""Tests del PathBuilder. Python puro, sin Spark."""
from __future__ import annotations

from farmia_ingest.config.models import (
    BatchDatasetConfig,
    EnvironmentConfig,
    StreamingDatasetConfig,
    TargetConfig,
)
from farmia_ingest.core.naming import PathBuilder


def _env() -> EnvironmentConfig:
    return EnvironmentConfig(
        catalog="farmia",
        landing_uri="abfss://landing@farmiaadls.dfs.core.windows.net",
        lakehouse_uri="abfss://lakehouse@farmiaadls.dfs.core.windows.net",
    )


def _batch() -> BatchDatasetConfig:
    return BatchDatasetConfig(
        dataset_id="ecommerce_sales_orders",
        source_system="ecommerce",
        fmt="json",
        target=TargetConfig(schema="bronze_ecommerce", table="sales_orders"),
    )


def test_landing_and_raw_paths():
    pb = PathBuilder(_env())
    cfg = _batch()
    assert pb.landing_path(cfg) == (
        "abfss://landing@farmiaadls.dfs.core.windows.net/ecommerce/ecommerce_sales_orders"
    )
    assert pb.raw_path(cfg) == (
        "abfss://lakehouse@farmiaadls.dfs.core.windows.net/raw/ecommerce/ecommerce_sales_orders"
    )


def test_bronze_and_checkpoint_and_schema_paths():
    pb = PathBuilder(_env())
    cfg = _batch()
    assert pb.bronze_path(cfg).endswith("/bronze/bronze_ecommerce/sales_orders")
    assert pb.checkpoint_path(cfg).endswith("/_checkpoints/ecommerce_sales_orders")
    assert pb.schema_location(cfg).endswith("/_schemas/ecommerce_sales_orders")


def test_no_double_slashes():
    pb = PathBuilder(_env())
    # el esquema abfss:// debe conservar sus dos barras, pero no debe haber '//' extra
    path = pb.bronze_path(_batch())
    assert "abfss://" in path
    assert "//" not in path.split("abfss://", 1)[1]


def test_table_name_fully_qualified():
    pb = PathBuilder(_env())
    assert pb.table_name(_batch()) == "farmia.bronze_ecommerce.sales_orders"


def test_streaming_bronze_path():
    pb = PathBuilder(_env())
    cfg = StreamingDatasetConfig(
        dataset_id="iot_sensor_readings",
        source_system="iot",
        target=TargetConfig(schema="bronze_iot", table="sensor_readings"),
        topic="farmia.iot.sensor-readings",
        fmt="json",
    )
    assert pb.bronze_path(cfg).endswith("/bronze/bronze_iot/sensor_readings")
    assert pb.checkpoint_path(cfg).endswith("/_checkpoints/iot_sensor_readings")
