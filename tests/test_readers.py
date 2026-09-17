"""Tests de construcción de opciones de lectura (puro, sin Spark)."""
from __future__ import annotations

from farmia_ingest.config.models import BatchDatasetConfig, TargetConfig
from farmia_ingest.readers.base import autoloader_options, spark_file_options


def _cfg(fmt="json", engine="autoloader", schema_evolution_mode="addNewColumns", reader_options=None):
    return BatchDatasetConfig(
        dataset_id="d", source_system="s", fmt=fmt,
        target=TargetConfig(schema="bronze_s", table="d"),
        engine=engine, schema_evolution_mode=schema_evolution_mode,
        reader_options=reader_options or {},
    )


def test_autoloader_json_infers_when_no_schema():
    opts = autoloader_options(_cfg("json"), "sloc")
    assert opts["cloudFiles.format"] == "json"
    assert opts["cloudFiles.schemaLocation"] == "sloc"
    assert opts["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert opts["cloudFiles.inferColumnTypes"] == "true"
    assert "cloudFiles.schemaHints" not in opts


def test_autoloader_uses_schema_hints_not_full_schema():
    # El esquema se pasa como HINTS (compatible con addNewColumns), nunca con .schema().
    opts = autoloader_options(_cfg("json"), "sloc", schema_hints="order_id STRING, total DOUBLE")
    assert opts["cloudFiles.schemaHints"] == "order_id STRING, total DOUBLE"
    assert opts["cloudFiles.inferColumnTypes"] == "true"  # sigue infiriendo columnas no tipadas


def test_autoloader_parquet_never_infers():
    opts = autoloader_options(_cfg("parquet"), "sloc")
    assert "cloudFiles.inferColumnTypes" not in opts  # parquet es auto-descriptivo


def test_autoloader_binaryfile_evolution_none():
    # binaryFile tiene esquema fijo: Auto Loader no admite addNewColumns, debe ser 'none'.
    opts = autoloader_options(_cfg("binaryFile", schema_evolution_mode="addNewColumns"), "sloc")
    assert opts["cloudFiles.schemaEvolutionMode"] == "none"


def test_autoloader_merges_reader_options():
    opts = autoloader_options(_cfg("csv", reader_options={"header": "true"}), "sloc")
    assert opts["header"] == "true"


def test_spark_file_options_recursive():
    opts = spark_file_options(_cfg("json", engine="spark"))
    assert opts["recursiveFileLookup"] == "true"
