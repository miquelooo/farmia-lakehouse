"""Tests de la lógica de metadatos y del enricher de imágenes (puro, sin Spark)."""
from __future__ import annotations

from farmia_ingest.core.metadata import (
    BATCH_METADATA_COLUMNS,
    INGEST_TS,
    SOURCE_FILE,
    source_uses_path_column,
)
from farmia_ingest.readers.enrichers import parent_folder_name


def test_binaryfile_uses_path_column():
    assert source_uses_path_column("binaryFile") is True


def test_other_formats_use_input_file_name():
    for fmt in ("json", "csv", "parquet", "avro", None):
        assert source_uses_path_column(fmt) is False


def test_metadata_columns_have_prefix_and_order():
    # Prefijo `_` (metadatos) y el timestamp/source presentes.
    assert all(c.startswith("_") for c in BATCH_METADATA_COLUMNS)
    assert INGEST_TS in BATCH_METADATA_COLUMNS and SOURCE_FILE in BATCH_METADATA_COLUMNS


def test_parent_folder_name_extracts_label():
    p = "abfss://landing@x.dfs.core.windows.net/fields/crop_field_images/healthy/img_0.png"
    assert parent_folder_name(p) == "healthy"


def test_parent_folder_name_handles_trailing_slash():
    assert parent_folder_name("a/b/c/") == "b"


def test_parent_folder_name_short_path():
    assert parent_folder_name("solo.png") is None
