"""Tests del Archiver (transformación landing->raw y movimiento, con mover falso)."""
from __future__ import annotations

from farmia_ingest.writers.archiver import Archiver


LANDING = "abfss://landing@farmiaadls.dfs.core.windows.net"
RAW = "abfss://lakehouse@farmiaadls.dfs.core.windows.net/raw"


def test_raw_for_maps_landing_to_raw():
    arch = Archiver(LANDING, RAW, mover=lambda s, d: None)
    src = f"{LANDING}/ecommerce/ecommerce_sales_orders/date=2026-09-01/file.json"
    dst = arch.raw_for(src)
    assert dst == f"{RAW}/ecommerce/ecommerce_sales_orders/date=2026-09-01/file.json"


def test_raw_for_leaves_non_landing_untouched():
    arch = Archiver(LANDING, RAW, mover=lambda s, d: None)
    other = "abfss://otro@x.dfs.core.windows.net/a/b.json"
    assert arch.raw_for(other) == other


def test_archive_calls_mover_with_expected_pairs():
    calls = []
    arch = Archiver(LANDING, RAW, mover=lambda s, d: calls.append((s, d)))
    files = [f"{LANDING}/ecommerce/x/date=1/a.json", f"{LANDING}/ecommerce/x/date=1/b.json"]
    moved = arch.archive(files)
    assert len(moved) == 2
    assert calls[0] == (files[0], f"{RAW}/ecommerce/x/date=1/a.json")


def test_archive_skips_files_outside_landing():
    calls = []
    arch = Archiver(LANDING, RAW, mover=lambda s, d: calls.append((s, d)))
    moved = arch.archive(["s3://other/a.json"])
    assert moved == [] and calls == []
