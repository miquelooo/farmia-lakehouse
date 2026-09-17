"""Tests del RunReport (aislamiento de errores). Puro, sin Spark."""
from __future__ import annotations

from farmia_ingest.orchestration.report import DatasetResult, RunReport


def _report():
    return RunReport(group="hourly", results=[
        DatasetResult("ecommerce_sales_orders", "batch", "ok", 1.2),
        DatasetResult("inventory_stock_snapshots", "batch", "error", 0.3, "YAML roto"),
        DatasetResult("weather_observations", "batch", "ok", 0.9),
    ])


def test_ok_and_errors_split():
    r = _report()
    assert {x.dataset_id for x in r.ok} == {"ecommerce_sales_orders", "weather_observations"}
    assert [x.dataset_id for x in r.errors] == ["inventory_stock_snapshots"]


def test_all_ok_false_when_any_error():
    assert _report().all_ok is False


def test_all_ok_true_when_no_errors():
    r = RunReport("g", [DatasetResult("d", "batch", "ok")])
    assert r.all_ok is True


def test_summary_line():
    assert _report().summary_line() == "grupo 'hourly': 2 OK, 1 con error"


def test_to_dict_is_serializable():
    d = _report().to_dict()
    assert d["group"] == "hourly" and d["ok"] == 2 and d["error"] == 1
    assert len(d["datasets"]) == 3 and d["datasets"][1]["error"] == "YAML roto"
    import json
    json.dumps(d)  # no lanza
