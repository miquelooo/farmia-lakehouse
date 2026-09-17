"""Tests de los generadores de datos sintéticos (Python puro + Faker, sin Spark)."""
from __future__ import annotations

import pytest

from farmia_ingest.samples import records as R


def test_all_datasets_have_generator():
    # 8 datasets con datos generables (crop_field_images son binarios, van aparte en el notebook).
    assert set(R.GENERATORS) == {
        "ecommerce_sales_orders",
        "inventory_stock_snapshots",
        "weather_observations",
        "logistics_shipments",
        "iot_sensor_readings",
        "customer_app_events",
        "social_media_events",
        "ecommerce_order_events",
    }


@pytest.mark.parametrize("dataset_id, gen", R.GENERATORS.items())
def test_generator_returns_requested_count(dataset_id, gen):
    rows = gen(n=7, seed=1)
    assert len(rows) == 7
    assert all(isinstance(r, dict) for r in rows)


def test_generators_are_deterministic():
    # Misma semilla -> mismo resultado (reproducibilidad).
    assert R.gen_sales_orders(n=5, seed=99) == R.gen_sales_orders(n=5, seed=99)


def test_different_seed_changes_output():
    assert R.gen_sales_orders(n=5, seed=1) != R.gen_sales_orders(n=5, seed=2)


def test_sales_orders_have_nested_structures():
    order = R.gen_sales_orders(n=1, seed=5)[0]
    assert {"order_id", "items", "shipping_address", "total"} <= order.keys()
    assert isinstance(order["items"], list) and order["items"]
    assert {"product_id", "qty", "unit_price"} <= order["items"][0].keys()
    assert {"city", "zip", "country"} <= order["shipping_address"].keys()


def test_order_events_match_avro_fields():
    ev = R.gen_order_events(n=1, seed=3)[0]
    assert set(ev.keys()) == {"order_id", "customer_id", "event_ts", "status", "amount", "currency"}


def test_social_events_have_platform_for_routing():
    events = R.gen_social_media_events(n=10, seed=7)
    assert all(e["platform"] in R.SOCIAL_PLATFORMS for e in events)
