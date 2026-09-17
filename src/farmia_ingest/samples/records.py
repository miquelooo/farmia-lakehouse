"""Generadores de datos sintéticos de FarmIA.

Producen listas de dicts (registros) para cada dataset. Son **Python puro** (Faker con import
perezoso) → testeables sin Spark y **deterministas por semilla** (mismos datos en cada ejecución).
El notebook `02_generate_sample_data.py` toma estos registros y los materializa en
`landing` (varios formatos) y en Confluent (JSON/Avro).

Cada dataset se genera con su propio `random.Random(seed)`, de modo que el resultado no depende
del orden de las llamadas.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

# Instante de referencia fijo: hace los timestamps deterministas (no usamos datetime.now()).
_BASE = datetime(2026, 9, 1, 8, 0, 0, tzinfo=timezone.utc)

# Dominio compartido entre datasets para que los datos sean coherentes entre sí.
PRODUCTS = [f"PRD-{i:03d}" for i in range(1, 21)]
WAREHOUSES = ["WH-MAD", "WH-BCN", "WH-SEV"]
FIELDS = ["FLD-NORTE", "FLD-SUR", "FLD-ESTE", "FLD-OESTE"]
CHANNELS = ["web", "app", "marketplace"]
SOCIAL_PLATFORMS = ["twitter", "instagram", "tiktok"]
IMAGE_LABELS = ["healthy", "pest"]


def _rng(seed: int):
    """Devuelve (faker, random) sembrados de forma independiente y determinista."""
    from faker import Faker  # import perezoso: dependencia opcional [samples]
    import random

    fake = Faker("es_ES")
    Faker.seed(seed)
    return fake, random.Random(seed)


def _ts(rnd, max_minutes: int = 7 * 24 * 60) -> str:
    """ISO-8601 en un instante aleatorio dentro de la última semana respecto a _BASE."""
    return (_BASE - timedelta(minutes=rnd.randint(0, max_minutes))).isoformat()


# --------------------------------------------------------------------------- BATCH

def gen_sales_orders(n: int = 40, seed: int = 42) -> list[dict]:
    """Ventas online (JSON): objetos anidados (items[] + shipping_address{})."""
    fake, rnd = _rng(seed)
    orders = []
    for i in range(n):
        items = [
            {
                "product_id": rnd.choice(PRODUCTS),
                "qty": rnd.randint(1, 5),
                "unit_price": round(rnd.uniform(3.0, 90.0), 2),
            }
            for _ in range(rnd.randint(1, 3))
        ]
        total = round(sum(it["qty"] * it["unit_price"] for it in items), 2)
        orders.append(
            {
                "order_id": f"ORD-{100000 + i}",
                "customer_id": f"CUST-{rnd.randint(1, 500):04d}",
                "order_ts": _ts(rnd),
                "channel": rnd.choice(CHANNELS),
                "items": items,
                "total": total,
                "currency": "EUR",
                "shipping_address": {
                    "city": fake.city(),
                    "zip": fake.postcode(),
                    "country": "ES",
                },
            }
        )
    return orders


def gen_inventory_snapshots(n: int = 60, seed: int = 43) -> list[dict]:
    """Inventario (CSV): plano, sin anidamiento."""
    _, rnd = _rng(seed)
    rows = []
    for i in range(n):
        rows.append(
            {
                "snapshot_ts": _ts(rnd),
                "warehouse_id": rnd.choice(WAREHOUSES),
                "product_id": rnd.choice(PRODUCTS),
                "sku": f"SKU{rnd.randint(10000, 99999)}",
                "on_hand": rnd.randint(0, 500),
                "reserved": rnd.randint(0, 100),
                "reorder_point": rnd.choice([20, 50, 100]),
            }
        )
    return rows


def gen_weather_observations(n: int = 50, seed: int = 44) -> list[dict]:
    """Meteorología (Parquet): tipos numéricos variados."""
    _, rnd = _rng(seed)
    rows = []
    for _ in range(n):
        rows.append(
            {
                "station_id": f"STN-{rnd.randint(1, 8):02d}",
                "obs_ts": _ts(rnd),
                "field_id": rnd.choice(FIELDS),
                "temp_c": round(rnd.uniform(5.0, 38.0), 1),
                "humidity_pct": round(rnd.uniform(20.0, 95.0), 1),
                "precip_mm": round(rnd.uniform(0.0, 25.0), 1),
                "wind_kmh": round(rnd.uniform(0.0, 60.0), 1),
            }
        )
    return rows


def gen_logistics_shipments(n: int = 40, seed: int = 45) -> list[dict]:
    """Proveedores/logística (Avro fichero): coincide con logistics_shipments.avsc."""
    _, rnd = _rng(seed)
    carriers = ["SEUR", "MRW", "GLS", "DHL"]
    statuses = ["dispatched", "in_transit", "delivered", "delayed"]
    rows = []
    for i in range(n):
        dispatch = _BASE - timedelta(minutes=rnd.randint(0, 7 * 24 * 60))
        eta = dispatch + timedelta(hours=rnd.randint(12, 96))
        rows.append(
            {
                "shipment_id": f"SHP-{200000 + i}",
                "supplier_id": f"SUP-{rnd.randint(1, 30):03d}",
                "order_id": f"ORD-{100000 + rnd.randint(0, 39)}",
                "dispatch_ts": dispatch.isoformat(),
                "eta_ts": eta.isoformat(),
                "carrier": rnd.choice(carriers),
                "status": rnd.choice(statuses),
                "weight_kg": round(rnd.uniform(0.5, 40.0), 2),
            }
        )
    return rows


# ----------------------------------------------------------------------- STREAMING

def gen_iot_sensor_readings(n: int = 30, seed: int = 46) -> list[dict]:
    """Sensores IoT (JSON singleplex)."""
    _, rnd = _rng(seed)
    metrics = [("temperature", "C"), ("humidity", "%"), ("soil_moisture", "%")]
    out = []
    for i in range(n):
        metric, unit = rnd.choice(metrics)
        out.append(
            {
                "sensor_id": f"SNS-{rnd.randint(1, 50):03d}",
                "field_id": rnd.choice(FIELDS),
                "reading_ts": _ts(rnd, max_minutes=120),
                "metric": metric,
                "value": round(rnd.uniform(0.0, 40.0), 2),
                "unit": unit,
            }
        )
    return out


def gen_customer_app_events(n: int = 30, seed: int = 47) -> list[dict]:
    """Eventos de la app (JSON singleplex)."""
    _, rnd = _rng(seed)
    types = ["screen_view", "add_to_cart", "checkout", "search", "login"]
    screens = ["home", "catalog", "product", "cart", "profile"]
    devices = ["android", "ios"]
    out = []
    for i in range(n):
        out.append(
            {
                "event_id": f"EVT-{300000 + i}",
                "customer_id": f"CUST-{rnd.randint(1, 500):04d}",
                "event_ts": _ts(rnd, max_minutes=120),
                "event_type": rnd.choice(types),
                "screen": rnd.choice(screens),
                "device": rnd.choice(devices),
            }
        )
    return out


def gen_social_media_events(n: int = 30, seed: int = 48) -> list[dict]:
    """Redes sociales (multiplex): incluye 'platform' para enrutar a farmia.social.<platform>."""
    fake, rnd = _rng(seed)
    sentiments = ["positive", "neutral", "negative"]
    out = []
    for i in range(n):
        out.append(
            {
                "platform": rnd.choice(SOCIAL_PLATFORMS),
                "post_id": f"POST-{400000 + i}",
                "event_ts": _ts(rnd, max_minutes=240),
                "author": fake.user_name(),
                "text": fake.sentence(nb_words=8),
                "sentiment": rnd.choice(sentiments),
                "likes": rnd.randint(0, 5000),
            }
        )
    return out


def gen_order_events(n: int = 30, seed: int = 49) -> list[dict]:
    """Eventos de pedidos e-commerce (Avro + Schema Registry): coincide con el .avsc."""
    _, rnd = _rng(seed)
    statuses = ["created", "paid", "shipped", "delivered", "cancelled"]
    out = []
    for i in range(n):
        out.append(
            {
                "order_id": f"ORD-{100000 + rnd.randint(0, 39)}",
                "customer_id": f"CUST-{rnd.randint(1, 500):04d}",
                "event_ts": _ts(rnd, max_minutes=180),
                "status": rnd.choice(statuses),
                "amount": round(rnd.uniform(5.0, 300.0), 2),
                "currency": "EUR",
            }
        )
    return out


# Índice por dataset_id (útil para el notebook y para tests parametrizados).
GENERATORS = {
    "ecommerce_sales_orders": gen_sales_orders,
    "inventory_stock_snapshots": gen_inventory_snapshots,
    "weather_observations": gen_weather_observations,
    "logistics_shipments": gen_logistics_shipments,
    "iot_sensor_readings": gen_iot_sensor_readings,
    "customer_app_events": gen_customer_app_events,
    "social_media_events": gen_social_media_events,
    "ecommerce_order_events": gen_order_events,
}
