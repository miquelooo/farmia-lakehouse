# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Generación de datos sintéticos (FarmIA)
# MAGIC
# MAGIC Puebla el entorno para poder probar el motor de ingesta. **Es config-driven**: las rutas de
# MAGIC `landing` y los topics salen de `conf/` (mismos ficheros que consume el motor), así no hay
# MAGIC divergencias entre lo que se genera y lo que se ingesta.
# MAGIC
# MAGIC - **Batch → landing** (5 formatos): JSON, CSV, Parquet, Avro e imágenes (`binaryFile`).
# MAGIC - **Streaming → Confluent**: JSON singleplex, multiplex (varios topics) y Avro + Schema Registry.
# MAGIC
# MAGIC Los registros los generan las funciones puras de `farmia_ingest.samples.records` (Faker,
# MAGIC deterministas por semilla). Ejecutar con cómputo **Serverless**.

# COMMAND ----------

# MAGIC %pip install --quiet confluent-kafka fastavro faker pillow authlib httpx
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md ## 0. Configuración (REPO_ROOT se autodetecta desde la ruta del notebook)

# COMMAND ----------

import sys

# Raíz del repo: se autodetecta desde la ruta del propio notebook (Git folder o Workspace Files).
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    REPO_ROOT = "/Workspace" + _ctx.notebookPath().get().rsplit("/notebooks/", 1)[0]
except Exception:  # fallback: ajústalo a mano si hiciera falta
    REPO_ROOT = "/Workspace/Repos/miquelmolinerr@gmail.com/farmia-lakehouse"
CONF_ROOT = f"{REPO_ROOT}/conf"

# Hacer importable el paquete sin necesidad de construir/instalar el wheel para esta utilidad.
if f"{REPO_ROOT}/src" not in sys.path:
    sys.path.append(f"{REPO_ROOT}/src")

from farmia_ingest.config.loader import load_environment, load_all_datasets
from farmia_ingest.core.naming import PathBuilder
from farmia_ingest.samples import records as R

env = load_environment(f"{CONF_ROOT}/environments/dev.yaml")
datasets = load_all_datasets(CONF_ROOT)
pb = PathBuilder(env)
print("entorno:", env.catalog, "| datasets:", len(datasets))

# COMMAND ----------

# MAGIC %md ## 1. Batch → landing
# MAGIC Un fichero por "extracción" bajo `landing/<source>/<dataset>/date=.../`, siguiendo la convención.

# COMMAND ----------

import csv
import io
import json

STAMP = "20260901_0800"          # marca de generación fija (reproducible)
DATE_DIR = "date=2026-09-01"


def _landing_dir(dataset_id: str) -> str:
    return f"{pb.landing_path(datasets[dataset_id])}/{DATE_DIR}"


def write_jsonl(dataset_id: str, rows: list[dict]) -> str:
    """JSON Lines (un objeto por línea) en un fichero nombrado."""
    cfg = datasets[dataset_id]
    path = f"{_landing_dir(dataset_id)}/{cfg.source_system}_{cfg.dataset_id}_{STAMP}.json"
    dbutils.fs.put(path, "\n".join(json.dumps(r, ensure_ascii=False) for r in rows), overwrite=True)
    return path


def write_csv(dataset_id: str, rows: list[dict]) -> str:
    cfg = datasets[dataset_id]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    path = f"{_landing_dir(dataset_id)}/{cfg.source_system}_{cfg.dataset_id}_{STAMP}.csv"
    dbutils.fs.put(path, buf.getvalue(), overwrite=True)
    return path


def write_spark(dataset_id: str, rows: list[dict], fmt: str) -> str:
    """Parquet/Avro se escriben con Spark (formatos binarios). coalesce(1) = un fichero por lote."""
    path = _landing_dir(dataset_id)
    (spark.createDataFrame(rows).coalesce(1).write.mode("append").format(fmt).save(path))
    return path


# JSON (anidado), CSV (plano), Parquet, Avro
print(write_jsonl("ecommerce_sales_orders", R.gen_sales_orders()))
print(write_csv("inventory_stock_snapshots", R.gen_inventory_snapshots()))
print(write_spark("weather_observations", R.gen_weather_observations(), "parquet"))
print(write_spark("logistics_shipments", R.gen_logistics_shipments(), "avro"))

# COMMAND ----------

# MAGIC %md ### 1.b Imágenes (binaryFile) — pequeñas PNG bajo subcarpetas por etiqueta

# COMMAND ----------

from PIL import Image

_COLORS = {"healthy": (60, 160, 60), "pest": (150, 90, 40)}

# En cómputo serverless no se puede escribir en el filesystem local del driver (file:/tmp). Usamos un
# volumen de Unity Catalog como área temporal escribible y copiamos los PNG a landing (abfss).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env.catalog}.samples")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {env.catalog}.samples.tmp_images")
_TMP_VOL = f"/Volumes/{env.catalog}/samples/tmp_images"


def write_images(dataset_id: str, per_label: int = 8) -> None:
    base = pb.landing_path(datasets[dataset_id])
    for label, color in _COLORS.items():
        for i in range(per_label):
            local = f"{_TMP_VOL}/{label}_{i}.png"
            Image.new("RGB", (64, 64), color).save(local)             # imagen sintética mínima (volumen FUSE)
            dbutils.fs.cp(local, f"{base}/{label}/{label}_{i}.png")   # volumen -> landing (abfss)
            dbutils.fs.rm(local)
    print(f"imágenes generadas en {base}/<label>/")


write_images("crop_field_images")

# COMMAND ----------

# MAGIC %md ## 2. Streaming → Confluent
# MAGIC Credenciales desde el secret scope `farmia-kafka` (nunca en el código).

# COMMAND ----------

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

SCOPE = env.kafka_secret_scope
BOOTSTRAP = dbutils.secrets.get(SCOPE, "kafka_bootstrap")
API_KEY = dbutils.secrets.get(SCOPE, "kafka_api_key")
API_SECRET = dbutils.secrets.get(SCOPE, "kafka_api_secret")

KAFKA_CONF = {
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
}

# Topics a usar (los de streaming, más las plataformas del multiplex).
TOPICS = [
    "farmia.iot.sensor-readings",
    "farmia.app.customer-events",
    "farmia.ecommerce.order-events",
] + [f"farmia.social.{p}" for p in R.SOCIAL_PLATFORMS]


def ensure_topics(names: list[str]) -> None:
    admin = AdminClient(KAFKA_CONF)
    existing = set(admin.list_topics(timeout=10).topics)
    to_create = [n for n in names if n not in existing]
    if not to_create:
        print("todos los topics ya existen")
        return
    # Confluent Cloud Basic exige replication_factor=3.
    futures = admin.create_topics([NewTopic(n, num_partitions=1, replication_factor=3) for n in to_create])
    for name, fut in futures.items():
        try:
            fut.result()
            print("creado:", name)
        except Exception as exc:  # ya existe u otra condición no bloqueante
            print("aviso:", name, exc)


ensure_topics(TOPICS)

# COMMAND ----------

# MAGIC %md ### 2.a JSON singleplex (IoT y App) + multiplex (redes sociales)

# COMMAND ----------

def publish_json(topic: str, rows: list[dict], key_field: str) -> int:
    producer = Producer(KAFKA_CONF)
    for r in rows:
        producer.produce(topic, key=str(r[key_field]), value=json.dumps(r, ensure_ascii=False))
    producer.flush()
    return len(rows)


# Singleplex: un topic por dataset
print("iot:", publish_json("farmia.iot.sensor-readings", R.gen_iot_sensor_readings(), "sensor_id"))
print("app:", publish_json("farmia.app.customer-events", R.gen_customer_app_events(), "customer_id"))

# Multiplex: cada evento va a farmia.social.<platform> (el motor los une con subscribePattern)
social = R.gen_social_media_events()
producer = Producer(KAFKA_CONF)
for ev in social:
    producer.produce(f"farmia.social.{ev['platform']}", key=ev["post_id"], value=json.dumps(ev, ensure_ascii=False))
producer.flush()
print("social:", len(social), "eventos repartidos en", {e["platform"] for e in social})

# COMMAND ----------

# MAGIC %md ### 2.b Avro + Schema Registry (pedidos) — genera wire format Confluent (cabecera 5 bytes)

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

sr_client = SchemaRegistryClient({
    "url": dbutils.secrets.get(SCOPE, "schema_registry_url"),
    "basic.auth.user.info": f'{dbutils.secrets.get(SCOPE, "sr_api_key")}:{dbutils.secrets.get(SCOPE, "sr_api_secret")}',
})

with open(f"{CONF_ROOT}/schemas/ecommerce_order_events.avsc", encoding="utf-8") as fh:
    order_event_schema = fh.read()

avro_serializer = AvroSerializer(sr_client, order_event_schema, lambda obj, ctx: obj)

topic = "farmia.ecommerce.order-events"
producer = Producer(KAFKA_CONF)
order_events = R.gen_order_events()
for ev in order_events:
    producer.produce(
        topic,
        key=ev["order_id"],
        value=avro_serializer(ev, SerializationContext(topic, MessageField.VALUE)),
    )
producer.flush()
print("order-events (avro):", len(order_events), "— el value lleva la cabecera de 5 bytes del Schema Registry")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listo
# MAGIC - `landing/` poblado con los 5 formatos batch (JSON/CSV/Parquet/Avro/imágenes).
# MAGIC - Confluent con eventos JSON (IoT, App), multiplex (redes) y Avro+SR (pedidos).
# MAGIC
# MAGIC Siguiente: `10_run_batch` ingestará landing → bronze; `11_run_streaming` levantará las queries Kafka.
