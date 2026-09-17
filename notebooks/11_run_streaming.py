# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — Ingesta streaming (Kafka -> bronze)
# MAGIC
# MAGIC Notebook **fino**: instala, carga config, lee las credenciales del secret scope y delega en
# MAGIC `farmia_ingest`. Levanta las 4 streaming queries del grupo `streaming`.
# MAGIC
# MAGIC Antes: haber ejecutado `02_generate_sample_data` para publicar eventos en Confluent.
# MAGIC Ejecutar con cómputo **Serverless**.

# COMMAND ----------

# MAGIC %pip install --quiet pyyaml jsonschema
# MAGIC %restart_python

# COMMAND ----------

import sys

# Raíz del repo: se autodetecta desde la ruta del propio notebook.
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    REPO_ROOT = "/Workspace" + _ctx.notebookPath().get().rsplit("/notebooks/", 1)[0]
except Exception:  # fallback: ajústalo a mano si hiciera falta
    REPO_ROOT = "/Workspace/Repos/miquelmolinerr@gmail.com/farmia-lakehouse"
CONF_ROOT = f"{REPO_ROOT}/conf"
if f"{REPO_ROOT}/src" not in sys.path:
    sys.path.append(f"{REPO_ROOT}/src")

from farmia_ingest.config.loader import load_environment, load_group
from farmia_ingest.config.models import KafkaConnection
from farmia_ingest.core.logging import get_logger
from farmia_ingest.pipelines.streaming import run_streaming_dataset

env = load_environment(f"{CONF_ROOT}/environments/dev.yaml")
logger = get_logger()

# Credenciales desde el secret scope (NUNCA en el código).
scope = env.kafka_secret_scope
conn = KafkaConnection(
    bootstrap=dbutils.secrets.get(scope, "kafka_bootstrap"),
    api_key=dbutils.secrets.get(scope, "kafka_api_key"),
    api_secret=dbutils.secrets.get(scope, "kafka_api_secret"),
    schema_registry_url=dbutils.secrets.get(scope, "schema_registry_url"),
    sr_api_key=dbutils.secrets.get(scope, "sr_api_key"),
    sr_api_secret=dbutils.secrets.get(scope, "sr_api_secret"),
)

# COMMAND ----------

# MAGIC %md ## Levantar las 4 streaming queries del grupo `streaming`
# MAGIC Para validar de forma acotada usamos `trigger(availableNow)` (procesa lo publicado y para).
# MAGIC Para modo continuo real, poner `trigger_seconds` en el YAML del dataset (p. ej. 60).

# COMMAND ----------

report = []
for cfg in load_group(CONF_ROOT, "streaming"):
    try:
        q = run_streaming_dataset(spark, env, cfg, conn, conf_root=CONF_ROOT,
                                  logger=logger, await_termination=True)
        report.append((cfg.dataset_id, "multiplex" if cfg.is_multiplex else (cfg.fmt or ""), "OK"))
    except Exception as exc:  # aislamiento: un dataset/topic caído no aborta el resto
        report.append((cfg.dataset_id, cfg.fmt or "multiplex", f"ERROR: {exc}"))

display(spark.createDataFrame(report, "dataset_id string, tipo string, estado string"))

# COMMAND ----------

# MAGIC %md ## Comprobaciones

# COMMAND ----------

# Singleplex JSON (IoT): columnas del evento + metadatos de Kafka al final.
display(spark.table(f"{env.catalog}.bronze_iot.sensor_readings").limit(10))

# COMMAND ----------

# Avro + Schema Registry (pedidos): sin campos null => el wire format (substring 5 bytes) es correcto.
display(spark.table(f"{env.catalog}.bronze_ecommerce.order_events").limit(10))

# COMMAND ----------

# Multiplex (redes): una tabla, key/value binarios, particionada por _kafka_topic.
display(spark.table(f"{env.catalog}.bronze_social.social_events")
        .groupBy("_kafka_topic").count())
