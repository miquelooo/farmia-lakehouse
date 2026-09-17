# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Ingesta batch (landing -> bronze)
# MAGIC
# MAGIC Notebook **fino**: solo instala el motor, carga la configuración y delega en `farmia_ingest`.
# MAGIC Toda la lógica vive en el paquete `farmia_ingest`. Ejecutar con cómputo **Serverless**.
# MAGIC
# MAGIC Antes: haber ejecutado `02_generate_sample_data` para poblar `landing`.

# COMMAND ----------

# MAGIC %pip install --quiet pyyaml jsonschema
# MAGIC %restart_python

# COMMAND ----------

import sys

# En despliegue real se instalaría el wheel con %pip install; en desarrollo se añade src/ al path.
# Raíz del repo: se autodetecta desde la ruta del propio notebook.
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    REPO_ROOT = "/Workspace" + _ctx.notebookPath().get().rsplit("/notebooks/", 1)[0]
except Exception:  # fallback: ajústalo a mano si hiciera falta
    REPO_ROOT = "/Workspace/Repos/miquelmolinerr@gmail.com/farmia-lakehouse"
CONF_ROOT = f"{REPO_ROOT}/conf"
if f"{REPO_ROOT}/src" not in sys.path:
    sys.path.append(f"{REPO_ROOT}/src")

from farmia_ingest.config.loader import load_environment, load_all_datasets
from farmia_ingest.core.logging import get_logger
from farmia_ingest.pipelines.batch import run_batch_dataset

env = load_environment(f"{CONF_ROOT}/environments/dev.yaml")
datasets = load_all_datasets(CONF_ROOT)
logger = get_logger()

# COMMAND ----------

# MAGIC %md ## Ingesta de todo el grupo `hourly` (5 formatos)
# MAGIC Recorre los 5 datasets batch con **aislamiento de errores**: si uno falla, los demás siguen
# MAGIC (el runner del paquete hace lo mismo). `dbutils.fs.mv` y `dbutils.fs.ls` se inyectan para archivar landing→raw.

# COMMAND ----------

from farmia_ingest.config.loader import load_group

report = []
for cfg in load_group(CONF_ROOT, "hourly"):
    try:
        run_batch_dataset(spark, env, cfg, conf_root=CONF_ROOT,
                          mover=dbutils.fs.mv, lister=dbutils.fs.ls, logger=logger)
        report.append((cfg.dataset_id, cfg.fmt, "OK"))
    except Exception as exc:  # aislamiento: un dataset roto no aborta el run completo
        report.append((cfg.dataset_id, cfg.fmt, f"ERROR: {exc}"))

display(spark.createDataFrame(report, "dataset_id string, formato string, estado string"))

# COMMAND ----------

# MAGIC %md ## Comprobaciones

# COMMAND ----------

# Todas las tablas bronze registradas en el catálogo, con sus metadatos de ingesta al final.
for cfg in load_group(CONF_ROOT, "hourly"):
    fqn = f"{env.catalog}.{cfg.target.schema}.{cfg.target.table}"
    print(f"== {fqn} ({cfg.fmt}) ==")
    display(spark.table(fqn).select("_ingest_ts", "_source_file", "_ingest_engine").limit(5))

# COMMAND ----------

# MAGIC %md ### Evolución de esquema (schema evolution)
# MAGIC Añade un fichero JSON con una columna nueva y re-ejecuta: el motor la incorpora (`mergeSchema` +
# MAGIC `schemaEvolutionMode`), sin romper. (También se prueba en `30_test_scenarios`.)

# COMMAND ----------

cfg = datasets["ecommerce_sales_orders"]
extra = f"{env.landing_uri}/{cfg.source_system}/{cfg.dataset_id}/date=2026-09-02/nuevo.json"
dbutils.fs.put(extra, '{"order_id":"ORD-999","customer_id":"CUST-0001","total":12.5,"discount_code":"PROMO10"}', overwrite=True)

# Con addNewColumns, Auto Loader PARA la query al ver la columna nueva para registrar el esquema;
# la siguiente ejecución ya la ingesta. Reintentamos para verlo en un solo run.
for intento in range(1, 4):
    try:
        run_batch_dataset(spark, env, cfg, conf_root=CONF_ROOT,
                          mover=dbutils.fs.mv, lister=dbutils.fs.ls, logger=logger)
        print(f"intento {intento}: OK")
        break
    except Exception as exc:
        print(f"intento {intento}: esquema evolucionando (columna nueva), reintentando...")

display(spark.table(f"{env.catalog}.{cfg.target.schema}.{cfg.target.table}")
        .where("order_id = 'ORD-999'"))  # debe aparecer la columna nueva discount_code
