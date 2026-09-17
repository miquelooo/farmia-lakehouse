# Databricks notebook source
# MAGIC %md
# MAGIC # 30 — Escenarios "raros" (robustez del motor)
# MAGIC
# MAGIC Comprueba el comportamiento del motor ante situaciones que salen mal. Aquí se verifican:
# MAGIC 1. **Aislamiento de errores**: un dataset roto no aborta el grupo (RunReport).
# MAGIC 2. **Idempotencia / checkpoint**: re-ejecutar batch no reingesta lo ya procesado.
# MAGIC 3. **Evolución de esquema**: un fichero con una columna nueva se incorpora.
# MAGIC 4. **Topic vacío**: una query streaming sin mensajes no rompe (0 filas).
# MAGIC
# MAGIC Ejecutar tras `02_generate_sample_data`. Cómputo **Serverless**.

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

from farmia_ingest.config.loader import load_environment
from farmia_ingest.core.logging import get_logger
from farmia_ingest.orchestration.runner import run_group
from farmia_ingest.pipelines.batch import run_batch_dataset

env = load_environment(f"{CONF_ROOT}/environments/dev.yaml")
logger = get_logger()

# COMMAND ----------

# MAGIC %md ## 1. Resiliencia ante datos corruptos (aislamiento en el runner)
# MAGIC Metemos un fichero JSON **corrupto** en un dataset y ejecutamos todo el grupo `hourly`.
# MAGIC Auto Loader **tolera** el registro malformado (lo rescata en `_rescued_data`), así que el grupo
# MAGIC termina en OK. El runner **aísla** errores por dataset: si un dataset fallara de forma
# MAGIC irrecuperable saldría ERROR y el resto seguiría (ver `orchestration/runner.py`).

# COMMAND ----------

bad = f"{env.landing_uri}/ecommerce/ecommerce_sales_orders/date=2026-09-03/roto.json"
dbutils.fs.put(bad, "{esto no es json valido ,,,", overwrite=True)

report, _ = run_group(spark, env, CONF_ROOT, "hourly",
                      mover=dbutils.fs.mv, lister=dbutils.fs.ls, logger=logger)
print(report.summary_line())
display(spark.createDataFrame(
    [(r.dataset_id, r.status, (r.error or "")[:80]) for r in report.results],
    "dataset_id string, estado string, error string"))

# COMMAND ----------

# MAGIC %md ## 2. Idempotencia / checkpoint
# MAGIC Re-ejecutar el mismo dataset no vuelve a ingestar lo ya procesado (Auto Loader lo recuerda).

# COMMAND ----------

cfg = None
from farmia_ingest.config.loader import load_all_datasets
cfg = load_all_datasets(CONF_ROOT)["weather_observations"]
tabla = f"{env.catalog}.{cfg.target.schema}.{cfg.target.table}"

antes = spark.table(tabla).count()
run_batch_dataset(spark, env, cfg, conf_root=CONF_ROOT,
                  mover=dbutils.fs.mv, lister=dbutils.fs.ls, logger=logger)
despues = spark.table(tabla).count()
print(f"filas antes={antes} despues={despues} -> {'OK (no reingesta)' if antes == despues else 'REVISAR'}")

# COMMAND ----------

# MAGIC %md ## 3. Evolución de esquema
# MAGIC Añadimos un JSON con una columna nueva (`discount_code`) y re-ejecutamos: se incorpora.

# COMMAND ----------

cfg = load_all_datasets(CONF_ROOT)["ecommerce_sales_orders"]
nuevo = f"{env.landing_uri}/ecommerce/ecommerce_sales_orders/date=2026-09-04/nuevo.json"
dbutils.fs.put(nuevo, '{"order_id":"ORD-EVO","total":9.9,"discount_code":"PROMO10"}', overwrite=True)

# addNewColumns para la query al detectar la columna nueva; la siguiente ejecución la ingesta.
for intento in range(1, 4):
    try:
        run_batch_dataset(spark, env, cfg, conf_root=CONF_ROOT,
                          mover=dbutils.fs.mv, lister=dbutils.fs.ls, logger=logger)
        print(f"intento {intento}: OK")
        break
    except Exception as exc:
        print(f"intento {intento}: esquema evolucionando, reintentando...")

display(spark.table(f"{env.catalog}.{cfg.target.schema}.{cfg.target.table}")
        .where("order_id = 'ORD-EVO'").select("order_id", "discount_code"))

# COMMAND ----------

# MAGIC %md ## 4. Topic vacío (streaming)
# MAGIC Una query sobre un topic sin mensajes no debe romper: simplemente 0 filas.
# MAGIC (Se prueba de forma acotada; requiere las credenciales del scope, como en `11_run_streaming`.)

# COMMAND ----------

print("Prueba manual: crea un topic vacío en Confluent, añádelo como dataset y ejecuta run_streaming;")
print("el RunReport debe marcarlo OK con 0 filas, sin abortar el resto de queries.")
