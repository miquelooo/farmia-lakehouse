# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Smoke test del entorno FarmIA
# MAGIC
# MAGIC Valida, antes de escribir el motor de ingesta, que todo el aprovisionamiento funciona:
# MAGIC 1. Acceso a ADLS (`landing` y `lakehouse`) — lectura/escritura
# MAGIC 2. `dbutils.fs.mv` (archivado landing → raw) en **serverless**
# MAGIC 3. Delta + Unity Catalog (tabla **managed** y **externa**)
# MAGIC 4. Lectura de los 6 secretos del scope `farmia-kafka`
# MAGIC 5. Conexión a Kafka (Confluent Cloud) vía SASL_SSL
# MAGIC
# MAGIC Ejecutar con cómputo **Serverless**. Cada comprobación imprime una línea de confirmación si va bien.

# COMMAND ----------

# MAGIC %md ## 0. Configuración

# COMMAND ----------

LANDING   = "abfss://landing@farmiaadls.dfs.core.windows.net"
LAKEHOUSE = "abfss://lakehouse@farmiaadls.dfs.core.windows.net"
CATALOG   = "farmia"
SCOPE     = "farmia-kafka"
TEST_TOPIC = "farmia.iot.sensor-readings"

print("spark:", spark.version)

# COMMAND ----------

# MAGIC %md ## 1. ADLS — escritura y lectura en landing y lakehouse

# COMMAND ----------

# landing
p_landing = f"{LANDING}/_smoke/hello.txt"
dbutils.fs.put(p_landing, "hola desde landing", overwrite=True)
assert dbutils.fs.head(p_landing).startswith("hola"), "No se pudo leer de landing"
print("✅ landing R/W OK")

# lakehouse
p_lake = f"{LAKEHOUSE}/_smoke/hello.txt"
dbutils.fs.put(p_lake, "hola desde lakehouse", overwrite=True)
assert dbutils.fs.head(p_lake).startswith("hola"), "No se pudo leer de lakehouse"
print("✅ lakehouse R/W OK")

# COMMAND ----------

# MAGIC %md ## 2. `dbutils.fs.mv` — archivado landing → raw (patrón del motor)

# COMMAND ----------

src = f"{LANDING}/_smoke/hello.txt"
dst = f"{LAKEHOUSE}/raw/_smoke/hello.txt"
dbutils.fs.mv(src, dst)
assert dbutils.fs.head(dst).startswith("hola"), "El mv no dejó el fichero en destino"
print("✅ dbutils.fs.mv (landing→raw) OK en serverless")

# COMMAND ----------

# MAGIC %md ## 3. Delta + Unity Catalog — tabla managed y externa

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.smoke")

# tabla MANAGED (va al storage gestionado del catálogo, __unitystorage)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.smoke.test_managed")
spark.sql(f"CREATE TABLE {CATALOG}.smoke.test_managed (id INT, val STRING) USING DELTA")
spark.sql(f"INSERT INTO {CATALOG}.smoke.test_managed VALUES (1, 'managed')")
assert spark.table(f"{CATALOG}.smoke.test_managed").count() == 1
print("✅ tabla Delta MANAGED OK")

# tabla EXTERNA en lakehouse/bronze (lo que hará el motor) — valida que no hay overlap.
# Path NUEVO por ejecución: el DROP de una tabla externa NO borra los ficheros, y Unity Catalog
# bloquea el rm de un path con tabla registrada (LOCATION_OVERLAP). Usar un path fresco lo evita.
import time
ext_path = f"{LAKEHOUSE}/bronze/_smoke/test_external_{int(time.time())}"
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.smoke.test_external")
spark.sql(f"""
  CREATE TABLE {CATALOG}.smoke.test_external (id INT, val STRING)
  USING DELTA LOCATION '{ext_path}'
""")
spark.sql(f"INSERT INTO {CATALOG}.smoke.test_external VALUES (1, 'external')")
assert spark.table(f"{CATALOG}.smoke.test_external").count() == 1
print("✅ tabla Delta EXTERNA en lakehouse/bronze OK (sin overlap con el catálogo)")

# COMMAND ----------

# MAGIC %md ## 4. Secretos — scope `farmia-kafka` (no imprime valores)

# COMMAND ----------

keys = ["kafka_bootstrap", "kafka_api_key", "kafka_api_secret",
        "schema_registry_url", "sr_api_key", "sr_api_secret"]
for k in keys:
    v = dbutils.secrets.get(SCOPE, k)   # el valor se muestra REDACTADO por seguridad
    assert v and len(v) > 0, f"Secreto vacío: {k}"
    print(f"✅ {k} (longitud {len(v)})")

# COMMAND ----------

# MAGIC %md ## 5. Kafka — conexión a Confluent Cloud (SASL_SSL)
# MAGIC Lee el topic en modo batch. Aunque tenga 0 mensajes, si las credenciales son correctas **no** dará error de autenticación.

# COMMAND ----------

bootstrap = dbutils.secrets.get(SCOPE, "kafka_bootstrap")
api_key   = dbutils.secrets.get(SCOPE, "kafka_api_key")
api_secret = dbutils.secrets.get(SCOPE, "kafka_api_secret")

# En Databricks el módulo JAAS lleva el prefijo "kafkashaded."
jaas = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{api_key}" password="{api_secret}";'
)

kafka_df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas)
    .option("subscribe", TEST_TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
n = kafka_df.count()
print(f"✅ Conexión Kafka OK — {n} mensajes en '{TEST_TOPIC}' (0 es normal si aún no has producido datos)")

# COMMAND ----------

# MAGIC %md ## 6. Limpieza

# COMMAND ----------

# Orden importante: primero DROP SCHEMA (quita los registros de tablas), luego rm de los ficheros;
# al revés, Unity Catalog daría LOCATION_OVERLAP al borrar un path con tabla aún registrada.
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.smoke CASCADE")
for _p in (f"{LAKEHOUSE}/raw/_smoke", f"{LAKEHOUSE}/_smoke", f"{LAKEHOUSE}/bronze/_smoke"):
    try:
        dbutils.fs.rm(_p, recurse=True)
    except Exception as _e:  # ficheros residuales que no bloquean la validación
        print("aviso limpieza fs:", _p, _e)
print("✅ limpieza hecha — entorno validado de punta a punta")
