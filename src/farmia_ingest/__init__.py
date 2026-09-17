"""farmia_ingest — motor de ingesta config-driven para el lakehouse de FarmIA.

Rutas soportadas:
- batch   : landing -> bronze (Auto Loader `cloudFiles` o Spark file source, según config).
- streaming: Kafka  -> bronze (Spark Structured Streaming; singleplex/multiplex; JSON/Avro).

Regla de portabilidad: este paquete NO importa pyspark ni dbutils en tiempo de import.
Todo lo que depende de Spark/Databricks se importa de forma perezosa dentro de las funciones
que lo necesitan, de modo que `config` y `core` son testeables sin un cluster.
"""

__version__ = "0.1.0"
