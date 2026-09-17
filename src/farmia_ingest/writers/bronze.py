"""Escritura en la capa bronze (Delta) — común a batch y streaming.

Compatible con **Spark Connect / serverless**: NO usa `foreachBatch` (que serializa la función y
no admite ni `dbutils` ni la sesión `spark` externa). El stream se escribe directamente a una
**tabla Delta externa** con `.option("path", ...).toTable(...)`: append + mergeSchema (evolución de
esquema) + particionado, con trigger `availableNow` (batch incremental) o `processingTime` (streaming
continuo) según `trigger_seconds`. El patrón de carga en bronze es SIEMPRE append.

El archivado landing->raw (que usa `dbutils`) NO se hace aquí, sino en el pipeline batch tras
`awaitTermination` (driver-side), porque dentro de un job Spark serverless no se puede usar `dbutils`.
"""
from __future__ import annotations


class BronzeDeltaWriter:
    def __init__(
        self,
        spark,
        cfg,
        bronze_path: str,
        checkpoint_path: str,
        table_name: str,
        logger=None,
        trigger_seconds: int | None = None,
        coalesce: int | None = None,
    ) -> None:
        self.spark = spark
        self.cfg = cfg
        self.bronze_path = bronze_path
        self.checkpoint_path = checkpoint_path
        self.table_name = table_name
        self.logger = logger
        self.trigger_seconds = trigger_seconds
        self.coalesce = coalesce

    def write(self, df):
        """Arranca la streaming query hacia una tabla Delta externa y la devuelve.

        `trigger_seconds` None => availableNow (batch incremental); entero => processingTime continuo.
        """
        # El esquema (base de datos) debe existir antes de registrar la tabla (driver-side).
        schema_fqn = self.table_name.rsplit(".", 1)[0]
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")

        # Optimización de almacenamiento: optimizeWrite + autoCompact atacan el
        # small-file problem en cada escritura. Se fijan a nivel de sesión (best-effort en serverless).
        for _k, _v in (
            ("spark.databricks.delta.optimizeWrite.enabled", "true"),
            ("spark.databricks.delta.autoCompact.enabled", "true"),
        ):
            try:
                self.spark.conf.set(_k, _v)
            except Exception:  # algunas confs no son modificables en serverless: no es crítico
                pass

        # Compactar la salida si el dataset lo pide (p. ej. imágenes -> coalesce(1) = un fichero/lote).
        if self.coalesce:
            df = df.coalesce(self.coalesce)

        stream = (
            df.writeStream.queryName(f"bronze_{self.cfg.dataset_id}")
            .outputMode("append")                              # bronze SIEMPRE append
            .option("checkpointLocation", self.checkpoint_path)
            .option("mergeSchema", "true")                     # evolución de esquema
            .option("path", self.bronze_path)                 # -> tabla EXTERNA en esa ruta
        )
        if self.cfg.target.partition_by:
            stream = stream.partitionBy(*self.cfg.target.partition_by)
        if self.trigger_seconds:
            stream = stream.trigger(processingTime=f"{self.trigger_seconds} seconds")
        else:
            stream = stream.trigger(availableNow=True)
        # toTable crea/registra la tabla externa en Unity Catalog y hace append por micro-batch.
        return stream.toTable(self.table_name)
