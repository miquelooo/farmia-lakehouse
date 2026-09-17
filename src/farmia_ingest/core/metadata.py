"""Columnas de metadatos de ingesta.

Se añade a cada registro, como mínimo, la **fecha de ingesta** y el **nombre del fichero origen**.
Convención: prefijo `_` y colocadas AL FINAL del esquema, porque Delta calcula el data skipping
sobre las 32 primeras columnas y así no se "gasta" ese presupuesto en metadatos.

Este módulo NO importa pyspark en tiempo de import: las funciones que usan Spark hacen
import perezoso, de modo que las constantes son testeables sin cluster.
"""
from __future__ import annotations

# Nombres canónicos de las columnas de metadatos (prefijo `_`, al final del esquema).
INGEST_TS = "_ingest_ts"          # timestamp en que el motor cargó el registro
SOURCE_FILE = "_source_file"      # fichero de origen (batch) — ruta en raw tras archivar
INGEST_ENGINE = "_ingest_engine"  # autoloader | spark (trazabilidad del engine usado)

BATCH_METADATA_COLUMNS = (INGEST_TS, SOURCE_FILE, INGEST_ENGINE)

# En streaming el "fichero origen" no aplica; se conservan los metadatos de Kafka
# (topic/partition/offset/timestamp) renombrados con `_`.
KAFKA_METADATA_COLUMNS = (
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    INGEST_TS,
)


def source_uses_path_column(fmt: str | None) -> bool:
    """`binaryFile` no rellena `input_file_name()`; su ruta está en la columna `path`."""
    return fmt == "binaryFile"


def with_batch_metadata(df, engine: str, fmt: str | None = None):
    """Añade las columnas de metadatos de ingesta batch al final del DataFrame.

    `_source_file` = `path` para binaryFile (imágenes) o `_metadata.file_path` para el resto.
    (Con Auto Loader/streaming `input_file_name()` devuelve vacío; `_metadata.file_path` sí funciona.)
    Import perezoso de pyspark.
    """
    from pyspark.sql import functions as F  # import perezoso (solo en runtime Spark)

    source = F.col("path") if source_uses_path_column(fmt) else F.col("_metadata.file_path")
    return (
        df.withColumn(INGEST_TS, F.current_timestamp())
        .withColumn(SOURCE_FILE, source)
        .withColumn(INGEST_ENGINE, F.lit(engine))
    )


def with_kafka_metadata(df):
    """Renombra los metadatos de Kafka con prefijo `_` (al final) y añade la fecha de ingesta.

    Las columnas de datos (deserializadas, o key/value binarios en multiplex) van primero; los
    metadatos de Kafka (topic/partition/offset/timestamp) + `_ingest_ts` van al final.
    """
    from pyspark.sql import functions as F  # import perezoso

    return (
        df.withColumnRenamed("topic", "_kafka_topic")
        .withColumnRenamed("partition", "_kafka_partition")
        .withColumnRenamed("offset", "_kafka_offset")
        .withColumnRenamed("timestamp", "_kafka_timestamp")
        .drop("timestampType")  # ruido del source de Kafka; seguro aunque no exista
        .withColumn(INGEST_TS, F.current_timestamp())
    )
