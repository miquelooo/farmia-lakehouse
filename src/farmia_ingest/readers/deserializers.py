"""Deserialización del `value` de Kafka (JSON o Avro) a columnas.

- JSON: `from_json(value::string, schema_ddl)`.
- Avro: `from_avro(...)` sobre el value binario, saltando la **cabecera de 5 bytes** del wire
  format de Confluent (byte 0 = magic; bytes 1-4 = id del esquema en el Schema Registry).

Tras deserializar, se conservan los metadatos de Kafka (topic/partition/offset/timestamp) para que
`with_kafka_metadata` los coloque al final. El multiplex NO pasa por aquí: mantiene key/value binarios.
"""
from __future__ import annotations

_KAFKA_META = ("topic", "partition", "offset", "timestamp")


def wire_format_strip_expr(value_col: str = "value") -> str:
    """Expr SQL que descarta los 5 bytes de cabecera del wire format Confluent.

    substring es 1-indexado: empieza en el byte 6 y toma (longitud - 5) bytes.
    """
    return f"substring({value_col}, 6, length({value_col})-5)"


def deserialize_json(df, schema_ddl: str):
    """value (binario) -> string -> struct(schema) -> columnas + metadatos de Kafka."""
    from pyspark.sql import functions as F

    parsed = df.withColumn("_v", F.from_json(F.col("value").cast("string"), schema_ddl))
    return parsed.select("_v.*", *_KAFKA_META)


def deserialize_avro(df, avro_schema: str):
    """value (binario, wire format) -> se salta la cabecera -> from_avro -> columnas + metadatos."""
    from pyspark.sql import functions as F
    from pyspark.sql.avro.functions import from_avro

    parsed = df.withColumn("_v", from_avro(F.expr(wire_format_strip_expr("value")), avro_schema))
    return parsed.select("_v.*", *_KAFKA_META)
