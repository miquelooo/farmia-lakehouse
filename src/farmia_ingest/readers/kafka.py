"""Lectura de topics de Kafka (Confluent) como streaming DataFrame.

No hay Auto Loader para Kafka: se usa el input source nativo `kafka` de Structured
Streaming. Soporta **singleplex** (`subscribe` a un topic) y **multiplex** (`subscribePattern`).
Las credenciales llegan en un `KafkaConnection` inyectado (leído del secret scope), nunca del YAML.
La construcción de opciones es pura y testeable; JAAS usa el prefijo `kafkashaded.` (Databricks).
"""
from __future__ import annotations

from ..config.models import KafkaConnection, StreamingDatasetConfig

# En Databricks el módulo JAAS va con el prefijo "kafkashaded.".
JAAS_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"


def jaas_config(api_key: str, api_secret: str) -> str:
    return f'{JAAS_MODULE} required username="{api_key}" password="{api_secret}";'


def kafka_options(
    conn: KafkaConnection, cfg: StreamingDatasetConfig, starting_offsets: str = "earliest"
) -> dict[str, str]:
    """Opciones del source `kafka` (seguridad SASL_SSL + suscripción según singleplex/multiplex)."""
    opts: dict[str, str] = {
        "kafka.bootstrap.servers": conn.bootstrap,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config(conn.api_key, conn.api_secret),
        "startingOffsets": starting_offsets,
    }
    if cfg.is_multiplex:
        opts["subscribePattern"] = cfg.subscribe_pattern
    else:
        opts["subscribe"] = cfg.topic
    opts.update({k: str(v) for k, v in cfg.reader_options.items()})
    return opts


class KafkaStreamReader:
    def __init__(
        self,
        spark,
        cfg: StreamingDatasetConfig,
        conn: KafkaConnection,
        starting_offsets: str = "earliest",
    ) -> None:
        self.spark = spark
        self.cfg = cfg
        self.conn = conn
        self.starting_offsets = starting_offsets

    def read(self):
        reader = self.spark.readStream.format("kafka")
        for key, value in kafka_options(self.conn, self.cfg, self.starting_offsets).items():
            reader = reader.option(key, value)
        return reader.load()


class KafkaStreamReaderBuilder:
    """Builder fluido para construir el reader de Kafka de forma legible."""

    def __init__(self, spark) -> None:
        self._spark = spark
        self._cfg: StreamingDatasetConfig | None = None
        self._conn: KafkaConnection | None = None
        self._starting_offsets = "earliest"

    def for_dataset(self, cfg: StreamingDatasetConfig) -> "KafkaStreamReaderBuilder":
        self._cfg = cfg
        return self

    def with_connection(self, conn: KafkaConnection) -> "KafkaStreamReaderBuilder":
        self._conn = conn
        return self

    def with_starting_offsets(self, value: str) -> "KafkaStreamReaderBuilder":
        self._starting_offsets = value
        return self

    def build(self) -> KafkaStreamReader:
        if not (self._cfg and self._conn):
            raise ValueError("KafkaStreamReaderBuilder: faltan cfg/conn")
        return KafkaStreamReader(self._spark, self._cfg, self._conn, self._starting_offsets)
