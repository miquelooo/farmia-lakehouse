"""Modelos de configuración (dataclasses).

El motor es *config-driven*: cada dataset se describe en un YAML y estas dataclasses son su
representación tipada en memoria. Se separan del entorno (`EnvironmentConfig`) para que el
mismo dataset pueda ejecutarse en dev/prod sin tocar su YAML (nada hardcodeado).
"""
from __future__ import annotations

from dataclasses import dataclass, field

# Valores permitidos (se validan también vía JSON Schema; aquí como fuente de verdad Python).
MODES = ("batch", "streaming")
ENGINES = ("autoloader", "spark")                       # engine de lectura de FICHEROS (batch)
BATCH_FORMATS = ("json", "csv", "parquet", "avro", "binaryFile")
STREAM_FORMATS = ("json", "avro")                       # formato del value del mensaje Kafka
EVOLUTION_MODES = ("addNewColumns", "rescue", "failOnNewColumns", "none")


@dataclass(frozen=True)
class EnvironmentConfig:
    """Parámetros del entorno de ejecución (dev/prod). Nada de esto vive en el YAML del dataset."""

    catalog: str
    landing_uri: str          # p.ej. abfss://landing@farmiaadls.dfs.core.windows.net
    lakehouse_uri: str        # p.ej. abfss://lakehouse@farmiaadls.dfs.core.windows.net
    raw_zone: str = "raw"
    bronze_zone: str = "bronze"
    checkpoints_prefix: str = "_checkpoints"
    schemas_prefix: str = "_schemas"          # schemaLocation de Auto Loader
    kafka_secret_scope: str = "farmia-kafka"


@dataclass(frozen=True)
class TargetConfig:
    """Destino en bronze: esquema (base de datos) + tabla + particionado."""

    schema: str
    table: str
    partition_by: tuple[str, ...] = ()

    def full_name(self, catalog: str) -> str:
        return f"{catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class BatchDatasetConfig:
    """Dataset batch (fichero en landing -> tabla Delta en bronze)."""

    dataset_id: str
    source_system: str
    fmt: str                      # uno de BATCH_FORMATS
    target: TargetConfig
    engine: str = "autoloader"    # autoloader (cloudFiles) | spark (file source)
    schema_file: str | None = None
    reader_options: dict = field(default_factory=dict)
    schema_evolution_mode: str = "addNewColumns"
    coalesce: int | None = None   # nº de ficheros de salida por micro-batch (small-file problem; p.ej. imágenes)
    mode: str = "batch"

    @property
    def is_streaming(self) -> bool:
        return False


@dataclass(frozen=True)
class StreamingDatasetConfig:
    """Dataset streaming (topic(s) de Kafka -> tabla Delta en bronze).

    - Singleplex: se fija `topic` y `fmt` (json/avro) y se deserializa el value.
    - Multiplex:  se fija `subscribe_pattern`; key/value quedan binarios (se estructura en silver).
    """

    dataset_id: str
    source_system: str
    target: TargetConfig
    fmt: str | None = None                # json/avro (singleplex); None en multiplex
    topic: str | None = None              # singleplex
    subscribe_pattern: str | None = None  # multiplex
    value_subject: str | None = None      # subject del Schema Registry (avro)
    key_subject: str | None = None
    schema_file: str | None = None
    reader_options: dict = field(default_factory=dict)
    trigger_seconds: int | None = None    # None => availableNow; >0 => processingTime continuo
    mode: str = "streaming"

    @property
    def is_streaming(self) -> bool:
        return True

    @property
    def is_multiplex(self) -> bool:
        return self.subscribe_pattern is not None


@dataclass(frozen=True)
class KafkaConnection:
    """Datos de conexión a Confluent (se rellenan desde el secret scope, NUNCA en el YAML)."""

    bootstrap: str
    api_key: str
    api_secret: str
    schema_registry_url: str | None = None
    sr_api_key: str | None = None
    sr_api_secret: str | None = None
