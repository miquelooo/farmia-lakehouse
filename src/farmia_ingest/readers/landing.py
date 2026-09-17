"""Lectura de ficheros de `landing` como streaming DataFrame.

Un único reader que soporta **dos engines**:
- `autoloader`: formato `cloudFiles` (Databricks) — descubrimiento eficiente + evolución integrada.
- `spark`: file source nativo de Structured Streaming — portable, ejecutable en local.
El cambio entre uno y otro es una rama según `cfg.engine`.
"""
from __future__ import annotations

from ..config.models import BatchDatasetConfig
from .base import autoloader_options, spark_file_options


class LandingStreamReader:
    def __init__(
        self,
        spark,
        cfg: BatchDatasetConfig,
        landing_path: str,
        schema_location: str,
        schema_ddl: str | None = None,
    ) -> None:
        self.spark = spark
        self.cfg = cfg
        self.landing_path = landing_path
        self.schema_location = schema_location
        self.schema_ddl = schema_ddl

    def read(self):
        return self._read_autoloader() if self.cfg.engine == "autoloader" else self._read_spark()

    @staticmethod
    def _apply(reader, options: dict[str, str]):
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader

    def _read_autoloader(self):
        # Auto Loader INFIERE el esquema (inferColumnTypes) y lo EVOLUCIONA (addNewColumns) — su
        # patrón idiomático. No le pasamos esquema: en serverless, los schemaHints con tipos anidados
        # impedían el descubrimiento de ficheros. El .ddl se reserva para el engine `spark`.
        reader = self.spark.readStream.format("cloudFiles")
        reader = self._apply(reader, autoloader_options(self.cfg, self.schema_location))
        return reader.load(self.landing_path)

    def _read_spark(self):
        # El file source de Structured Streaming NO infiere esquema por defecto. En serverless esta
        # conf no es modificable (CONFIG_NOT_AVAILABLE): se ignora y basta con dar el esquema (.ddl)
        # a json/csv (parquet/avro/binaryFile son auto-descriptivos).
        try:
            self.spark.conf.set("spark.sql.streaming.schemaInference", "true")
        except Exception:
            pass
        reader = self.spark.readStream.format(self.cfg.fmt)
        reader = self._apply(reader, spark_file_options(self.cfg))
        if self.schema_ddl:
            reader = reader.schema(self.schema_ddl)
        return reader.load(self.landing_path)


class LandingStreamReaderBuilder:
    """Builder fluido para construir el reader de landing de forma legible."""

    def __init__(self, spark) -> None:
        self._spark = spark
        self._cfg: BatchDatasetConfig | None = None
        self._landing: str | None = None
        self._schema_location: str | None = None
        self._schema_ddl: str | None = None

    def for_dataset(self, cfg: BatchDatasetConfig) -> "LandingStreamReaderBuilder":
        self._cfg = cfg
        return self

    def with_landing_path(self, path: str) -> "LandingStreamReaderBuilder":
        self._landing = path
        return self

    def with_schema_location(self, path: str) -> "LandingStreamReaderBuilder":
        self._schema_location = path
        return self

    def with_schema_ddl(self, ddl: str | None) -> "LandingStreamReaderBuilder":
        self._schema_ddl = ddl
        return self

    def build(self) -> LandingStreamReader:
        if not (self._cfg and self._landing and self._schema_location):
            raise ValueError("LandingStreamReaderBuilder: faltan cfg/landing/schema_location")
        return LandingStreamReader(
            self._spark, self._cfg, self._landing, self._schema_location, self._schema_ddl
        )
