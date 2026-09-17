"""PathBuilder: construye rutas y nombres de forma consistente a partir del entorno.

Centraliza la convención de rutas para que ningún path esté hardcodeado por ahí:
- landing:      {landing_uri}/{source_system}/{dataset_id}
- raw (archivo):{lakehouse_uri}/{raw_zone}/{source_system}/{dataset_id}
- bronze:       {lakehouse_uri}/{bronze_zone}/{schema}/{table}
- checkpoint:   {lakehouse_uri}/{checkpoints_prefix}/{dataset_id}
- schemaLoc:    {lakehouse_uri}/{schemas_prefix}/{dataset_id}
"""
from __future__ import annotations

from ..config.models import (
    BatchDatasetConfig,
    EnvironmentConfig,
    StreamingDatasetConfig,
)


class PathBuilder:
    def __init__(self, env: EnvironmentConfig) -> None:
        self.env = env

    @staticmethod
    def _join(*parts: str) -> str:
        """Une segmentos con '/' evitando barras duplicadas (respeta el esquema abfss://)."""
        cleaned = [p.strip("/") for p in parts[1:]]
        base = parts[0].rstrip("/")
        return "/".join([base, *cleaned]) if cleaned else base

    def landing_path(self, cfg: BatchDatasetConfig) -> str:
        return self._join(self.env.landing_uri, cfg.source_system, cfg.dataset_id)

    def raw_path(self, cfg: BatchDatasetConfig) -> str:
        return self._join(
            self.env.lakehouse_uri, self.env.raw_zone, cfg.source_system, cfg.dataset_id
        )

    def bronze_path(self, cfg: BatchDatasetConfig | StreamingDatasetConfig) -> str:
        return self._join(
            self.env.lakehouse_uri, self.env.bronze_zone, cfg.target.schema, cfg.target.table
        )

    def checkpoint_path(self, cfg: BatchDatasetConfig | StreamingDatasetConfig) -> str:
        return self._join(self.env.lakehouse_uri, self.env.checkpoints_prefix, cfg.dataset_id)

    def schema_location(self, cfg: BatchDatasetConfig) -> str:
        return self._join(self.env.lakehouse_uri, self.env.schemas_prefix, cfg.dataset_id)

    def table_name(self, cfg: BatchDatasetConfig | StreamingDatasetConfig) -> str:
        return cfg.target.full_name(self.env.catalog)
