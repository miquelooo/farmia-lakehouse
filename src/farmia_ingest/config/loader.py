"""Carga de configuración desde YAML a las dataclasses tipadas.

La configuración está externalizada (nada hardcodeado): entorno, datasets y grupos son
ficheros YAML. En Databricks estos YAML se despliegan en el workspace o en la cuenta de
almacenamiento; el loader solo necesita un directorio raíz `conf/`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..core.errors import ConfigError
from .models import (
    BatchDatasetConfig,
    EnvironmentConfig,
    StreamingDatasetConfig,
    TargetConfig,
)
from .validation import validate_dataset_dict

DatasetConfig = BatchDatasetConfig | StreamingDatasetConfig


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"No existe el fichero de configuración: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:  # YAML mal formado
        raise ConfigError(f"YAML inválido en {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"El YAML de {path} debe ser un mapa de claves/valores")
    return data


def load_environment(path: str | Path) -> EnvironmentConfig:
    raw = _read_yaml(Path(path))
    try:
        return EnvironmentConfig(**raw)
    except TypeError as exc:  # claves inesperadas / faltantes
        raise ConfigError(f"Entorno inválido en {path}: {exc}") from exc


def _to_target(raw: dict[str, Any]) -> TargetConfig:
    return TargetConfig(
        schema=raw["schema"],
        table=raw["table"],
        partition_by=tuple(raw.get("partition_by", [])),
    )


def dataset_from_dict(raw: dict[str, Any], *, source: str = "<dict>") -> DatasetConfig:
    """Valida y convierte el dict de un dataset en su dataclass (batch o streaming)."""
    validate_dataset_dict(raw, source=source)
    target = _to_target(raw["target"])

    if raw["mode"] == "batch":
        return BatchDatasetConfig(
            dataset_id=raw["dataset_id"],
            source_system=raw["source_system"],
            fmt=raw["format"],
            target=target,
            engine=raw.get("engine", "autoloader"),
            schema_file=raw.get("schema_file"),
            reader_options=raw.get("reader_options", {}),
            schema_evolution_mode=raw.get("schema_evolution_mode", "addNewColumns"),
            coalesce=raw.get("coalesce"),
        )

    return StreamingDatasetConfig(
        dataset_id=raw["dataset_id"],
        source_system=raw["source_system"],
        target=target,
        fmt=raw.get("format"),
        topic=raw.get("topic"),
        subscribe_pattern=raw.get("subscribe_pattern"),
        value_subject=raw.get("value_subject"),
        key_subject=raw.get("key_subject"),
        schema_file=raw.get("schema_file"),
        reader_options=raw.get("reader_options", {}),
        trigger_seconds=raw.get("trigger_seconds"),
    )


def load_dataset(path: str | Path) -> DatasetConfig:
    path = Path(path)
    return dataset_from_dict(_read_yaml(path), source=str(path))


def load_all_datasets(conf_root: str | Path) -> dict[str, DatasetConfig]:
    """Carga todos los datasets de `conf/datasets/**.yaml` indexados por dataset_id."""
    root = Path(conf_root) / "datasets"
    result: dict[str, DatasetConfig] = {}
    for path in sorted(root.rglob("*.yaml")):
        cfg = load_dataset(path)
        if cfg.dataset_id in result:
            raise ConfigError(f"dataset_id duplicado: {cfg.dataset_id} ({path})")
        result[cfg.dataset_id] = cfg
    return result


def load_group(conf_root: str | Path, group: str) -> list[DatasetConfig]:
    """Resuelve un grupo (p.ej. 'hourly') a la lista de datasets que debe cargar ese job.

    Un grupo es `conf/groups/<group>.yaml` con `{ datasets: [<dataset_id>, ...] }`.
    Así el mismo motor sirve a varios workflows (cada 15/30/60 min) según parámetro.
    """
    group_path = Path(conf_root) / "groups" / f"{group}.yaml"
    raw = _read_yaml(group_path)
    ids = raw.get("datasets")
    if not isinstance(ids, list) or not ids:
        raise ConfigError(f"El grupo {group_path} debe tener una lista 'datasets' no vacía")

    all_datasets = load_all_datasets(conf_root)
    missing = [d for d in ids if d not in all_datasets]
    if missing:
        raise ConfigError(f"El grupo '{group}' referencia datasets inexistentes: {missing}")
    return [all_datasets[d] for d in ids]


def read_schema_text(conf_root: str | Path, schema_file: str) -> str:
    """Lee el texto de un fichero de esquema (`schemas/<id>.ddl` o `.avsc`) relativo a conf_root."""
    path = Path(conf_root) / schema_file
    if not path.exists():
        raise ConfigError(f"No existe el fichero de esquema: {path}")
    return path.read_text(encoding="utf-8").strip()
