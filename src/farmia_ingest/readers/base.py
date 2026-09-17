"""Helpers puros para construir las opciones de lectura (testeables sin Spark).

Separar la construcción de opciones (dict) de su aplicación al reader permite testear la lógica
sin necesidad de un cluster.
"""
from __future__ import annotations

from ..config.models import BatchDatasetConfig

FILE_FORMATS = ("json", "csv", "parquet", "avro", "binaryFile")


def autoloader_options(
    cfg: BatchDatasetConfig, schema_location: str, schema_hints: str | None = None
) -> dict[str, str]:
    """Opciones del formato `cloudFiles` (Auto Loader).

    El esquema (si lo hay) se pasa como `cloudFiles.schemaHints`, NO con `.schema()`: así Auto Loader
    tipa las columnas conocidas y AÚN puede evolucionar (`addNewColumns`). Un esquema completo con
    `.schema()` es incompatible con la evolución de esquema (error CF_ADD_NEW_NOT_SUPPORTED).
    """
    # binaryFile tiene esquema fijo (path/modificationTime/length/content): Auto Loader NO admite
    # addNewColumns para ese formato, hay que usar 'none'.
    evolution = "none" if cfg.fmt == "binaryFile" else cfg.schema_evolution_mode
    opts: dict[str, str] = {
        "cloudFiles.format": cfg.fmt,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.schemaEvolutionMode": evolution,
    }
    # JSON/CSV: inferir tipos (compatible con los hints; Parquet/Avro/binaryFile son auto-descriptivos).
    if cfg.fmt in ("json", "csv"):
        opts["cloudFiles.inferColumnTypes"] = "true"
    if schema_hints:
        opts["cloudFiles.schemaHints"] = schema_hints
    opts.update({k: str(v) for k, v in cfg.reader_options.items()})
    return opts


def spark_file_options(cfg: BatchDatasetConfig) -> dict[str, str]:
    """Opciones del file source nativo de Structured Streaming (engine `spark`)."""
    opts: dict[str, str] = {"recursiveFileLookup": "true"}  # leer subcarpetas date=...
    opts.update({k: str(v) for k, v in cfg.reader_options.items()})
    return opts
