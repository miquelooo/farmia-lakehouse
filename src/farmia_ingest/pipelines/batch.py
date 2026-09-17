"""Pipeline batch: landing -> bronze para UN dataset (une reader + writer).

Serverless-safe: el archivado landing->raw se hace en el **driver**, DESPUÉS de `awaitTermination`
(no dentro de `foreachBatch`, que en Spark Connect no admite `dbutils`). Se enumeran los ficheros
con un `lister` inyectado (dbutils.fs.ls) y se mueven con un `mover` inyectado (dbutils.fs.mv).
"""
from __future__ import annotations

from ..config.loader import read_schema_text
from ..config.models import BatchDatasetConfig, EnvironmentConfig
from ..core.logging import log_event
from ..core.metadata import with_batch_metadata
from ..core.naming import PathBuilder
from ..core.queries import await_query_bounded
from ..readers.enrichers import with_image_label
from ..readers.landing import LandingStreamReaderBuilder
from ..writers.archiver import Archiver
from ..writers.bronze import BronzeDeltaWriter


def _list_files(lister, path: str) -> list[str]:
    """Lista recursivamente los ficheros (no directorios) bajo `path` con el lister inyectado
    (en Databricks: dbutils.fs.ls). Devuelve rutas absolutas. Tolerante a rutas inexistentes."""
    out: list[str] = []
    try:
        entries = lister(path)
    except Exception:  # la carpeta puede no existir todavía
        return out
    for f in entries:
        if f.path.endswith("/"):
            out.extend(_list_files(lister, f.path))
        else:
            out.append(f.path)
    return out


def run_batch_dataset(
    spark,
    env: EnvironmentConfig,
    cfg: BatchDatasetConfig,
    conf_root: str,
    mover=None,
    lister=None,
    logger=None,
    await_termination: bool = True,
):
    """Ejecuta la ingesta batch de un dataset.

    `mover`  = dbutils.fs.mv (inyectado) para archivar landing->raw.
    `lister` = dbutils.fs.ls (inyectado) para enumerar los ficheros a archivar.
    """
    pb = PathBuilder(env)

    # Solo los esquemas Spark (.ddl) se pasan al reader; Parquet/Avro son auto-descriptivos.
    schema_ddl = None
    if cfg.schema_file and cfg.schema_file.endswith(".ddl"):
        schema_ddl = read_schema_text(conf_root, cfg.schema_file)

    reader = (
        LandingStreamReaderBuilder(spark)
        .for_dataset(cfg)
        .with_landing_path(pb.landing_path(cfg))
        .with_schema_location(pb.schema_location(cfg))
        .with_schema_ddl(schema_ddl)
        .build()
    )
    df = reader.read()
    # Enriquecimiento por formato: en imágenes derivamos `label` de la carpeta (para particionar).
    if cfg.fmt == "binaryFile":
        df = with_image_label(df)
    df = with_batch_metadata(df, cfg.engine, cfg.fmt)

    raw_base = f"{env.lakehouse_uri.rstrip('/')}/{env.raw_zone.strip('/')}"
    archiver = Archiver(env.landing_uri, raw_base, mover) if mover is not None else None

    # En bronze, _source_file apunta a la ruta donde quedará el fichero tras archivarse (raw).
    if archiver is not None:
        from pyspark.sql import functions as F  # import perezoso (solo en runtime Spark)

        landing = env.landing_uri.rstrip("/")
        df = df.withColumn(
            "_source_file",
            F.when(
                F.col("_source_file").startswith(landing),
                F.concat(F.lit(raw_base), F.expr(f"substring(_source_file, {len(landing) + 1})")),
            ).otherwise(F.col("_source_file")),
        )

    # Snapshot de los ficheros en landing ANTES de procesar: availableNow los ingesta todos y se
    # archivan al terminar (así no se mueven ficheros que llegaran a mitad de proceso).
    files_before = (
        _list_files(lister, pb.landing_path(cfg))
        if (archiver is not None and lister is not None)
        else []
    )

    writer = BronzeDeltaWriter(
        spark,
        cfg,
        bronze_path=pb.bronze_path(cfg),
        checkpoint_path=pb.checkpoint_path(cfg),
        table_name=pb.table_name(cfg),
        logger=logger,
        coalesce=getattr(cfg, "coalesce", None),
    )
    query = writer.write(df)
    if await_termination:
        # Espera robusta (sondeo isActive). Si la query terminó con error (p. ej. Auto Loader
        # addNewColumns para la query al detectar una columna nueva), await_query_bounded lo propaga
        # ANTES de archivar: los ficheros siguen en landing y una re-ejecución los ingesta.
        await_query_bounded(query, cfg.dataset_id)
        # Optimización visible a nivel de tabla (además del optimizeWrite de sesión del writer).
        try:
            spark.sql(
                f"ALTER TABLE {pb.table_name(cfg)} SET TBLPROPERTIES ("
                "'delta.autoOptimize.optimizeWrite' = 'true', "
                "'delta.autoOptimize.autoCompact' = 'true')"
            )
        except Exception:
            pass
        # Archivar landing -> raw (driver-side, fuera de todo job Spark).
        if archiver is not None and files_before:
            moved = archiver.archive(files_before)
            if logger is not None:
                log_event(
                    logger,
                    "archivado landing->raw",
                    dataset_id=cfg.dataset_id,
                    archived_files=len(moved),
                )
    return query
