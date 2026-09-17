"""Runner: ejecuta un grupo de datasets con AISLAMIENTO DE ERRORES.

Si un dataset falla (YAML roto, topic caído, fichero corrupto...), se captura, se registra en el
RunReport y **el resto del grupo sigue ejecutándose**. El mismo runner sirve para batch y streaming:
- batch: cada dataset corre con trigger availableNow (acotado) y se espera en línea.
- streaming: cada query se arranca sin bloquear; al final se espera con awaitAnyTermination (continuo).
"""
from __future__ import annotations

from time import perf_counter

from ..config.loader import load_group
from ..core.logging import log_event
from ..pipelines.batch import run_batch_dataset
from ..pipelines.streaming import run_streaming_dataset
from .report import DatasetResult, RunReport


def run_group(
    spark,
    env,
    conf_root: str,
    group: str,
    *,
    mover=None,
    lister=None,
    conn=None,
    logger=None,
    await_streams: bool = True,
):
    """Ejecuta todos los datasets de un grupo, aislando errores. Devuelve (RunReport, queries)."""
    report = RunReport(group=group)
    queries = []

    for cfg in load_group(conf_root, group):
        start = perf_counter()
        try:
            if cfg.is_streaming:
                query = run_streaming_dataset(
                    spark, env, cfg, conn, conf_root, logger=logger, await_termination=False
                )
                queries.append(query)
            else:
                run_batch_dataset(spark, env, cfg, conf_root, mover=mover, lister=lister, logger=logger)
            report.results.append(
                DatasetResult(cfg.dataset_id, cfg.mode, "ok", perf_counter() - start)
            )
            if logger:
                log_event(logger, "dataset OK", dataset_id=cfg.dataset_id, mode=cfg.mode)
        except Exception as exc:  # AISLAMIENTO: no se propaga; el resto del grupo continúa
            report.results.append(
                DatasetResult(cfg.dataset_id, cfg.mode, "error", perf_counter() - start, str(exc))
            )
            if logger:
                log_event(logger, "dataset ERROR", dataset_id=cfg.dataset_id, error=str(exc))

    if logger:
        log_event(logger, "run finalizado", **report.to_dict())

    # Si se arrancaron streams continuos, mantener el job vivo hasta que alguno termine/caiga.
    if queries and await_streams:
        spark.streams.awaitAnyTermination()

    return report, queries
