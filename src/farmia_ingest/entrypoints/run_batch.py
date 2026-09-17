"""Entry point del job batch (landing -> bronze).

Uso (CLI, vía `farmia-run-batch`) o desde un job de Databricks:
    farmia-run-batch --conf-root conf --env dev --group hourly

Recibe qué GRUPO cargar, de modo que la misma librería sirve a varios workflows (cada 15/30/60 min)
cambiando solo el parámetro (parametrización por job).
"""
from __future__ import annotations

import argparse
import sys

from ..config.loader import load_environment
from ..core.databricks import get_dbutils
from ..core.logging import get_logger, log_event
from ..core.spark import get_spark
from ..orchestration.runner import run_group


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ingesta batch FarmIA (landing -> bronze)")
    parser.add_argument("--conf-root", default="conf")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--group", default="hourly")
    args = parser.parse_args(argv)

    spark = get_spark()
    env = load_environment(f"{args.conf_root}/environments/{args.env}.yaml")
    logger = get_logger()

    dbx = get_dbutils(spark)
    mover = dbx.fs.mv if dbx is not None else None  # sin dbutils no se archiva landing->raw
    if mover is None:
        log_event(logger, "aviso: sin dbutils, no se archivará landing->raw")

    report, _ = run_group(spark, env, args.conf_root, args.group, mover=mover, logger=logger)
    log_event(logger, report.summary_line(), **report.to_dict())
    return 0 if report.all_ok else 1  # el job falla si algún dataset falló


if __name__ == "__main__":
    sys.exit(main())
