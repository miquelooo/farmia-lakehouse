"""Entry point del job streaming (Kafka -> bronze).

Uso (CLI, vía `farmia-run-streaming`) o desde un job de Databricks:
    farmia-run-streaming --conf-root conf --env dev --group streaming

Requiere Databricks (lee las credenciales de Kafka/Schema Registry del secret scope). Arranca todas
las streaming queries del grupo y mantiene el job vivo (awaitAnyTermination).
"""
from __future__ import annotations

import argparse
import sys

from ..config.loader import load_environment
from ..config.models import KafkaConnection
from ..core.databricks import get_dbutils
from ..core.logging import get_logger, log_event
from ..core.spark import get_spark
from ..orchestration.runner import run_group


def _connection_from_secrets(dbx, scope: str) -> KafkaConnection:
    get = lambda key: dbx.secrets.get(scope, key)  # noqa: E731
    return KafkaConnection(
        bootstrap=get("kafka_bootstrap"),
        api_key=get("kafka_api_key"),
        api_secret=get("kafka_api_secret"),
        schema_registry_url=get("schema_registry_url"),
        sr_api_key=get("sr_api_key"),
        sr_api_secret=get("sr_api_secret"),
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ingesta streaming FarmIA (Kafka -> bronze)")
    parser.add_argument("--conf-root", default="conf")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--group", default="streaming")
    args = parser.parse_args(argv)

    spark = get_spark()
    env = load_environment(f"{args.conf_root}/environments/{args.env}.yaml")
    logger = get_logger()

    dbx = get_dbutils(spark)
    if dbx is None:
        raise RuntimeError("run_streaming requiere Databricks (credenciales del secret scope)")
    conn = _connection_from_secrets(dbx, env.kafka_secret_scope)

    report, _ = run_group(spark, env, args.conf_root, args.group, conn=conn, logger=logger)
    log_event(logger, report.summary_line(), **report.to_dict())
    return 0 if report.all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
