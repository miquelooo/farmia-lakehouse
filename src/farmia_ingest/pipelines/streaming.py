"""Pipeline streaming: Kafka -> bronze para UN dataset (une reader + deserializador + writer).

Reutiliza el `BronzeDeltaWriter` del batch (mismo destino y patrón), cambiando la fuente a Kafka y
el trigger a continuo si se pide. Singleplex deserializa (JSON/Avro); multiplex deja key/value
binarios y particiona por topic (se estructura en silver).
"""
from __future__ import annotations

from ..config.loader import read_schema_text
from ..config.models import EnvironmentConfig, KafkaConnection, StreamingDatasetConfig
from ..core.errors import IngestionError
from ..core.metadata import with_kafka_metadata
from ..core.naming import PathBuilder
from ..core.queries import await_query_bounded
from ..readers.deserializers import deserialize_avro, deserialize_json
from ..readers.kafka import KafkaStreamReaderBuilder
from ..writers.bronze import BronzeDeltaWriter


def run_streaming_dataset(
    spark,
    env: EnvironmentConfig,
    cfg: StreamingDatasetConfig,
    conn: KafkaConnection,
    conf_root: str,
    logger=None,
    await_termination: bool = False,
    starting_offsets: str = "earliest",
):
    """Ejecuta la ingesta streaming de un dataset (Kafka -> bronze)."""
    pb = PathBuilder(env)

    raw = (
        KafkaStreamReaderBuilder(spark)
        .for_dataset(cfg)
        .with_connection(conn)
        .with_starting_offsets(starting_offsets)
        .build()
        .read()
    )

    # Deserializar según el patrón/formato; el multiplex NO se deserializa (key/value binarios).
    if cfg.is_multiplex:
        data = raw
    elif cfg.fmt == "json":
        data = deserialize_json(raw, read_schema_text(conf_root, cfg.schema_file))
    elif cfg.fmt == "avro":
        data = deserialize_avro(raw, read_schema_text(conf_root, cfg.schema_file))
    else:
        raise IngestionError(cfg.dataset_id, f"formato streaming no soportado: {cfg.fmt}")

    df = with_kafka_metadata(data)

    writer = BronzeDeltaWriter(
        spark,
        cfg,
        bronze_path=pb.bronze_path(cfg),
        checkpoint_path=pb.checkpoint_path(cfg),
        table_name=pb.table_name(cfg),
        logger=logger,
        trigger_seconds=cfg.trigger_seconds,
    )
    query = writer.write(df)
    if await_termination:
        if cfg.trigger_seconds:
            # Streaming continuo (processingTime): bloquea hasta que se pare la query.
            query.awaitTermination()
        else:
            # availableNow (validación acotada): sondeo isActive robusto en serverless. Un topic
            # vacío procesa 0 y para enseguida; no es error.
            await_query_bounded(query, cfg.dataset_id)
    return query
