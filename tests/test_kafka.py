"""Tests de la construcción de opciones de Kafka y del JAAS (puro, sin Spark)."""
from __future__ import annotations

from farmia_ingest.config.models import KafkaConnection, StreamingDatasetConfig, TargetConfig
from farmia_ingest.readers.kafka import JAAS_MODULE, jaas_config, kafka_options


def _conn():
    return KafkaConnection(bootstrap="host:9092", api_key="KEY", api_secret="SECRET")


def _singleplex():
    return StreamingDatasetConfig(
        dataset_id="iot_sensor_readings", source_system="iot",
        target=TargetConfig(schema="bronze_iot", table="sensor_readings"),
        fmt="json", topic="farmia.iot.sensor-readings",
    )


def _multiplex():
    return StreamingDatasetConfig(
        dataset_id="social_media_events", source_system="social",
        target=TargetConfig(schema="bronze_social", table="social_events", partition_by=("_kafka_topic",)),
        subscribe_pattern=r"farmia\.social\..+",
    )


def test_jaas_uses_kafkashaded_prefix_and_creds():
    j = jaas_config("KEY", "SECRET")
    assert j.startswith(JAAS_MODULE)
    assert 'username="KEY"' in j and 'password="SECRET"' in j
    assert j.rstrip().endswith(";")


def test_options_security_is_sasl_ssl():
    opts = kafka_options(_conn(), _singleplex())
    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "PLAIN"
    assert opts["kafka.bootstrap.servers"] == "host:9092"
    assert opts["startingOffsets"] == "earliest"


def test_singleplex_uses_subscribe():
    opts = kafka_options(_conn(), _singleplex())
    assert opts["subscribe"] == "farmia.iot.sensor-readings"
    assert "subscribePattern" not in opts


def test_multiplex_uses_subscribe_pattern():
    opts = kafka_options(_conn(), _multiplex())
    assert opts["subscribePattern"] == r"farmia\.social\..+"
    assert "subscribe" not in opts


def test_starting_offsets_overridable():
    opts = kafka_options(_conn(), _singleplex(), starting_offsets="latest")
    assert opts["startingOffsets"] == "latest"
