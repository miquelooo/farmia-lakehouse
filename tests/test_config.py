"""Tests de carga de configuración (loader) contra los YAML reales de conf/."""
from __future__ import annotations

import pytest

from conftest import CONF

from farmia_ingest.config.loader import (
    dataset_from_dict,
    load_environment,
    load_group,
)
from farmia_ingest.config.models import BatchDatasetConfig, StreamingDatasetConfig
from farmia_ingest.core.errors import ConfigError


def test_load_environment_dev():
    env = load_environment(CONF / "environments" / "dev.yaml")
    assert env.catalog == "farmia"
    assert env.landing_uri.startswith("abfss://landing@")
    assert env.bronze_zone == "bronze"
    assert env.kafka_secret_scope == "farmia-kafka"


def test_load_group_hourly_resolves_datasets():
    cfgs = load_group(CONF, "hourly")
    assert len(cfgs) >= 1
    ids = {c.dataset_id for c in cfgs}
    assert "ecommerce_sales_orders" in ids


def test_group_missing_dataset_raises():
    with pytest.raises(ConfigError):
        load_group(CONF, "no_existe_este_grupo")


def test_dataset_from_dict_batch_type():
    raw = {
        "dataset_id": "weather_observations",
        "source_system": "weather",
        "mode": "batch",
        "format": "parquet",
        "target": {"schema": "bronze_weather", "table": "observations",
                   "partition_by": ["_ingest_ts"]},
    }
    cfg = dataset_from_dict(raw, source="test")
    assert isinstance(cfg, BatchDatasetConfig)
    assert cfg.fmt == "parquet"
    assert cfg.engine == "autoloader"           # default aplicado
    assert cfg.target.partition_by == ("_ingest_ts",)
    assert cfg.is_streaming is False


def test_dataset_from_dict_streaming_multiplex():
    raw = {
        "dataset_id": "social_media_events",
        "source_system": "social",
        "mode": "streaming",
        "subscribe_pattern": "[a-zA-Z].+",
        "target": {"schema": "bronze_social", "table": "events",
                   "partition_by": ["_kafka_topic"]},
    }
    cfg = dataset_from_dict(raw, source="test")
    assert isinstance(cfg, StreamingDatasetConfig)
    assert cfg.is_multiplex is True
    assert cfg.is_streaming is True
