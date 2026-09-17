"""Acceso a la SparkSession.

En Databricks `spark` ya existe (sesión activa). En local (engine `spark`) se crea una. Import
perezoso de pyspark para respetar la regla de portabilidad de `core`.
"""
from __future__ import annotations


def get_spark(app_name: str = "farmia_ingest"):
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.appName(app_name).getOrCreate()
