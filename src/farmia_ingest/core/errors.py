"""Excepciones del motor.

Se distingue entre errores de *configuración* (se detectan antes de tocar Spark, en
validación/carga) y errores de *ingesta* (fallo procesando un dataset concreto). El runner
captura `IngestionError` por dataset para aislar fallos: un dataset roto no aborta el resto.
"""
from __future__ import annotations


class FarmiaError(Exception):
    """Base de todos los errores del motor."""


class ConfigError(FarmiaError):
    """Configuración inválida (YAML mal formado, campos que faltan, valores no permitidos)."""


class ValidationError(ConfigError):
    """El YAML no cumple el JSON Schema del motor."""


class IngestionError(FarmiaError):
    """Fallo procesando un dataset concreto en tiempo de ejecución.

    Lleva el `dataset_id` para poder reportarlo en el RunReport sin perder el rastro.
    """

    def __init__(self, dataset_id: str, message: str) -> None:
        self.dataset_id = dataset_id
        super().__init__(f"[{dataset_id}] {message}")
