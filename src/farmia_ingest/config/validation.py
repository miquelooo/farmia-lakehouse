"""Validación de la configuración de un dataset contra el JSON Schema del motor.

Se valida el dict crudo del YAML ANTES de construir las dataclasses, para dar errores claros
y tempranos. El schema vive empaquetado junto al código.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..core.errors import ValidationError

_SCHEMA_PATH = Path(__file__).with_name("dataset.schema.json")


@lru_cache(maxsize=1)
def _schema() -> dict[str, Any]:
    return json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))


def validate_dataset_dict(raw: dict[str, Any], *, source: str = "<dict>") -> None:
    """Valida el dict de un dataset. Lanza ValidationError con mensaje legible si falla."""
    import jsonschema  # import local: dependencia de runtime del paquete

    validator = jsonschema.Draft7Validator(_schema())
    errors = sorted(validator.iter_errors(raw), key=lambda e: list(e.path))
    if errors:
        detail = "; ".join(
            f"{'/'.join(str(p) for p in e.path) or '<root>'}: {e.message}" for e in errors
        )
        raise ValidationError(f"Config inválida en {source}: {detail}")
