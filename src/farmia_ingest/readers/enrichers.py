"""Enriquecimientos específicos por tipo de dataset.

Por ahora, para imágenes (`binaryFile`): derivar una `label` a partir de la carpeta que
contiene el fichero (p. ej. `.../crop_field_images/healthy/img.png` -> label="healthy").
Se implementa como una función pura testeable (sin UDF ni dependencia de Spark).
"""
from __future__ import annotations


def parent_folder_name(path: str) -> str | None:
    """Nombre de la carpeta que contiene el fichero (penúltimo segmento de la ruta)."""
    parts = [p for p in path.rstrip("/").split("/") if p]
    return parts[-2] if len(parts) >= 2 else None


def with_image_label(df, label_col: str = "label", path_col: str = "path"):
    """Añade una columna `label` con el nombre de la carpeta padre (import perezoso de pyspark)."""
    from pyspark.sql import functions as F

    return df.withColumn(label_col, F.element_at(F.split(F.col(path_col), "/"), -2))
