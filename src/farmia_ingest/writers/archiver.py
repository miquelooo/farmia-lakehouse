"""Archivado de ficheros procesados: landing -> raw.

El movimiento físico se delega en un `mover(src, dst)` inyectado (en Databricks: `dbutils.fs.mv`),
para no importar dbutils dentro del paquete (portabilidad + testeabilidad con un mover falso).
La transformación de ruta landing->raw es pura y se testea aparte.
"""
from __future__ import annotations

from typing import Callable


class Archiver:
    def __init__(self, landing_uri: str, raw_base: str, mover: Callable[[str, str], None]) -> None:
        self.landing_uri = landing_uri.rstrip("/")
        self.raw_base = raw_base.rstrip("/")
        self.mover = mover

    def raw_for(self, src: str) -> str:
        """Mapea un fichero de landing a su ruta equivalente en raw (misma sub-estructura)."""
        if src.startswith(self.landing_uri):
            return self.raw_base + src[len(self.landing_uri):]
        return src  # si no cuelga de landing, no se transforma

    def archive(self, source_files: list[str]) -> list[tuple[str, str]]:
        """Mueve cada fichero de landing a raw. Devuelve las parejas (src, dst) movidas."""
        moved: list[tuple[str, str]] = []
        for src in source_files:
            dst = self.raw_for(src)
            if dst != src:
                self.mover(src, dst)
                moved.append((src, dst))
        return moved
