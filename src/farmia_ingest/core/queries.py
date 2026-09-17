"""Espera de streaming queries robusta en serverless / Spark Connect.

En serverless, `StreamingQuery.awaitTermination()` puede no detectar el fin de una query
`availableNow` (se queda bloqueado aunque la query ya haya parado). Aquí esperamos **sondeando
`isActive`**, que sí es fiable, con un límite para no colgar el proceso si una fuente nunca
llega a descubrir datos. Si la query terminó con error (p. ej. Auto Loader `addNewColumns` para
la query al detectar una columna nueva), se propaga como `IngestionError`.
"""
from __future__ import annotations

import time

from .errors import IngestionError

# availableNow procesa los datos existentes en segundos; el límite solo salta si una query se
# queda "esperando datos" y colgaría el proceso.
DEFAULT_TIMEOUT_S = 300


def await_query_bounded(query, dataset_id: str, timeout_s: int = DEFAULT_TIMEOUT_S) -> None:
    """Espera (sondeando) a que una query `availableNow` termine. Corta al superar el límite
    y propaga si la query falló."""
    waited = 0
    while query.isActive and waited < timeout_s:
        time.sleep(3)
        waited += 3
    if query.isActive:
        query.stop()
        raise IngestionError(dataset_id, f"la query no terminó en {timeout_s}s")
    exc = query.exception()
    if exc is not None:
        raise IngestionError(dataset_id, f"la query terminó con error: {exc}")
