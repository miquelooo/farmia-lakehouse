"""Fixtures comunes de los tests. Añade `src/` al path para importar el paquete sin instalar."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
CONF = ROOT / "conf"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
