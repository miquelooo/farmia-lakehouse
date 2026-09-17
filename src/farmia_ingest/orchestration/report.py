"""Informe de ejecución (RunReport).

Resume el resultado de un run por grupo: qué datasets fueron OK y cuáles fallaron. Es Python puro
(testeable sin Spark) y serializable a dict para volcarlo al log JSON. Da soporte al aislamiento
de errores por dataset del runner.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass
class DatasetResult:
    dataset_id: str
    mode: str                 # batch | streaming
    status: str               # ok | error
    duration_s: float = 0.0
    error: str | None = None


@dataclass
class RunReport:
    group: str
    results: list[DatasetResult] = field(default_factory=list)

    @property
    def ok(self) -> list[DatasetResult]:
        return [r for r in self.results if r.status == "ok"]

    @property
    def errors(self) -> list[DatasetResult]:
        return [r for r in self.results if r.status == "error"]

    @property
    def all_ok(self) -> bool:
        return not self.errors

    def summary_line(self) -> str:
        return f"grupo '{self.group}': {len(self.ok)} OK, {len(self.errors)} con error"

    def to_dict(self) -> dict:
        return {
            "group": self.group,
            "ok": len(self.ok),
            "error": len(self.errors),
            "datasets": [asdict(r) for r in self.results],
        }
