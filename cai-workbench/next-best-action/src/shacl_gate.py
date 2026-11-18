"""Phase 8b: SHACL validation gate before Trino decision persist."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .decision_rdf import SHAPES_PATH, decision_record_to_ttl


@dataclass
class ShaclReport:
    conforms: bool
    report_text: str
    violations: list[str]
    engine: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "conforms": self.conforms,
            "engine": self.engine,
            "violations": self.violations,
            "report_excerpt": self.report_text[:4000],
        }


class ShaclGateBlocked(RuntimeError):
    """Raised when NBA_SHACL_GATE blocks Trino update."""

    def __init__(self, report: ShaclReport) -> None:
        self.report = report
        super().__init__(
            "SHACL validation failed; decision_record not persisted to Trino"
        )


def _violations_from_report(report_text: str) -> list[str]:
    lines = []
    for line in report_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if "Constraint Violation" in stripped or "sh:Violation" in stripped:
            lines.append(stripped)
        elif stripped.startswith("- ") and "Focus Node" in report_text:
            lines.append(stripped)
    if not lines and "Non-conformant" in report_text:
        lines.append("Non-conformant data graph (see full SHACL report)")
    return lines[:20]


def validate_decision_ttl(
    data_ttl: str,
    shapes_path: Path | None = None,
) -> ShaclReport:
    shapes_path = shapes_path or SHAPES_PATH
    shapes_ttl = shapes_path.read_text(encoding="utf-8")
    try:
        from pyshacl import validate
    except ImportError as exc:
        return ShaclReport(
            conforms=False,
            report_text=str(exc),
            violations=["pyshacl not installed (pip install pyshacl rdflib)"],
            engine="unavailable",
        )

    try:
        conforms, report_graph, report_text = validate(
            data_ttl,
            shacl_graph=shapes_ttl,
            data_graph_format="turtle",
            shacl_graph_format="turtle",
            inference="none",
            advanced=False,
            debug=False,
        )
    except Exception as exc:  # noqa: BLE001 — RDF parse / SHACL runtime
        return ShaclReport(
            conforms=False,
            report_text=str(exc),
            violations=[str(exc)],
            engine="pyshacl",
        )
    text = str(report_text)
    violations = _violations_from_report(text) if not conforms else []
    return ShaclReport(
        conforms=bool(conforms),
        report_text=text,
        violations=violations,
        engine="pyshacl",
    )


def validate_decision_artifacts(
    decision_record: dict[str, Any],
    grounding: dict[str, Any] | None,
    *,
    shapes_path: Path | None = None,
) -> tuple[str, ShaclReport]:
    data_ttl = decision_record_to_ttl(decision_record, grounding)
    report = validate_decision_ttl(data_ttl, shapes_path=shapes_path)
    return data_ttl, report


def assert_shacl_conforms(report: ShaclReport, *, gate_enabled: bool) -> None:
    if not gate_enabled:
        return
    if not report.conforms:
        raise ShaclGateBlocked(report)