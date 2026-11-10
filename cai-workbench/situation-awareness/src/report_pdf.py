"""PDF generation and artifact index for situation reports."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fpdf import FPDF

from .config import Settings


class SituationReportPDF(FPDF):
    def header(self) -> None:
        self.set_font("Helvetica", "B", 12)
        self.cell(0, 10, "Baltic Situation Awareness (Lagebild)", new_x="LMARGIN", new_y="NEXT")
        self.ln(2)


def _sanitize_for_pdf(text: str) -> str:
    return text.encode("latin-1", errors="replace").decode("latin-1")


def write_report_pdf(
    report_text: str,
    comparison_text: str | None,
    settings: Settings,
    report_time: datetime,
) -> Path:
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    stamp = report_time.strftime("%Y%m%d_%H%M%S")
    pdf_path = settings.artifacts_dir / f"lagebild_report_{stamp}.pdf"
    meta_path = settings.artifacts_dir / f"lagebild_report_{stamp}.json"
    txt_path = settings.artifacts_dir / f"lagebild_report_{stamp}.txt"

    txt_path.write_text(report_text, encoding="utf-8")

    pdf = SituationReportPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5, _sanitize_for_pdf(f"Generated: {report_time.isoformat()}\n\n{report_text}"))
    if comparison_text:
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 8, "24h / 48h Comparison", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, _sanitize_for_pdf(comparison_text))
    pdf.output(str(pdf_path))

    meta = {
        "timestamp": report_time.isoformat(),
        "pdf": pdf_path.name,
        "text": txt_path.name,
        "has_comparison": bool(comparison_text),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _update_index(settings.artifacts_dir, meta)
    _link_latest(settings.artifacts_dir, pdf_path)
    return pdf_path


def _link_latest(artifacts_dir: Path, pdf_path: Path) -> None:
    latest = artifacts_dir / "latest.pdf"
    try:
        if latest.is_symlink() or latest.exists():
            latest.unlink()
        latest.symlink_to(pdf_path.name)
    except OSError:
        latest.write_bytes(pdf_path.read_bytes())


def _update_index(artifacts_dir: Path, meta: dict) -> None:
    index_path = artifacts_dir / "index.json"
    records: list[dict] = []
    if index_path.is_file():
        records = json.loads(index_path.read_text(encoding="utf-8"))
    records.append(meta)
    records.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
    records = records[:96]
    index_path.write_text(json.dumps(records, indent=2), encoding="utf-8")


def load_report_texts(
    artifacts_dir: Path, hours_ago: float, now: datetime | None = None
) -> str | None:
    index_path = artifacts_dir / "index.json"
    if not index_path.is_file():
        return None
    now = now or datetime.now(timezone.utc)
    records = json.loads(index_path.read_text(encoding="utf-8"))
    target = now.timestamp() - hours_ago * 3600
    best: tuple[float, str] | None = None
    for rec in records:
        ts = datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = abs(ts.timestamp() - target)
        txt_name = rec.get("text")
        if not txt_name:
            continue
        txt_path = artifacts_dir / txt_name
        if not txt_path.is_file():
            continue
        if best is None or delta < best[0]:
            best = (delta, txt_path.read_text(encoding="utf-8"))
    return best[1] if best else None
