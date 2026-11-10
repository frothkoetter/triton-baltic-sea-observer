"""NATO-style maritime SITREP PDF generation and artifact index."""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from fpdf import FPDF

from .config import Settings

MONTHS_ZULU = (
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
)

# NATO maritime SITREP palette (aligned with templates/nato_sitrep_reference.pdf).
NATO_HEADER_BLUE = (0, 61, 122)
NATO_TABLE_HEADER_FILL = (210, 218, 226)
NATO_TABLE_GRID = (80, 90, 100)


def format_dtg(report_time: datetime) -> str:
    """Military DTG, e.g. ``291350Z OCT 26`` (UTC)."""
    if report_time.tzinfo is None:
        report_time = report_time.replace(tzinfo=timezone.utc)
    else:
        report_time = report_time.astimezone(timezone.utc)
    mon = MONTHS_ZULU[report_time.month - 1]
    yy = report_time.year % 100
    return f"{report_time.day:02d}{report_time.hour:02d}{report_time.minute:02d}Z {mon} {yy:02d}"


def _sanitize_for_pdf(text: str) -> str:
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _primary_hotspot(dataset_summary: str) -> str:
    m = re.search(r"busiest harbours\s*[—–-]\s*([^;,]+)", dataset_summary, re.I)
    if m:
        return m.group(1).strip().split("(")[0].strip().upper()
    return "BALTIC SEA (GER)"


def _tracks_observed(dataset_summary: str, row_count: int | None) -> str:
    m = re.search(r"(\d+)\s+events", dataset_summary)
    n = int(m.group(1)) if m else (row_count or 0)
    return f"{n} TRACKS ANALYZED"


_HEADING_CORE_RE = re.compile(
    r"^(\d+\.|SECTION\s+\d+[:.)]?\s*)(.+)$", re.I
)


def normalize_heading_line(line: str) -> str | None:
    """Return normalized section title from a markdown/plain heading line, or None."""
    num, title = parse_section_heading(line)
    return title


def parse_section_heading(line: str) -> tuple[str | None, str | None]:
    """Parse ``1. Executive Summary`` / markdown headings into (number, TITLE)."""
    stripped = line.strip()
    if not stripped or len(stripped) > 120:
        return None, None
    stripped = re.sub(r"^\*\*|\*\*$", "", stripped)
    stripped = re.sub(r"^#{1,3}\s+", "", stripped).strip()
    if not stripped:
        return None, None
    m = _HEADING_CORE_RE.match(stripped)
    if m:
        prefix = (m.group(1) or "").strip()
        body = (m.group(2) or "").strip().rstrip("*").strip()
        if not body:
            return None, None
        num_m = re.match(r"^(\d+)\.", prefix, re.I)
        number = num_m.group(1) if num_m else None
        return number, body.upper()
    if stripped.startswith("**") and stripped.endswith("**"):
        inner = stripped.strip("*").strip()
        if inner:
            return None, inner.upper()
    return None, None


def _is_markdown_table_row(line: str) -> bool:
    s = line.strip()
    if not s or s.count("|") < 2:
        return False
    if re.match(r"^[\s|:\-]+$", s.replace("|", "")):
        return True
    return "|" in s


def _is_table_separator(line: str) -> bool:
    s = line.strip().replace(" ", "")
    return bool(s) and bool(re.match(r"^\|?[\-:|]+\|?$", s))


def parse_markdown_table(lines: list[str]) -> list[list[str]]:
    """Parse GitHub-style pipe table lines into a rectangular grid."""
    rows: list[list[str]] = []
    for line in lines:
        if _is_table_separator(line):
            continue
        stripped = line.strip().strip("|")
        cells = [c.strip() for c in stripped.split("|")]
        if any(cells):
            rows.append(cells)
    if not rows:
        return []
    width = max(len(r) for r in rows)
    return [r + [""] * (width - len(r)) for r in rows]


def _is_italic_caption_line(line: str) -> bool:
    s = line.strip()
    return bool(re.match(r"^\*[^*].+\*$", s))


def _strip_italic_wrappers(line: str) -> str:
    s = line.strip()
    if _is_italic_caption_line(s):
        return s.strip("*").strip()
    return s


def inline_markdown_to_html(text: str) -> str:
    """Convert lightweight markdown emphasis to HTML for fpdf ``write_html``."""
    escaped = html.escape(text)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(
        r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", escaped
    )
    return escaped


@dataclass(frozen=True)
class BodyBlock:
    kind: Literal["paragraph", "table", "caption"]
    text: str = ""
    rows: tuple[tuple[str, ...], ...] = ()


def parse_body_blocks(body: str) -> list[BodyBlock]:
    """Split section body into paragraphs, captions, and pipe tables."""
    lines = body.splitlines()
    blocks: list[BodyBlock] = []
    para: list[str] = []
    i = 0

    def flush_para() -> None:
        nonlocal para
        if para:
            text = " ".join(p.strip() for p in para if p.strip())
            if text:
                blocks.append(BodyBlock(kind="paragraph", text=text))
            para = []

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            flush_para()
            i += 1
            continue

        if _is_markdown_table_row(stripped):
            flush_para()
            table_lines: list[str] = []
            while i < len(lines) and _is_markdown_table_row(lines[i].strip()):
                table_lines.append(lines[i])
                i += 1
            rows = parse_markdown_table(table_lines)
            if rows:
                frozen = tuple(tuple(r) for r in rows)
                blocks.append(BodyBlock(kind="table", rows=frozen))
            continue

        if _is_italic_caption_line(stripped) and i + 1 < len(lines):
            nxt = lines[i + 1].strip()
            if _is_markdown_table_row(nxt):
                flush_para()
                blocks.append(
                    BodyBlock(
                        kind="caption",
                        text=_strip_italic_wrappers(stripped),
                    )
                )
                i += 1
                continue

        if normalize_heading_line(stripped) and len(stripped) < 120:
            flush_para()
            i += 1
            continue

        para.append(stripped)
        i += 1

    flush_para()
    return blocks


def _split_report_sections(report_text: str) -> list[tuple[str, str]]:
    """Map LLM output into labelled SITREP sections when numbered headings exist."""
    lines = report_text.strip().splitlines()
    if not lines:
        return [("SECTION 1: SITUATION SUMMARY", "(No narrative generated.)")]

    sections: list[tuple[str, str]] = []
    current_title = "SECTION 1: SITUATION SUMMARY"
    current_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            current_lines.append("")
            continue
        number, heading = parse_section_heading(stripped)
        if heading:
            if current_lines:
                sections.append((current_title, "\n".join(current_lines).strip()))
            if number:
                current_title = f"SECTION {number}: {heading}"
            elif heading.startswith("SECTION"):
                current_title = heading
            else:
                current_title = f"SECTION: {heading}"
            current_lines = []
        else:
            current_lines.append(stripped)

    if current_lines:
        sections.append((current_title, "\n".join(current_lines).strip()))

    if not sections:
        return [("SECTION 1: SITUATION SUMMARY", report_text.strip())]
    return sections


class NatoSitrepPDF(FPDF):
    """Layout aligned with NATO maritime SITREP samples (classification banner + sections)."""

    def __init__(
        self,
        *,
        classification: str,
        theatre: str,
        dtg: str,
        precedence: str,
    ) -> None:
        super().__init__()
        self.classification = classification
        self.theatre = theatre
        self.dtg = dtg
        self.precedence = precedence
        self._header_block_bottom_y = 0.0

    def header(self) -> None:
        margin_l = self.l_margin
        content_w = self.w - self.l_margin - self.r_margin

        self.set_font("Helvetica", "B", 9)
        self.set_text_color(0, 0, 0)
        self.set_y(8)
        self.cell(
            0,
            5,
            _sanitize_for_pdf(self.classification),
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        band_top = self.get_y() + 2
        band_h = 22
        self.set_fill_color(*NATO_HEADER_BLUE)
        self.rect(margin_l, band_top, content_w, band_h, style="F")

        self.set_xy(margin_l, band_top + 3)
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 14)
        self.cell(
            content_w,
            8,
            "MARITIME SITUATION REPORT (SITREP)",
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_x(margin_l)
        self.set_font("Helvetica", "B", 11)
        self.cell(
            content_w,
            6,
            _sanitize_for_pdf(self.theatre),
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )

        self.set_text_color(0, 0, 0)
        self.set_xy(self.l_margin, band_top + band_h + 4)
        self.set_font("Helvetica", "", 10)
        self.cell(
            0,
            5,
            f"DTG: {self.dtg}",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.cell(
            0,
            5,
            f"PRECEDENCE: {self.precedence}",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self._header_block_bottom_y = self.get_y()
        self.ln(2)

    def footer(self) -> None:
        self.set_y(-18)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(0, 0, 0)
        self.cell(
            0,
            4,
            _sanitize_for_pdf(self.classification),
            align="C",
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.cell(0, 4, f"-- {self.page_no()} of {{nb}} --", align="C")

    def situation_summary_table(
        self,
        dtg: str,
        area: str,
        observations: str,
        hotspot: str,
    ) -> None:
        col_w = (self.w - self.l_margin - self.r_margin) / 4
        headers = [
            "DATE-TIME GROUP",
            "AREA OF OPERATIONS",
            "OBSERVATIONS",
            "PRIMARY HOTSPOT",
        ]
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(*NATO_TABLE_HEADER_FILL)
        self.set_draw_color(*NATO_TABLE_GRID)
        for h in headers:
            self.cell(col_w, 6, h, border=1, fill=True)
        self.ln()
        self.set_font("Helvetica", "", 8)
        for val in (dtg, area, observations, hotspot):
            self.cell(col_w, 7, _sanitize_for_pdf(val)[:48], border=1)
        self.ln(10)

    def section_heading(self, title: str) -> None:
        title = re.sub(r"^\*\*|\*\*$", "", title.strip())
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(0, 0, 0)
        self.multi_cell(0, 6, _sanitize_for_pdf(title))
        self.ln(2)

    def body_paragraph(self, text: str) -> None:
        self.set_font("Helvetica", "", 9)
        self.multi_cell(0, 4.5, _sanitize_for_pdf(text))
        self.ln(2)

    def render_data_table(self, rows: list[list[str]]) -> None:
        if not rows:
            return
        content_w = self.w - self.l_margin - self.r_margin
        ncols = len(rows[0])
        col_w = content_w / ncols
        self.set_draw_color(*NATO_TABLE_GRID)
        for ridx, row in enumerate(rows):
            is_header = ridx == 0
            if is_header:
                self.set_font("Helvetica", "B", 8)
                self.set_fill_color(*NATO_TABLE_HEADER_FILL)
            else:
                self.set_font("Helvetica", "", 8)
                self.set_fill_color(255, 255, 255)
            line_h = 5.5
            for cell in row:
                txt = _sanitize_for_pdf(cell)[:64]
                self.cell(
                    col_w,
                    line_h,
                    txt,
                    border=1,
                    fill=is_header,
                )
            self.ln()
        self.ln(3)

    def render_markdown_body(self, body: str) -> None:
        for block in parse_body_blocks(body):
            if block.kind == "caption":
                self.set_font("Helvetica", "I", 8)
                self.set_text_color(60, 60, 60)
                self.multi_cell(
                    0, 4.5, _sanitize_for_pdf(block.text), new_x="LMARGIN", new_y="NEXT"
                )
                self.set_text_color(0, 0, 0)
                self.ln(1)
            elif block.kind == "table":
                self.render_data_table([list(r) for r in block.rows])
            else:
                fragment = inline_markdown_to_html(block.text)
                self.set_font("Helvetica", "", 9)
                try:
                    self.write_html(f"<p>{fragment}</p>")
                except Exception:
                    self.body_paragraph(block.text)
                self.ln(1)


def write_report_pdf(
    report_text: str,
    comparison_text: str | None,
    settings: Settings,
    report_time: datetime,
    *,
    dataset_summary: str = "",
    row_count: int | None = None,
) -> Path:
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    stamp = report_time.strftime("%Y%m%d_%H%M%S")
    base = f"nato_sitrep_{stamp}"
    pdf_path = settings.artifacts_dir / f"{base}.pdf"
    meta_path = settings.artifacts_dir / f"{base}.json"
    txt_path = settings.artifacts_dir / f"{base}.txt"

    txt_path.write_text(report_text, encoding="utf-8")

    classification = __import__("os").environ.get(
        "LAGEBILD_CLASSIFICATION", "NATO UNCLASSIFIED // RELEASABLE TO MARCOM"
    )
    theatre = __import__("os").environ.get("LAGEBILD_THEATRE", "JMOC BALTIC THEATRE")
    precedence = __import__("os").environ.get("LAGEBILD_PRECEDENCE", "ROUTINE")
    if comparison_text and "ESCALATED" in comparison_text.upper():
        precedence = __import__("os").environ.get(
            "LAGEBILD_PRECEDENCE_ESCALATED", "FLASH / IMMEDIATE"
        )

    dtg = format_dtg(report_time)
    hotspot = _primary_hotspot(dataset_summary or report_text)
    observations = _tracks_observed(dataset_summary or report_text, row_count)

    pdf = NatoSitrepPDF(
        classification=classification,
        theatre=theatre,
        dtg=dtg,
        precedence=precedence,
    )
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=22)
    pdf.set_top_margin(56)
    pdf.add_page()
    pdf.situation_summary_table(
        dtg=dtg,
        area="BALTIC SEA (GER)",
        observations=observations,
        hotspot=hotspot,
    )

    for idx, (title, body) in enumerate(_split_report_sections(report_text), start=1):
        if idx > 1 and pdf.get_y() > 240:
            pdf.add_page()
        if not title.upper().startswith("SECTION"):
            title = f"SECTION {idx}: {title}"
        pdf.section_heading(title)
        pdf.render_markdown_body(body)

    if comparison_text:
        pdf.add_page()
        pdf.section_heading("SECTION: 24H / 48H DELTA & DIRECTIVES")
        pdf.render_markdown_body(comparison_text)

    pdf.output(str(pdf_path))

    meta = {
        "timestamp": report_time.isoformat(),
        "format": "nato_sitrep",
        "pdf": pdf_path.name,
        "text": txt_path.name,
        "dtg": dtg,
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
