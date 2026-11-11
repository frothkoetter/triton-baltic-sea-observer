"""NATO-style maritime SITREP PDF generation and artifact index."""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Callable
from typing import Literal

import pandas as pd
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
NATO_CRITICAL_RED = (155, 28, 28)
NATO_CRITICAL_FILL = (255, 236, 236)

_CRITICAL_SECTION_RE = re.compile(r"\bCRITICAL\b", re.I)
_CRITICAL_TERM_RE = re.compile(
    r"\b("
    r"submarine(?:s)?|sanction(?:ed|s)?|blocked|"
    r"frigate|corvette|landing\s+ship|LST|Ropucha|"
    r"gas\s+tanker|oil\s+tanker|container\s+ship|"
    r"high[- ]priority|probable|confidence\s+[\d.]+|"
    r"E\.O\.\s+14024|Project\s+11356|surface\s+combatant(?:s)?|"
    r"magnetic\s+anomaly|export\s+control"
    r")\b",
    re.I,
)


def format_dtg(report_time: datetime) -> str:
    """Military DTG, e.g. ``291350Z OCT 26`` (UTC)."""
    if report_time.tzinfo is None:
        report_time = report_time.replace(tzinfo=timezone.utc)
    else:
        report_time = report_time.astimezone(timezone.utc)
    mon = MONTHS_ZULU[report_time.month - 1]
    yy = report_time.year % 100
    return f"{report_time.day:02d}{report_time.hour:02d}{report_time.minute:02d}Z {mon} {yy:02d}"


_UNICODE_TO_LATIN: dict[str, str] = {
    "\u2014": "--",  # em dash
    "\u2013": "-",  # en dash
    "\u2012": "-",  # figure dash
    "\u2212": "-",  # minus sign
    "\u2018": "'",  # left single quote
    "\u2019": "'",  # right single quote
    "\u201a": ",",  # single low-9 quotation mark
    "\u201c": '"',  # left double quote
    "\u201d": '"',  # right double quote
    "\u201e": '"',  # double low-9 quotation mark
    "\u2026": "...",  # ellipsis
    "\u00a0": " ",  # nbsp
    "\u200b": "",  # zero-width space
    "\u2022": "*",  # bullet
    "\u2192": "->",
    "\u2190": "<-",
}


def _sanitize_for_pdf(text: str) -> str:
    """Make LLM text safe for fpdf core fonts (latin-1 / Helvetica)."""
    if not text:
        return text
    for char, repl in _UNICODE_TO_LATIN.items():
        text = text.replace(char, repl)
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

_PRIMARY_NUMBERED_SECTION_RE = re.compile(
    r"^\s*(?:#{1,3}\s+)?(?:\*\*)?\d+\.\s+.+",
    re.I,
)


def normalize_heading_line(line: str) -> str | None:
    """Return normalized section title from a markdown/plain heading line, or None."""
    num, title = parse_section_heading(line)
    return title


def _is_primary_section_break(line: str) -> bool:
    """True only for top-level report sections, not nested SECTION N: action lines."""
    stripped = line.strip()
    if not stripped or len(stripped) > 120:
        return False
    if _PRIMARY_NUMBERED_SECTION_RE.match(stripped):
        return True
    m = re.match(r"^SECTION\s+\d+\s*:\s*(.+)$", stripped, re.I)
    if m:
        rest = m.group(1)
        if "**" in rest:
            return False
        if re.search(r"\b(IMMEDIATE|MPA|USV|VERIFICATION|FOLLOW-UP|RE-SCAN)\b", rest, re.I):
            return False
        return True
    if stripped.startswith("**") and stripped.endswith("**"):
        inner = stripped.strip("*").strip()
        if inner and not re.match(r"^SECTION\s+\d+", inner, re.I):
            return bool(parse_section_heading(stripped)[1])
    return False


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


_MD_SUBHEADING_RE = re.compile(r"^#{1,3}\s+(.+)$")
_BULLET_LINE_RE = re.compile(r"^[\s]*(?:[-*•]|\u2022)\s+(.+)$")


def _is_horizontal_rule(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if s in ("---", "***", "___"):
        return True
    return bool(re.match(r"^[-*_]{3,}\s*$", s))


def _parse_markdown_subheading(line: str) -> str | None:
    """Return subheading title from ``##`` / ``###`` lines (uppercase)."""
    m = _MD_SUBHEADING_RE.match(line.strip())
    if not m:
        return None
    title = re.sub(r"^\*\*|\*\*$", "", m.group(1).strip()).strip()
    return title.upper() if title else None


def _is_italic_caption_line(line: str) -> bool:
    s = line.strip()
    return bool(re.match(r"^\*[^*].+\*$", s))


def _strip_italic_wrappers(line: str) -> str:
    s = line.strip()
    if _is_italic_caption_line(s):
        return s.strip("*").strip()
    return s


def _is_critical_section(title: str) -> bool:
    return bool(_CRITICAL_SECTION_RE.search(title))


def _latin1_encodable(text: str) -> bool:
    try:
        text.encode("latin-1")
        return True
    except UnicodeEncodeError:
        return False


def _strip_html_tags(fragment: str) -> str:
    return re.sub(r"<[^>]+>", "", fragment)


def _highlight_critical_terms_html(escaped: str) -> str:
    """Wrap threat / sanctions terms in red bold after HTML escape."""

    def repl(match: re.Match[str]) -> str:
        return (
            f'<font color="#9B1C1C"><b>{match.group(0)}</b></font>'
        )

    return _CRITICAL_TERM_RE.sub(repl, escaped)


def inline_markdown_to_html(
    text: str, *, emphasize_critical: bool = True
) -> str:
    """Convert lightweight markdown emphasis to HTML for fpdf ``write_html``."""
    escaped = html.escape(_sanitize_for_pdf(text))
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(
        r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", escaped
    )
    if emphasize_critical:
        escaped = _highlight_critical_terms_html(escaped)
    return escaped


@dataclass(frozen=True)
class BodyBlock:
    kind: Literal["paragraph", "table", "caption", "subheading", "bullet_list"]
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
            parts = [
                p.strip()
                for p in para
                if p.strip() and not _is_horizontal_rule(p.strip())
            ]
            text = " ".join(parts)
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

        if _is_horizontal_rule(stripped):
            flush_para()
            i += 1
            continue

        sub_title = _parse_markdown_subheading(stripped)
        if sub_title is not None:
            flush_para()
            blocks.append(BodyBlock(kind="subheading", text=sub_title))
            i += 1
            continue

        bullet_m = _BULLET_LINE_RE.match(stripped)
        if bullet_m:
            flush_para()
            bullets: list[str] = []
            while i < len(lines):
                bm = _BULLET_LINE_RE.match(lines[i].strip())
                if not bm:
                    break
                bullets.append(bm.group(1).strip())
                i += 1
            if bullets:
                frozen = tuple((b,) for b in bullets)
                blocks.append(BodyBlock(kind="bullet_list", rows=frozen))
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

        if _is_primary_section_break(stripped):
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
        if not _is_primary_section_break(stripped):
            current_lines.append(stripped)
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

    def render_subheading(self, text: str) -> None:
        margin_l = self.l_margin
        content_w = self.w - self.l_margin - self.r_margin
        y = self.get_y()
        bar_w = 2.5
        line_h = 6.0
        self.set_fill_color(*NATO_HEADER_BLUE)
        self.rect(margin_l, y, bar_w, line_h, style="F")
        self.set_xy(margin_l + bar_w + 2, y)
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*NATO_HEADER_BLUE)
        self.cell(
            content_w - bar_w - 2,
            line_h,
            _sanitize_for_pdf(text),
            new_x="LMARGIN",
            new_y="NEXT",
        )
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def _estimate_blocks_height(
        self, blocks: list[BodyBlock], inner_w: float
    ) -> float:
        pad = 3.0
        total = pad * 2
        for block in blocks:
            if block.kind == "subheading":
                total += 10.0
            elif block.kind == "caption":
                total += 6.0
            elif block.kind == "table":
                total += 5.5 * max(len(block.rows), 1) + 3.0
            elif block.kind == "bullet_list":
                total += 4.5 * max(len(block.rows), 1) + 1.0
            elif block.kind == "paragraph":
                safe = _sanitize_for_pdf(block.text)
                line_count = len(
                    self.multi_cell(
                        inner_w,
                        4.5,
                        safe,
                        split_only=True,
                    )
                )
                total += 4.5 * max(line_count, 1) + 1.0
        return total

    def _render_body_blocks(self, blocks: list[BodyBlock]) -> None:
        for block in blocks:
            if block.kind == "subheading":
                self.render_subheading(block.text)
            elif block.kind == "caption":
                self.set_font("Helvetica", "I", 8)
                self.set_text_color(60, 60, 60)
                self.multi_cell(
                    0, 4.5, _sanitize_for_pdf(block.text), new_x="LMARGIN", new_y="NEXT"
                )
                self.set_text_color(0, 0, 0)
                self.ln(1)
            elif block.kind == "table":
                self.render_data_table([list(r) for r in block.rows])
            elif block.kind == "bullet_list":
                self.set_font("Helvetica", "", 9)
                for row in block.rows:
                    item = row[0] if row else ""
                    safe = _sanitize_for_pdf(item)
                    plain = re.sub(r"\*\*(.+?)\*\*", r"\1", safe)
                    is_category = bool(
                        re.match(
                            r"^[A-Za-z][A-Za-z0-9 /\-]{2,48}:\s*$",
                            plain.strip(),
                        )
                    )
                    if is_category:
                        self.set_font("Helvetica", "B", 9)
                    self.multi_cell(
                        0,
                        4.5,
                        f"  * {plain.strip()}",
                        new_x="LMARGIN",
                        new_y="NEXT",
                    )
                    if is_category:
                        self.set_font("Helvetica", "", 9)
                self.ln(1)
            else:
                safe = _sanitize_for_pdf(block.text)
                plain = re.sub(r"\*\*(.+?)\*\*", r"\1", safe)
                fragment = inline_markdown_to_html(safe)
                self.set_font("Helvetica", "", 9)
                self._write_html_or_plain(f"<p>{fragment}</p>", plain)
                self.ln(1)

    def render_framed_block(
        self,
        render_fn: Callable[[], None],
        *,
        estimated_height: float,
        fill: tuple[int, int, int] | None = None,
    ) -> None:
        pad = 3.0
        x = self.l_margin
        content_w = self.w - self.l_margin - self.r_margin
        y0 = self.get_y()
        frame_h0 = max(estimated_height, pad * 2)

        self.set_fill_color(*(fill or NATO_TABLE_HEADER_FILL))
        self.set_draw_color(*NATO_TABLE_GRID)
        self.set_line_width(0.25)
        self.rect(x, y0, content_w, frame_h0, style="DF")

        old_l, old_r = self.l_margin, self.r_margin
        self.set_left_margin(x + pad)
        self.set_right_margin(self.w - (x + content_w - pad))
        self.set_xy(x + pad, y0 + pad)

        render_fn()

        y1 = self.get_y() + pad
        self.set_left_margin(old_l)
        self.set_right_margin(old_r)

        actual_h = y1 - y0
        if actual_h > frame_h0:
            self.set_fill_color(*(fill or NATO_TABLE_HEADER_FILL))
            self.rect(x, y0 + frame_h0, content_w, actual_h - frame_h0, style="F")
        self.set_draw_color(*NATO_TABLE_GRID)
        self.rect(x, y0, content_w, actual_h, style="D")

        self.set_y(y1)
        self.ln(2)

    def section_heading(self, title: str) -> None:
        title = re.sub(r"^\*\*|\*\*$", "", title.strip())
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(0, 0, 0)
        self.multi_cell(0, 6, _sanitize_for_pdf(title))
        self.ln(2)

    def _write_html_or_plain(self, html: str, plain: str) -> None:
        """``write_html`` on core fonts rejects some entities; never leave the page closed."""
        visible = _strip_html_tags(html)
        if not _latin1_encodable(visible):
            self.multi_cell(
                0,
                4.5,
                _sanitize_for_pdf(plain),
                new_x="LMARGIN",
                new_y="NEXT",
            )
            return
        try:
            self.write_html(html, table_line_separators=True)
        except Exception:
            if not getattr(self, "page", None):
                raise
            self.multi_cell(
                0,
                4.5,
                _sanitize_for_pdf(plain),
                new_x="LMARGIN",
                new_y="NEXT",
            )

    def _bullet_item_html(self, item: str) -> str:
        safe = _sanitize_for_pdf(item)
        cat = re.match(r"^\s*\*\*(.+?)\*\*\s*:?\s*$", safe)
        if cat:
            label = html.escape(cat.group(1).strip())
            return f'<font color="#003D7A"><b>{label}</b></font>'
        cat2 = re.match(r"^([A-Za-z][A-Za-z0-9 /\-]{2,40}):\s*$", safe)
        if cat2:
            label = html.escape(cat2.group(1).strip())
            return f'<font color="#003D7A"><b>{label}:</b></font>'
        return inline_markdown_to_html(safe)

    def render_sitrep_section(self, title: str, body: str) -> None:
        """Bordered section with title band and in-body critical emphasis."""
        title = re.sub(r"^\*\*|\*\*$", "", title.strip())
        critical = _is_critical_section(title)
        blocks = parse_body_blocks(body)
        pad = 3.0
        content_w = self.w - self.l_margin - self.r_margin
        inner_w = content_w - 2 * pad
        title_band_h = 7.5
        est_h = pad * 2 + title_band_h + 2 + self._estimate_blocks_height(
            blocks, inner_w
        )

        def render_inner() -> None:
            margin_l = self.l_margin
            band_w = self.w - self.l_margin - self.r_margin
            y_band = self.get_y()
            if critical:
                self.set_fill_color(*NATO_CRITICAL_RED)
            else:
                self.set_fill_color(*NATO_HEADER_BLUE)
            self.rect(margin_l, y_band, band_w, title_band_h, style="F")
            self.set_xy(margin_l + 2, y_band + 1.5)
            self.set_font("Helvetica", "B", 9)
            self.set_text_color(255, 255, 255)
            title_html = inline_markdown_to_html(title, emphasize_critical=False)
            if critical:
                title_html = re.sub(
                    r"(?i)(\(critical\)|\bcritical\b)",
                    r'<font color="#FFE082"><b>\1</b></font>',
                    title_html,
                )
            title_plain = _sanitize_for_pdf(title)
            self._write_html_or_plain(
                f"<p>{title_html}</p>",
                title_plain,
            )
            self.set_text_color(0, 0, 0)
            self.set_y(y_band + title_band_h + 2)
            self._render_body_blocks(blocks)

        section_fill = NATO_CRITICAL_FILL if critical else None
        self.render_framed_block(
            render_inner, estimated_height=est_h, fill=section_fill
        )

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

    def render_markdown_body(self, body: str, *, framed: bool = False) -> None:
        blocks = parse_body_blocks(body)
        if framed:
            pad = 3.0
            content_w = self.w - self.l_margin - self.r_margin
            inner_w = content_w - 2 * pad
            est_h = self._estimate_blocks_height(blocks, inner_w)
            self.render_framed_block(
                lambda: self._render_body_blocks(blocks),
                estimated_height=est_h,
            )
        else:
            self._render_body_blocks(blocks)

    def render_comparison_body(self, body: str) -> None:
        self.render_markdown_body(body, framed=True)


def load_prior_lagebild_snapshot(
    artifacts_dir: Path,
    *,
    hours: float = 24,
    exclude_base: str | None = None,
) -> dict[str, object] | None:
    """Load harbour/source counts from the most recent prior SITREP meta JSON."""
    if not artifacts_dir.is_dir():
        return None
    candidates = sorted(artifacts_dir.glob("nato_sitrep_*.json"), reverse=True)
    now = datetime.now(timezone.utc)
    for path in candidates:
        if exclude_base and path.stem == exclude_base:
            continue
        try:
            meta = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        ts_raw = meta.get("timestamp")
        if ts_raw:
            ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if (now - ts.astimezone(timezone.utc)).total_seconds() > hours * 3600:
                continue
        harbour = meta.get("harbour_counts") or {}
        source = meta.get("source_counts") or {}
        if not harbour and not source and meta.get("row_count") is None:
            continue
        return {
            "harbour_counts": {str(k): int(v) for k, v in harbour.items()},
            "source_counts": {str(k): int(v) for k, v in source.items()},
            "row_count": meta.get("row_count"),
            "meta_path": path.name,
        }
    return None


def write_report_pdf(
    report_text: str,
    comparison_text: str | None,
    settings: Settings,
    report_time: datetime,
    *,
    dataset_summary: str = "",
    row_count: int | None = None,
    df_used: pd.DataFrame | None = None,
) -> tuple[Path, Path | None, Path | None, Path | None]:
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

    map_path: Path | None = None
    charts_path: Path | None = None
    if df_used is not None and not df_used.empty:
        from .lagebild_viz_prep import normalize_lagebild_frame

        df_used = normalize_lagebild_frame(df_used)
        content_w = pdf.w - pdf.l_margin - pdf.r_margin
        charts_path = settings.artifacts_dir / f"{base}_charts.png"
        try:
            from .lagebild_charts import render_lagebild_charts

            render_lagebild_charts(
                df_used,
                charts_path,
                comparison_text=comparison_text,
            )
            if charts_path.is_file() and charts_path.stat().st_size > 0:
                pdf.section_heading("SECTION: DATA VISUALIZATION")
                pdf.image(str(charts_path), w=min(content_w, 180))
                pdf.ln(4)
            else:
                charts_path = None
        except Exception:
            charts_path = None

        map_path = settings.artifacts_dir / f"{base}_map.png"
        try:
            from .lagebild_map import render_lagebild_map

            render_lagebild_map(
                df_used,
                map_path,
                comparison_text=comparison_text,
                dataset_summary=dataset_summary,
            )
            if map_path.is_file() and map_path.stat().st_size > 0:
                pdf.section_heading("SECTION: SITUATION MAP (BALTIC SEA)")
                pdf.image(str(map_path), w=min(content_w, 180))
                pdf.ln(4)
            else:
                map_path = None
        except Exception:
            map_path = None

    source_counts: dict[str, int] = {}
    harbour_counts: dict[str, int] = {}
    zebra_path: Path | None = None
    if df_used is not None and not df_used.empty:
        prior = load_prior_lagebild_snapshot(
            settings.artifacts_dir, exclude_base=base
        )
        prior_source = (prior or {}).get("source_counts") or {}
        prior_harbour = (prior or {}).get("harbour_counts") or {}
        zebra_path = settings.artifacts_dir / f"{base}_zebra.png"
        try:
            from .lagebild_zebra import (
                harbour_counts_from_df,
                render_lagebild_zebra,
                source_counts_from_df,
            )

            source_counts = source_counts_from_df(df_used)
            harbour_counts = harbour_counts_from_df(df_used)
            render_lagebild_zebra(
                df_used,
                zebra_path,
                prior_source_counts=dict(prior_source),
                prior_harbour_counts=dict(prior_harbour),
                comparison_text=comparison_text,
            )
            if zebra_path.is_file() and zebra_path.stat().st_size > 0:
                pdf.section_heading("SECTION: ANALYTICS (ZEBRA STYLE)")
                content_w = pdf.w - pdf.l_margin - pdf.r_margin
                pdf.image(str(zebra_path), w=min(content_w, 180))
                pdf.ln(4)
            else:
                zebra_path = None
        except Exception:
            zebra_path = None

    for idx, (title, body) in enumerate(_split_report_sections(report_text), start=1):
        if idx > 1 and pdf.get_y() > 220:
            pdf.add_page()
        if not title.upper().startswith("SECTION"):
            title = f"SECTION {idx}: {title}"
        pdf.render_sitrep_section(title, body)

    if comparison_text:
        pdf.add_page()
        pdf.section_heading("SECTION: 24H / 48H DELTA & DIRECTIVES")
        pdf.render_comparison_body(comparison_text)

    pdf.output(str(pdf_path))

    meta = {
        "timestamp": report_time.isoformat(),
        "format": "nato_sitrep",
        "pdf": pdf_path.name,
        "text": txt_path.name,
        "map": map_path.name if map_path else None,
        "charts": charts_path.name if charts_path else None,
        "zebra": zebra_path.name if zebra_path else None,
        "dtg": dtg,
        "has_comparison": bool(comparison_text),
        "row_count": row_count if row_count is not None else (len(df_used) if df_used is not None else None),
        "source_counts": source_counts,
        "harbour_counts": harbour_counts,
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    _update_index(settings.artifacts_dir, meta)
    _link_latest(settings.artifacts_dir, pdf_path)
    return pdf_path, map_path, charts_path, zebra_path


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
