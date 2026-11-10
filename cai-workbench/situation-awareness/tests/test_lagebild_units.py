#!/usr/bin/env python3
"""Unit tests for situation-awareness (no Trino / live LLM)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from src.compare_reports import parse_comparison_sections, rule_based_theme_diff
from src.config import Settings
from src.data_access import dataframe_to_markdown_bounded, lagebild_dataset_summary
from src.llm_client import _normalize_azure_base_url, normalize_api_key
from src.lagebild_charts import render_lagebild_charts
from src.lagebild_map import basemap_backend, render_lagebild_map
from src.lagebild_zebra import render_lagebild_zebra, source_mix_radar
from src.report_pdf import (
    _is_primary_section_break,
    _sanitize_for_pdf,
    _split_report_sections,
    format_dtg,
    inline_markdown_to_html,
    normalize_heading_line,
    parse_body_blocks,
    parse_markdown_table,
    parse_section_heading,
    load_prior_lagebild_snapshot,
    write_report_pdf,
)
from datetime import datetime, timedelta, timezone


class TestDatasetSummary(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertIn("No events", lagebild_dataset_summary(pd.DataFrame()))

    def test_counts(self) -> None:
        df = pd.DataFrame(
            {
                "data_source": ["AIS", "AIS", "LiDAR"],
                "harbour_name": ["Kiel", "Kiel", "Rostock"],
            }
        )
        summary = lagebild_dataset_summary(df)
        self.assertIn("3 events", summary)
        self.assertIn("AIS: 2", summary)
        self.assertIn("Kiel", summary)


class TestMarkdownTruncation(unittest.TestCase):
    def test_within_limit(self) -> None:
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        md, used = dataframe_to_markdown_bounded(df, 32000)
        self.assertEqual(len(used), 2)
        self.assertLessEqual(len(md), 32000)

    def test_truncates_large_table(self) -> None:
        df = pd.DataFrame({"text": ["word " * 200 for _ in range(500)]})
        md, used = dataframe_to_markdown_bounded(df, 2000)
        self.assertLess(len(used), len(df))
        self.assertLessEqual(len(md), 2000)


class TestComparisonParsing(unittest.TestCase):
    def test_escalation_lines(self) -> None:
        text = """
## Per-Theme Delta
ESCALATED | Sanctions-related AIS activity near Kiel
NEW | LiDAR frigate contact Rostock
UNCHANGED | Routine ferry traffic Wismar
"""
        deltas = parse_comparison_sections(text)
        statuses = {d.status for d in deltas}
        self.assertIn("ESCALATED", statuses)
        self.assertIn("NEW", statuses)
        self.assertGreaterEqual(len(deltas), 3)

    def test_rule_based_diff(self) -> None:
        cur = "sanctions Kiel frigate Rostock"
        prior = "routine ferry Wismar"
        deltas = rule_based_theme_diff(cur, prior)
        self.assertTrue(any(d.status == "NEW" for d in deltas))


class TestApiKeyNormalize(unittest.TestCase):
    def test_strips_trailing_space(self) -> None:
        self.assertEqual(normalize_api_key("abc123 "), "abc123")

    def test_strips_bearer_prefix(self) -> None:
        self.assertEqual(normalize_api_key("Bearer tok"), "tok")


class TestAzureNormalize(unittest.TestCase):
    def test_strips_chat_completions_and_api_version(self) -> None:
        url = (
            "https://example.openai.azure.com/openai/deployments/x"
            "/chat/completions?api-version=2025-01-01-preview"
        )
        base, ver = _normalize_azure_base_url(url)
        self.assertEqual(
            base, "https://example.openai.azure.com/openai/deployments/x"
        )
        self.assertEqual(ver, "2025-01-01-preview")


class TestSectionSplitting(unittest.TestCase):
    def test_nested_section_action_lines_stay_in_section_six(self) -> None:
        report = """**6. Recommended Actions**
SECTION 1: **IMMEDIATE MPA/USV VERIFICATION:** Deploy patrol near Kiel.
SECTION 2: **SANCTIONS FOLLOW-UP:** Contact port authority Rostock.
**7. Closing**
All items closed.
"""
        self.assertFalse(
            _is_primary_section_break("SECTION 1: **IMMEDIATE MPA/USV VERIFICATION:**")
        )
        sections = _split_report_sections(report)
        titles = [t for t, _ in sections]
        self.assertTrue(any("RECOMMENDED" in t.upper() for t in titles))
        action_section = next(b for t, b in sections if "RECOMMENDED" in t.upper())
        self.assertIn("IMMEDIATE MPA", action_section)
        self.assertIn("SANCTIONS FOLLOW-UP", action_section)
        self.assertLessEqual(len(sections), 3)

    def test_primary_numbered_headings_still_split(self) -> None:
        report = """**1. Executive Summary**
Overview.

**2. Threat Assessment**
Details.
"""
        sections = _split_report_sections(report)
        self.assertEqual(len(sections), 2)
        self.assertIn("EXECUTIVE", sections[0][0].upper())


class TestLagebildZebra(unittest.TestCase):
    def test_zebra_png_smoke(self) -> None:
        import tempfile

        df = pd.DataFrame(
            {
                "data_source": ["AIS", "AIS", "LiDAR", "Buoy", "AIS"],
                "harbour_name": ["Kiel", "Kiel", "Rostock", "Wismar", "Rostock"],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "zebra.png"
            render_lagebild_zebra(
                df,
                path,
                prior_source_counts={"AIS": 1, "LiDAR": 2},
                prior_harbour_counts={"Kiel": 1, "Rostock": 1},
            )
            self.assertTrue(path.is_file())
            self.assertGreater(path.stat().st_size, 2048)

    def test_radar_single_series_without_prior(self) -> None:
        import matplotlib.pyplot as plt
        import tempfile

        df = pd.DataFrame(
            {"data_source": ["AIS", "LiDAR"], "harbour_name": ["Kiel", "Rostock"]}
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "radar.png"
            fig = plt.figure(figsize=(4, 4))
            ax = fig.add_subplot(111, projection="polar")
            source_mix_radar(df, ax, prior_counts=None)
            fig.savefig(path)
            plt.close(fig)
            self.assertGreater(path.stat().st_size, 2048)


class TestPriorSnapshotMeta(unittest.TestCase):
    def test_meta_includes_counts(self) -> None:
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            s = Settings()
            s.artifacts_dir = Path(tmp)
            df = pd.DataFrame(
                {
                    "data_source": ["AIS", "LiDAR"],
                    "harbour_name": ["Kiel", "Rostock"],
                    "severity": ["low", "medium"],
                }
            )
            write_report_pdf(
                "Summary only.",
                None,
                s,
                datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc),
                df_used=df,
                row_count=2,
            )
            meta_files = list(Path(tmp).glob("nato_sitrep_*.json"))
            self.assertEqual(len(meta_files), 1)
            meta = json.loads(meta_files[0].read_text(encoding="utf-8"))
            self.assertIn("source_counts", meta)
            self.assertIn("harbour_counts", meta)
            self.assertEqual(meta["source_counts"].get("AIS"), 1)
            self.assertEqual(meta["harbour_counts"].get("Kiel"), 1)

    def test_load_prior_snapshot_excludes_current(self) -> None:
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            artifacts = Path(tmp)
            ts = datetime.now(timezone.utc) - timedelta(hours=2)
            prior = {
                "timestamp": ts.isoformat(),
                "source_counts": {"AIS": 3},
                "harbour_counts": {"Kiel": 2},
                "row_count": 3,
            }
            stamp = ts.strftime("%Y%m%d_%H%M%S")
            (artifacts / f"nato_sitrep_{stamp}.json").write_text(
                json.dumps(prior), encoding="utf-8"
            )
            loaded = load_prior_lagebild_snapshot(
                artifacts, exclude_base="nato_sitrep_current_stub"
            )
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded["source_counts"]["AIS"], 3)


class TestLagebildCharts(unittest.TestCase):
    def test_render_charts_png(self) -> None:
        import tempfile

        df = pd.DataFrame(
            {
                "data_source": ["AIS", "AIS", "LiDAR", "Buoy"],
                "harbour_name": ["Kiel", "Rostock", "Kiel", "Wismar"],
                "severity": ["high", "medium", "low", "medium"],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "charts.png"
            render_lagebild_charts(
                df,
                path,
                comparison_text="NEW | Contact near Kiel\nUNCHANGED | Ferry corridor",
            )
            self.assertTrue(path.is_file())
            self.assertGreater(path.stat().st_size, 1024)

    def test_render_charts_without_comparison(self) -> None:
        import tempfile

        df = pd.DataFrame(
            {
                "data_source": ["AIS"],
                "harbour_name": ["Kiel"],
                "severity": ["low"],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "charts.png"
            render_lagebild_charts(df, path)
            self.assertGreater(path.stat().st_size, 1024)


class TestLagebildMap(unittest.TestCase):
    def test_render_map_png(self) -> None:
        import tempfile

        df = pd.DataFrame(
            {
                "harbour_name": ["Kiel", "Kiel", "Rostock"],
                "severity": ["high", "medium", "low"],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "map.png"
            render_lagebild_map(
                df,
                path,
                comparison_text="NEW | LiDAR near Rostock",
                dataset_summary="3 events; busiest harbours — Kiel (2)",
            )
            self.assertTrue(path.is_file())
            self.assertGreater(path.stat().st_size, 500)
            self.assertIn(basemap_backend(), ("cartopy", "fallback"))


class TestMarkdownPdfHelpers(unittest.TestCase):
    def test_normalize_bold_numbered_heading(self) -> None:
        self.assertEqual(
            normalize_heading_line("**1. Executive Summary**"),
            "EXECUTIVE SUMMARY",
        )
        num, title = parse_section_heading("**1. Executive Summary**")
        self.assertEqual(num, "1")
        self.assertEqual(title, "EXECUTIVE SUMMARY")
        self.assertEqual(
            normalize_heading_line("## 2. Threat Matrix"),
            "THREAT MATRIX",
        )

    def test_parse_markdown_table(self) -> None:
        lines = [
            "| Harbour | Submarine | Level |",
            "|---|---|",
            "| Kiel | Possible | MEDIUM |",
            "| Rostock | No | LOW |",
        ]
        grid = parse_markdown_table(lines)
        self.assertEqual(grid[0], ["Harbour", "Submarine", "Level"])
        self.assertEqual(grid[1][0], "Kiel")

    def test_parse_body_blocks_caption_before_table(self) -> None:
        body = (
            "*Summary Table: harbour contacts*\n"
            "| Harbour | Count |\n"
            "|---|---:|\n"
            "| Kiel | 3 |\n"
            "\n"
            "Narrative with **bold** emphasis."
        )
        blocks = parse_body_blocks(body)
        kinds = [b.kind for b in blocks]
        self.assertEqual(kinds[0], "caption")
        self.assertEqual(kinds[1], "table")
        self.assertEqual(blocks[-1].kind, "paragraph")

    def test_inline_markdown_to_html(self) -> None:
        out = inline_markdown_to_html("**critical** and *watch*")
        self.assertIn("<b>critical</b>", out)
        self.assertIn("<i>watch</i>", out)
        self.assertNotIn("**", out)

    def test_sanitize_unicode_punctuation(self) -> None:
        raw = "Kiel corridor — increased “contacts”…"
        safe = _sanitize_for_pdf(raw)
        safe.encode("latin-1")
        self.assertIn("--", safe)
        self.assertNotIn("\u2014", safe)


class TestNatoPdf(unittest.TestCase):
    def test_format_dtg(self) -> None:
        dt = datetime(2026, 10, 29, 13, 50, tzinfo=timezone.utc)
        self.assertEqual(format_dtg(dt), "291350Z OCT 26")

    def test_writes_nato_sitrep_pdf(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            s = Settings()
            s.artifacts_dir = Path(tmp)
            report = """**1. Executive Summary**
**critical** AIS pattern near Kiel.

*Summary Table: contacts*
| Harbour | Submarine | Level |
|---|---|---|
| Kiel | Possible | MEDIUM |
| Rostock | No | LOW |
"""
            df = pd.DataFrame(
                {
                    "harbour_name": ["Kiel", "Rostock"],
                    "severity": ["medium", "low"],
                }
            )
            path, map_path, charts_path, zebra_path = write_report_pdf(
                report,
                None,
                s,
                datetime(2026, 10, 29, 13, 50, tzinfo=timezone.utc),
                dataset_summary="75 events; busiest harbours — Rostock (12)",
                row_count=75,
                df_used=df,
            )
            self.assertTrue(path.name.startswith("nato_sitrep_"))
            self.assertTrue(path.read_bytes().startswith(b"%PDF"))
            self.assertGreater(path.stat().st_size, 500)
            self.assertIsNotNone(map_path)
            assert map_path is not None
            self.assertGreater(map_path.stat().st_size, 500)
            self.assertIsNotNone(charts_path)
            assert charts_path is not None
            self.assertGreater(charts_path.stat().st_size, 1024)
            self.assertIsNotNone(zebra_path)
            assert zebra_path is not None
            self.assertGreater(zebra_path.stat().st_size, 2048)
            path2, _, _, _ = write_report_pdf(
                "Idle traffic — no change near Greifswald.",
                None,
                s,
                datetime(2026, 10, 29, 14, 0, tzinfo=timezone.utc),
            )
            self.assertTrue(path2.read_bytes().startswith(b"%PDF"))
            try:
                from pypdf import PdfReader

                text = "\n".join(
                    p.extract_text() or "" for p in PdfReader(str(path)).pages
                )
                self.assertNotIn("**", text)
                self.assertNotIn("| Harbour |", text)
                self.assertIn("EXECUTIVE SUMMARY", text.upper())
                self.assertIn("Kiel", text)
            except ImportError:
                pass


class TestConfig(unittest.TestCase):
    def test_defaults_are_placeholders(self) -> None:
        s = Settings()
        self.assertNotIn("cloudera.site", s.llm_base_url)
        self.assertEqual(s.connection_name, "cdw-aw-se-impala")


if __name__ == "__main__":
    unittest.main()
