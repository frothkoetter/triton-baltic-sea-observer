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
from src.report_pdf import (
    format_dtg,
    inline_markdown_to_html,
    normalize_heading_line,
    parse_body_blocks,
    parse_markdown_table,
    parse_section_heading,
    write_report_pdf,
)
from datetime import datetime, timezone


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
            path = write_report_pdf(
                report,
                None,
                s,
                datetime(2026, 10, 29, 13, 50, tzinfo=timezone.utc),
                dataset_summary="75 events; busiest harbours — Rostock (12)",
                row_count=75,
            )
            self.assertTrue(path.name.startswith("nato_sitrep_"))
            self.assertTrue(path.read_bytes().startswith(b"%PDF"))
            self.assertGreater(path.stat().st_size, 500)
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
