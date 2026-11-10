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
from src.llm_client import _normalize_azure_base_url


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


class TestConfig(unittest.TestCase):
    def test_defaults_are_placeholders(self) -> None:
        s = Settings()
        self.assertNotIn("cloudera.site", s.llm_base_url)
        self.assertEqual(s.connection_name, "cdw-aw-se-impala")


if __name__ == "__main__":
    unittest.main()
