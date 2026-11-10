"""Lagebild pipeline orchestration (no side effects on import)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .compare_reports import rule_based_theme_diff
from .config import Settings
from .data_access import (
    dataframe_to_markdown_bounded,
    generate_summary_insert_sql,
    lagebild_dataset_summary,
    load_lagebild_dataframe,
)
from .llm_client import chat_completion_text
from .prompts import build_comparison_prompt, build_situation_prompt
from .report_pdf import load_report_texts, write_report_pdf

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class PipelineResult:
    report_text: str
    comparison_text: str | None
    dataset_summary: str
    pdf_path: Path | None
    data_source: str
    sql_insert: str | None
    row_count: int


def run_pipeline(
    settings: Settings | None = None,
    *,
    skip_llm: bool = False,
    skip_comparison: bool = False,
    on_llm_token: Callable[[str], None] | None = None,
) -> PipelineResult:
    settings = settings or Settings()
    if settings.sample_csv is None:
        settings.sample_csv = settings.resolve_sample_csv(PROJECT_ROOT)

    dataframe, data_source = load_lagebild_dataframe(settings, PROJECT_ROOT)
    md_table, df_used = dataframe_to_markdown_bounded(dataframe, settings.max_char_limit)
    dataset_summary = lagebild_dataset_summary(df_used)
    report_time = datetime.now(timezone.utc)
    report_time_str = report_time.strftime("%Y-%m-%d %H:%M:%S UTC")

    if skip_llm:
        report_text = (
            f"Situation Awareness Summary ({report_time_str})\n\n"
            f"[LLM skipped — dataset only]\n{dataset_summary}\n"
        )
        comparison_text = None
    else:
        messages = build_situation_prompt(report_time_str, dataset_summary, md_table)
        report_text = chat_completion_text(
            settings, messages, stream=bool(on_llm_token), on_token=on_llm_token
        )
        comparison_text = None
        if not skip_comparison:
            report_24h = load_report_texts(settings.artifacts_dir, 24.0, report_time)
            report_48h = load_report_texts(settings.artifacts_dir, 48.0, report_time)
            try:
                cmp_messages = build_comparison_prompt(
                    report_text, report_24h, report_48h, report_time_str
                )
                comparison_text = chat_completion_text(settings, cmp_messages, stream=False)
            except Exception:
                if report_24h:
                    deltas = rule_based_theme_diff(report_text, report_24h)
                    comparison_text = "\n".join(d.line for d in deltas)
                else:
                    comparison_text = "UNCHANGED | No prior report for comparison."

    pdf_path = write_report_pdf(report_text, comparison_text, settings, report_time)
    sql_insert = None
    if settings.insert_sql_enabled:
        sql_insert = generate_summary_insert_sql(report_text, report_time)

    return PipelineResult(
        report_text=report_text,
        comparison_text=comparison_text,
        dataset_summary=dataset_summary,
        pdf_path=pdf_path,
        data_source=data_source,
        sql_insert=sql_insert,
        row_count=len(df_used),
    )


def execute_sql_insert(settings: Settings, sql_insert: str) -> None:
    """Optional: persist lines to defense.Situation_Awareness_Summary via CML."""
    import cml.data_v1 as cmldata

    conn = cmldata.get_connection(settings.connection_name)
    try:
        with conn.get_base_connection() as base_conn:
            with base_conn.cursor() as cursor:
                cursor.execute(sql_insert)
    finally:
        conn.close()
