"""Fetch and prepare defense.lagebild data for LLM context."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import Settings


GERMAN_HARBOUR_FILTER_SQL = """
SELECT * FROM {qualified_lagebild}
WHERE harbour_name IN (
    SELECT name FROM {qualified_harbours} WHERE country = 'Germany'
)
""".strip()


def qualified_table(catalog: str, schema: str, table: str) -> str:
    """Build Trino-style qualified name; Impala often uses schema.table only."""
    if catalog and catalog.lower() not in ("", "none", "impala"):
        return f"{catalog}.{schema}.{table}"
    return f"{schema}.{table}"


def default_lagebild_sql(settings: Settings) -> str:
    if settings.sql_override:
        return settings.sql_override.strip()
    lagebild = qualified_table(settings.trino_catalog, "defense", "lagebild")
    harbours = qualified_table(settings.trino_catalog, "defense", "baltic_sea_harbours")
    return GERMAN_HARBOUR_FILTER_SQL.format(
        qualified_lagebild=lagebild,
        qualified_harbours=harbours,
    )


def lagebild_dataset_summary(df: pd.DataFrame) -> str:
    """Short header for the LLM from defense.lagebild row counts."""
    if df.empty:
        return "No events in the last 24h for the selected German harbours."

    n = len(df)
    parts = [f"{n} events in the reporting window"]
    src_col = None
    for candidate in ("data_source", "Data_Source"):
        if candidate in df.columns:
            src_col = candidate
            break
    if src_col is None:
        return parts[0]

    counts = df[src_col].fillna("Unknown").astype(str).value_counts()
    by_source = ", ".join(f"{name}: {int(count)}" for name, count in counts.items())
    parts.append(f"by Data_Source — {by_source}")
    if "harbour_name" in df.columns:
        top = df["harbour_name"].value_counts().head(3)
        hot = ", ".join(f"{name} ({int(c)})" for name, c in top.items())
        parts.append(f"busiest harbours — {hot}")
    return "; ".join(parts)


def dataframe_to_markdown_bounded(
    df: pd.DataFrame, max_char_limit: int
) -> tuple[str, pd.DataFrame]:
    """Convert DataFrame to markdown, truncating rows if over char budget."""
    if df.empty:
        return "(no rows returned)", df

    full_md = df.to_markdown(index=False)
    if len(full_md) <= max_char_limit:
        return full_md, df

    n_rows = len(df)
    while n_rows > 1:
        avg_row_length = max(len(full_md) / n_rows, 1.0)
        n_rows = max(int(max_char_limit / avg_row_length) - 1, 1)
        trimmed = df.head(n_rows)
        full_md = trimmed.to_markdown(index=False)
        if len(full_md) <= max_char_limit:
            return full_md, trimmed
    return full_md[:max_char_limit], df.head(1)


def load_sample_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Sample CSV not found: {path}")
    return pd.read_csv(path)


def fetch_from_cml(connection_name: str, sql: str) -> pd.DataFrame:
    import cml.data_v1 as cmldata

    conn = cmldata.get_connection(connection_name)
    try:
        return conn.get_pandas_dataframe(sql)
    finally:
        conn.close()


def load_lagebild_dataframe(settings: Settings, project_root: Path) -> tuple[pd.DataFrame, str]:
    """
    Load lagebild data from CML connection or fall back to sample CSV.

    Returns (dataframe, source_label).
    """
    sql = default_lagebild_sql(settings)
    use_sample = _env_bool("LAGEBILD_USE_SAMPLE", False)

    if not use_sample:
        try:
            df = fetch_from_cml(settings.connection_name, sql)
            return df, f"cml:{settings.connection_name}"
        except Exception as exc:
            sample_path = settings.resolve_sample_csv(project_root)
            if sample_path.is_file():
                return load_sample_csv(sample_path), f"sample_csv (CML failed: {exc})"
            raise

    sample_path = settings.resolve_sample_csv(project_root)
    return load_sample_csv(sample_path), "sample_csv"


def generate_summary_insert_sql(summary_text: str, timestamp: Any) -> str:
    """Bulk INSERT for defense.Situation_Awareness_Summary (optional persistence)."""
    values: list[str] = []
    sql_timestamp = timestamp.strftime("'%Y-%m-%d %H:%M:%S'")
    for line_num, line_content in enumerate(summary_text.splitlines()):
        if not line_content.strip():
            continue
        escaped = line_content.replace("'", "''")
        values.append(f"({sql_timestamp}, {line_num + 1}, '{escaped}')")
    if not values:
        return "-- No content generated for insertion."
    return (
        "INSERT INTO defense.Situation_Awareness_Summary "
        "(summary_timestamp, summare_line, summary_text) VALUES \n"
        + ",\n".join(values)
        + ";"
    )


def _env_bool(name: str, default: bool = False) -> bool:
    import os

    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")
