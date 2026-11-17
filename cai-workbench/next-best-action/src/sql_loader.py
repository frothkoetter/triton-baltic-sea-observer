"""Load parameterized SQL snippets from sql/alerts/."""

from __future__ import annotations

from pathlib import Path

_SQL_ROOT = Path(__file__).resolve().parent.parent / "sql" / "alerts"


def qualified_schema(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}"


def render_sql(filename: str, *, qualified: str, limit: int = 5) -> str:
    path = _SQL_ROOT / filename
    if not path.is_file():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8")
    return text.format(qualified=qualified, limit=limit)