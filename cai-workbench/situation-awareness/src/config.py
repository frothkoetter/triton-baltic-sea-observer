"""Environment-driven configuration for the Lagebild pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Settings:
    """Runtime settings from environment variables (no secrets in defaults)."""

    connection_name: str = field(
        default_factory=lambda: os.environ.get(
            "LAGEBILD_CONNECTION_NAME", "cdw-aw-se-impala"
        )
    )
    sql_override: str | None = field(
        default_factory=lambda: os.environ.get("LAGEBILD_SQL") or None
    )
    trino_catalog: str = field(
        default_factory=lambda: os.environ.get("LAGEBILD_TRINO_CATALOG", "iceberg")
    )
    llm_base_url: str = field(
        default_factory=lambda: os.environ.get(
            "LAGEBILD_LLM_BASE_URL",
            "https://your-cml-cluster.example/namespaces/serving-default/endpoints/mistral7binstruct/v1",
        )
    )
    model_id: str = field(
        default_factory=lambda: os.environ.get(
            "LAGEBILD_MODEL_ID", "mistralai/mistral-7b-instruct-v0.3"
        )
    )
    api_key: str | None = field(
        default_factory=lambda: os.environ.get("LAGEBILD_API_KEY") or None
    )
    jwt_path: Path = field(
        default_factory=lambda: Path(
            os.environ.get("LAGEBILD_JWT_PATH", "/tmp/jwt")
        )
    )
    max_char_limit: int = field(
        default_factory=lambda: int(os.environ.get("LAGEBILD_MAX_CHAR_LIMIT", "32000"))
    )
    sample_csv: Path | None = field(default=None)
    artifacts_dir: Path = field(default_factory=lambda: Path("artifacts/reports"))
    insert_sql_enabled: bool = field(
        default_factory=lambda: _env_bool("LAGEBILD_INSERT_SQL", False)
    )
    trino_host: str | None = field(
        default_factory=lambda: os.environ.get("LAGEBILD_TRINO_HOST") or None
    )
    trino_port: int = field(
        default_factory=lambda: int(os.environ.get("LAGEBILD_TRINO_PORT", "443"))
    )
    trino_user: str | None = field(
        default_factory=lambda: os.environ.get("LAGEBILD_TRINO_USER") or None
    )
    trino_password: str | None = field(
        default_factory=lambda: os.environ.get("LAGEBILD_TRINO_PASSWORD") or None
    )
    trino_http_scheme: str = field(
        default_factory=lambda: os.environ.get("LAGEBILD_TRINO_HTTP_SCHEME", "https")
    )
    trino_schema: str = field(
        default_factory=lambda: os.environ.get("LAGEBILD_TRINO_SCHEMA", "defense")
    )
    summary_table: str = field(
        default_factory=lambda: os.environ.get(
            "LAGEBILD_SUMMARY_TABLE", "situation_awareness_summary"
        )
    )
    temperature: float = field(
        default_factory=lambda: float(os.environ.get("LAGEBILD_LLM_TEMPERATURE", "0.2"))
    )
    max_tokens: int = field(
        default_factory=lambda: int(os.environ.get("LAGEBILD_LLM_MAX_TOKENS", "2048"))
    )

    def resolve_sample_csv(self, project_root: Path) -> Path:
        env_path = os.environ.get("LAGEBILD_SAMPLE_CSV")
        if env_path:
            return Path(env_path)
        return project_root.parent / "data" / "lagebild" / "sample_lagebild.csv"
