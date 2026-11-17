"""OpenAI-compatible NBA narrative (falls back to LAGEBILD_* env)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .config import Settings


def _env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default


@dataclass
class LlmSettings:
    llm_base_url: str = field(
        default_factory=lambda: _env_first(
            "NBA_LLM_BASE_URL", "LAGEBILD_LLM_BASE_URL", default=""
        )
    )
    model_id: str = field(
        default_factory=lambda: _env_first(
            "NBA_MODEL_ID",
            "LAGEBILD_MODEL_ID",
            default="mistralai/mistral-7b-instruct-v0.3",
        )
    )
    api_key: str | None = field(default=None)
    jwt_path: Path = field(
        default_factory=lambda: Path(
            _env_first("NBA_JWT_PATH", "LAGEBILD_JWT_PATH", default="/tmp/jwt")
        )
    )
    temperature: float = field(
        default_factory=lambda: float(os.environ.get("NBA_LLM_TEMPERATURE", "0.2"))
    )
    max_tokens: int = field(
        default_factory=lambda: int(os.environ.get("NBA_LLM_MAX_TOKENS", "1200"))
    )
    azure_api_version: str | None = field(
        default_factory=lambda: os.environ.get("NBA_AZURE_API_VERSION")
        or os.environ.get("LAGEBILD_AZURE_API_VERSION")
    )


def normalize_api_key(raw: str) -> str:
    key = raw.strip().strip("\ufeff")
    if key.lower().startswith("bearer "):
        key = key[7:].strip()
    if not key:
        raise ValueError("API key is empty after stripping whitespace.")
    return key


def resolve_api_key(llm: LlmSettings) -> str:
    env_key = os.environ.get("NBA_API_KEY") or os.environ.get("LAGEBILD_API_KEY")
    if env_key:
        return normalize_api_key(env_key)
    if llm.api_key:
        return normalize_api_key(llm.api_key)
    if llm.jwt_path.is_file():
        data = json.loads(llm.jwt_path.read_text(encoding="utf-8"))
        token = data.get("access_token") or data.get("token")
        if token:
            return normalize_api_key(str(token))
    raise ValueError(
        "Set NBA_API_KEY or LAGEBILD_API_KEY or JWT at NBA_JWT_PATH / LAGEBILD_JWT_PATH."
    )


def llm_configured() -> bool:
    try:
        LlmSettings()
        if not _env_first("NBA_LLM_BASE_URL", "LAGEBILD_LLM_BASE_URL"):
            return False
        resolve_api_key(LlmSettings())
        return True
    except ValueError:
        return False


def _normalize_azure_base_url(base_url: str) -> tuple[str, str | None]:
    version = None
    if "api-version=" in base_url:
        from urllib.parse import parse_qs, urlsplit, urlunsplit

        split = urlsplit(base_url)
        qs = parse_qs(split.query)
        if "api-version" in qs:
            version = qs["api-version"][0]
        base_url = urlunsplit((split.scheme, split.netloc, split.path, "", ""))
    for suffix in ("/chat/completions", "/v1/chat/completions"):
        if base_url.rstrip("/").endswith(suffix):
            base_url = base_url.rstrip("/")[: -len(suffix)]
    return base_url.rstrip("/"), version


def chat_completion_text(
    llm: LlmSettings,
    messages: list[dict[str, str]],
    *,
    on_token: Callable[[str], None] | None = None,
) -> str:
    from openai import OpenAI

    base_url = llm.llm_base_url.rstrip("/")
    api_version = llm.azure_api_version
    if "openai.azure.com" in base_url or "cognitiveservices.azure.com" in base_url:
        base_url, parsed_ver = _normalize_azure_base_url(base_url)
        api_version = api_version or parsed_ver
        kwargs: dict = {
            "base_url": base_url,
            "api_key": resolve_api_key(llm),
        }
        if api_version:
            kwargs["default_query"] = {"api-version": api_version}
        client = OpenAI(**kwargs)
    else:
        client = OpenAI(base_url=base_url, api_key=resolve_api_key(llm))
    completion = client.chat.completions.create(
        model=llm.model_id,
        messages=messages,
        temperature=llm.temperature,
        top_p=0.7,
        max_tokens=llm.max_tokens,
        stream=False,
    )
    text = completion.choices[0].message.content or ""
    if on_token and text:
        on_token(text)
    return text


def build_nba_messages(context: dict) -> list[dict[str, str]]:
    system = (
        "You are a NATO maritime ops advisor. Write 2 short paragraphs in German: "
        "(1) situation tied to the alert, (2) recommended COA with legal/OPSEC caveats. "
        "Do not invent asset positions. Use only provided facts."
    )
    user = json.dumps(context, ensure_ascii=False, indent=2)
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]