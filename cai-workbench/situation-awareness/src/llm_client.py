"""OpenAI-compatible client for Cloudera model serving."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from openai import OpenAI

from .config import Settings


def normalize_api_key(raw: str) -> str:
    """Strip whitespace and an accidental ``Bearer `` prefix (OpenAI client adds it)."""
    key = raw.strip()
    if key.lower().startswith("bearer "):
        key = key[7:].strip()
    if not key:
        raise ValueError("API key is empty after stripping whitespace.")
    return key


def resolve_api_key(settings: Settings) -> str:
    if settings.api_key:
        return normalize_api_key(settings.api_key)
    if settings.jwt_path.is_file():
        with open(settings.jwt_path, encoding="utf-8") as fh:
            data = json.load(fh)
        token = data.get("access_token") or data.get("token")
        if token:
            return normalize_api_key(str(token))
    raise ValueError(
        "Set LAGEBILD_API_KEY or place a JWT at LAGEBILD_JWT_PATH (default /tmp/jwt)."
    )


def _normalize_azure_base_url(base_url: str) -> tuple[str, str | None]:
    """Strip /chat/completions and api-version query from a pasted Azure endpoint."""
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


def build_client(settings: Settings) -> OpenAI:
    base_url = settings.llm_base_url.rstrip("/")
    api_version = settings.azure_api_version
    if "openai.azure.com" in base_url or "cognitiveservices.azure.com" in base_url:
        base_url, parsed_ver = _normalize_azure_base_url(base_url)
        api_version = api_version or parsed_ver
        kwargs: dict = {
            "base_url": base_url,
            "api_key": resolve_api_key(settings),
        }
        if api_version:
            kwargs["default_query"] = {"api-version": api_version}
        return OpenAI(**kwargs)
    return OpenAI(base_url=base_url, api_key=resolve_api_key(settings))


def chat_completion_text(
    settings: Settings,
    messages: list[dict[str, str]],
    *,
    stream: bool = False,
    on_token: Callable[[str], None] | None = None,
) -> str:
    client = build_client(settings)
    if not stream:
        completion = client.chat.completions.create(
            model=settings.model_id,
            messages=messages,
            temperature=settings.temperature,
            top_p=0.7,
            max_tokens=settings.max_tokens,
            stream=False,
        )
        return completion.choices[0].message.content or ""

    parts: list[str] = []
    completion = client.chat.completions.create(
        model=settings.model_id,
        messages=messages,
        temperature=settings.temperature,
        top_p=0.7,
        max_tokens=settings.max_tokens,
        stream=True,
    )
    for chunk in completion:
        content = chunk.choices[0].delta.content
        if content:
            parts.append(content)
            if on_token:
                on_token(content)
    return "".join(parts)
