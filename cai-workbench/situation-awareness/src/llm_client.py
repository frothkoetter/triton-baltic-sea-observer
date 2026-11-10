"""OpenAI-compatible client for Cloudera model serving."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from openai import OpenAI

from .config import Settings


def resolve_api_key(settings: Settings) -> str:
    if settings.api_key:
        return settings.api_key
    if settings.jwt_path.is_file():
        with open(settings.jwt_path, encoding="utf-8") as fh:
            data = json.load(fh)
        token = data.get("access_token") or data.get("token")
        if token:
            return str(token)
    raise ValueError(
        "Set LAGEBILD_API_KEY or place a JWT at LAGEBILD_JWT_PATH (default /tmp/jwt)."
    )


def build_client(settings: Settings) -> OpenAI:
    return OpenAI(base_url=settings.llm_base_url, api_key=resolve_api_key(settings))


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
