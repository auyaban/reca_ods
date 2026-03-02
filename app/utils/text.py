from __future__ import annotations

import re
import unicodedata
from typing import Any

_MOJIBAKE_REPLACEMENTS: dict[str, str] = {
    "Ã¡": "a",
    "Ã©": "e",
    "Ã­": "i",
    "Ã³": "o",
    "Ãº": "u",
    "Ã": "a",
    "Ã‰": "e",
    "Ã": "i",
    "Ã“": "o",
    "Ãš": "u",
    "Ã±": "n",
    "Ã‘": "n",
}


def normalize_spaces(value: Any) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"\s+", " ", text).strip()


def _fix_mojibake(text: str) -> str:
    for source, target in _MOJIBAKE_REPLACEMENTS.items():
        text = text.replace(source, target)
    return text


def normalize_text(value: Any, *, lowercase: bool = True) -> str:
    text = normalize_spaces(value)
    text = _fix_mojibake(text)
    if lowercase:
        text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip()


def normalize_key(value: Any, *, keep_chars: str = "/") -> str:
    text = normalize_text(value, lowercase=True)
    allowed = set(keep_chars)
    return "".join(ch for ch in text if ch.isalnum() or ch in allowed)


def normalize_search_text(value: Any) -> str:
    text = normalize_text(value, lowercase=True)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()
