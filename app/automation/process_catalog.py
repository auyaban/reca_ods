from __future__ import annotations

import difflib
import os
from pathlib import Path

from app.config import get_settings
from app.utils.text import normalize_search_text


def default_process_templates_dir() -> Path:
    settings = get_settings()
    configured = str(settings.automation_process_templates_dir or "").strip()
    if configured:
        return Path(os.path.expandvars(configured)).expanduser()
    return Path.home() / "Desktop" / "RECA_INCLUSION_LABORAL" / "templates"


def list_process_template_names() -> list[str]:
    templates_dir = default_process_templates_dir()
    if not templates_dir.exists() or not templates_dir.is_dir():
        return []

    names: list[str] = []
    for path in templates_dir.iterdir():
        if not path.is_file():
            continue
        stem = path.stem.strip()
        if stem:
            names.append(stem)
    return sorted(names, key=lambda item: item.lower())


def guess_process_from_filename(filename: str, template_names: list[str] | None = None) -> tuple[str, float]:
    source = normalize_search_text(Path(str(filename or "")).stem)
    if not source:
        return "", 0.0

    candidates = template_names or list_process_template_names()
    best_name = ""
    best_score = 0.0
    source_tokens = set(source.split())

    for candidate in candidates:
        normalized_candidate = normalize_search_text(candidate)
        if not normalized_candidate:
            continue
        if normalized_candidate == source:
            return candidate, 1.0
        if normalized_candidate in source or source in normalized_candidate:
            score = 0.95
        else:
            candidate_tokens = set(normalized_candidate.split())
            overlap = len(source_tokens & candidate_tokens) / max(len(source_tokens), len(candidate_tokens), 1)
            ratio = difflib.SequenceMatcher(None, source, normalized_candidate).ratio()
            score = max(overlap, ratio)
        if score > best_score:
            best_name = candidate
            best_score = score

    if best_score < 0.45:
        return "", 0.0
    return best_name, round(best_score, 2)
