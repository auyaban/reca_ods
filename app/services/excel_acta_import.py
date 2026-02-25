from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any


MAX_SCAN_ROWS = 260
MAX_SCAN_COLS = 70


def _norm(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text


def _clean_text(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _clean_cedula(value: Any) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    return digits


def _clean_nit(value: Any) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    match = re.search(r"\b\d{6,12}(?:-\d)?\b", raw)
    return match.group(0) if match else raw


def _to_iso_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = _clean_text(value)
    if not text:
        return ""

    text = text.split(" ")[0]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _sheet_matrix(path: str) -> list[tuple[str, list[list[Any]]]]:
    try:
        from openpyxl import load_workbook
    except Exception as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError("No se pudo importar openpyxl.") from exc

    wb = load_workbook(path, data_only=True, read_only=True)
    result: list[tuple[str, list[list[Any]]]] = []
    for ws in wb.worksheets:
        rows: list[list[Any]] = []
        for row in ws.iter_rows(min_row=1, max_row=MAX_SCAN_ROWS, max_col=MAX_SCAN_COLS, values_only=True):
            rows.append(list(row))
        result.append((ws.title, rows))
    return result


def _is_likely_label(text: str) -> bool:
    if not text:
        return False
    if len(text) > 70:
        return False
    return text.endswith(":") or any(token in text for token in ("nit", "fecha", "modalidad", "profesional"))


def _first_neighbor_value(rows: list[list[Any]], r: int, c: int) -> Any:
    # Right side first.
    for dc in range(1, 9):
        cc = c + dc
        if cc >= len(rows[r]):
            break
        value = rows[r][cc]
        if _clean_text(value):
            norm = _norm(value)
            if not _is_likely_label(norm):
                return value

    # Next rows under same/adjacent columns
    for dr in range(1, 3):
        rr = r + dr
        if rr >= len(rows):
            break
        for dc in (0, 1):
            cc = c + dc
            if cc >= len(rows[rr]):
                continue
            value = rows[rr][cc]
            if _clean_text(value):
                norm = _norm(value)
                if not _is_likely_label(norm):
                    return value
    return None


def _find_labeled_value(rows: list[list[Any]], label_tokens: tuple[str, ...], starts_with: bool = False) -> Any:
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            norm = _norm(value)
            if not norm:
                continue
            if len(norm) > 55:
                continue
            matched = any(norm.startswith(token) for token in label_tokens) if starts_with else any(
                token in norm for token in label_tokens
            )
            if matched:
                neighbor = _first_neighbor_value(rows, r, c)
                if _clean_text(neighbor):
                    return neighbor
    return None


def _is_person_candidate(value: Any) -> bool:
    text = _clean_text(value)
    if not text or len(text) < 3:
        return False
    norm = _norm(text)
    if re.search(r"(https?://|www\.|@[a-z0-9._-]+|\.com\b|\.org\b|\.net\b|\.co\b)", norm):
        return False
    banned = (
        "codigo",
        "tema",
        "version",
        "correo",
        "telefono",
        "nit",
        "empresa",
        "fecha",
        "modalidad",
        "objetivo",
        "cargo",
        "sede",
        "asesor",
        "direccion",
        "ciudad",
    )
    if any(token in norm for token in banned):
        return False
    return bool(re.search(r"[a-zA-ZáéíóúñÁÉÍÓÚÑ]", text))


def _extract_profesional(rows: list[list[Any]]) -> str:
    labels = (
        "profesional asignado reca",
        "profesional asignado",
    )
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            norm = _norm(value)
            if not norm or len(norm) > 60:
                continue
            if not any(norm == token or norm.startswith(token + ":") for token in labels):
                continue
            candidate = _first_neighbor_value(rows, r, c)
            if _is_person_candidate(candidate):
                return _clean_text(candidate)
    return ""


def _extract_profesional_from_asistentes(rows: list[list[Any]]) -> str:
    asist_row = -1
    for r, row in enumerate(rows):
        for value in row:
            norm = _norm(value)
            if not norm:
                continue
            if re.search(r"\b\d+\s*\.\s*asistentes\b", norm) or norm == "asistentes" or norm.endswith(" asistentes"):
                asist_row = r
                break
        if asist_row >= 0:
            break

    if asist_row < 0:
        return ""

    for rr in range(asist_row + 1, min(asist_row + 14, len(rows))):
        row = rows[rr]
        if not row:
            continue
        row_norm = [_norm(cell) for cell in row]
        has_nombre_label = any("nombre completo" in cell for cell in row_norm if cell)
        if not has_nombre_label:
            continue

        # Case 1: inline text "Nombre completo: Juan Perez"
        for raw in row:
            text = _clean_text(raw)
            norm = _norm(raw)
            if "nombre completo" in norm and ":" in text:
                inline = text.split(":", 1)[1].strip()
                if _is_person_candidate(inline):
                    return inline

        # Case 2: value in adjacent columns in same row
        for raw in row:
            value = _clean_text(raw)
            norm = _norm(raw)
            if not value:
                continue
            if "nombre completo" in norm:
                continue
            if "cargo" in norm:
                continue
            if _is_person_candidate(value):
                return value
    return ""


def _extract_nit(rows: list[list[Any]]) -> str:
    nit_labels = ("numero de nit", "nit empresa", "razon social / nit", "nit:")
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            raw = _clean_text(value)
            norm = _norm(value)
            if not raw:
                continue
            if len(norm) > 55:
                continue
            if not any(token in norm for token in nit_labels):
                continue
            own = _clean_nit(raw)
            if re.fullmatch(r"\d{6,12}(?:-\d)?", own):
                return own
            for dc in range(1, 9):
                cc = c + dc
                if cc >= len(row):
                    break
                candidate = _clean_nit(row[cc])
                if re.fullmatch(r"\d{6,12}(?:-\d)?", candidate):
                    return candidate
            for dr in range(1, 2):
                rr = r + dr
                if rr >= len(rows):
                    break
                candidate = _clean_nit(rows[rr][c] if c < len(rows[rr]) else "")
                if re.fullmatch(r"\d{6,12}(?:-\d)?", candidate):
                    return candidate
    return ""


def _extract_participants(rows: list[list[Any]]) -> list[dict[str, str]]:
    participants: list[dict[str, str]] = []
    name_headers = (
        "nombre vinculado",
        "nombre completo",
        "nombres y apellidos",
        "nombre del vinculado",
        "nombre usuario",
        "nombre participante",
    )
    ced_headers = ("cedula", "c.c", "cc", "documento")

    for idx, row in enumerate(rows):
        normalized = [_norm(cell) for cell in row]
        name_col = next((i for i, cell in enumerate(normalized) if any(token in cell for token in name_headers)), None)
        if name_col is None:
            continue
        ced_col = next((i for i, cell in enumerate(normalized) if any(token in cell for token in ced_headers)), None)
        if ced_col is None:
            continue

        empty_streak = 0
        for j in range(idx + 1, min(idx + 120, len(rows))):
            current = rows[j]
            ced_raw = current[ced_col] if ced_col is not None and ced_col < len(current) else ""
            ced = _clean_cedula(ced_raw)

            if not ced:
                empty_streak += 1
                if empty_streak >= 4:
                    break
                continue
            empty_streak = 0

            if ced and len(ced) < 5:
                continue

            participants.append({"cedula_usuario": ced})
    return participants


def _dedupe_participants(participants: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for item in participants:
        ced = _clean_cedula(item.get("cedula_usuario", ""))
        if not ced:
            continue
        key = ced.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append({"cedula_usuario": ced})
    return unique


def parse_acta_excel(file_path: str) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise RuntimeError(f"No existe el archivo: {file_path}")

    sheets = _sheet_matrix(str(path))

    nit = ""
    empresa = ""
    fecha_servicio = ""
    profesional = ""
    modalidad = ""
    participants: list[dict[str, str]] = []

    for _, rows in sheets:
        if not nit:
            nit = _extract_nit(rows)
        if not empresa:
            empresa_value = _find_labeled_value(
                rows,
                ("nombre de la empresa", "razon social"),
                starts_with=True,
            )
            empresa = _clean_text(empresa_value)
        if not fecha_servicio:
            fecha_value = _find_labeled_value(
                rows,
                ("fecha de la visita", "fecha servicio", "fecha de firma de contrato", "fecha firma de contrato"),
                starts_with=True,
            )
            fecha_servicio = _to_iso_date(fecha_value)
        if not profesional:
            profesional = _extract_profesional_from_asistentes(rows) or _extract_profesional(rows)
        if not modalidad:
            modalidad_value = _find_labeled_value(
                rows,
                ("modalidad:", "modalidad"),
                starts_with=True,
            )
            modalidad = _clean_text(modalidad_value)

        participants.extend(_extract_participants(rows))

    participants = _dedupe_participants(participants)

    warnings: list[str] = []
    if not nit:
        warnings.append("No se detecto NIT en el archivo.")
    if not fecha_servicio:
        warnings.append("No se detecto fecha de servicio en formato valido.")

    return {
        "file_path": str(path),
        "nit_empresa": nit,
        "nombre_empresa": empresa,
        "fecha_servicio": fecha_servicio,
        "nombre_profesional": profesional,
        "modalidad_servicio": modalidad,
        "participantes": participants,
        "warnings": warnings,
    }
