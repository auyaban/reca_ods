from __future__ import annotations

import re
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.google_sheets_client import (
    download_drive_file,
    export_spreadsheet_to_excel,
    extract_drive_file_id,
    get_drive_file_metadata,
)
from app.utils.text import normalize_text

MAX_SCAN_ROWS = 260
MAX_SCAN_COLS = 70
_GOOGLE_SPREADSHEET_MIME = "application/vnd.google-apps.spreadsheet"
_EXCEL_EXTENSIONS = {".xlsx", ".xlsm"}
_PDF_EXTENSIONS = {".pdf"}
_PDF_BLOCK_RE = re.compile(
    r"(?ms)(?:^|\n)\s*(?P<idx>[1-9])\s+(?P<body>.*?)(?=(?:\n\s*[1-9]\s+[A-ZÁÉÍÓÚÑ])|\Z)"
)


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


def _clean_name(value: Any) -> str:
    text = _clean_text(value)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" .:-")


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
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError("No se pudo importar openpyxl.") from exc

    wb = load_workbook(path, data_only=True, read_only=True)
    try:
        result: list[tuple[str, list[list[Any]]]] = []
        for ws in wb.worksheets:
            rows: list[list[Any]] = []
            for row in ws.iter_rows(min_row=1, max_row=MAX_SCAN_ROWS, max_col=MAX_SCAN_COLS, values_only=True):
                rows.append(list(row))
            result.append((ws.title, rows))
        return result
    finally:
        wb.close()


def _is_likely_label(text: str) -> bool:
    if not text:
        return False
    if len(text) > 70:
        return False
    return text.endswith(":") or any(token in text for token in ("nit", "fecha", "modalidad", "profesional"))


def _first_neighbor_value(rows: list[list[Any]], r: int, c: int) -> Any:
    # Right side first.
    for dc in range(1, 16):
        cc = c + dc
        if cc >= len(rows[r]):
            break
        value = rows[r][cc]
        if _clean_text(value):
            if len(_clean_text(value)) > 60:
                continue
            norm = normalize_text(value)
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
                norm = normalize_text(value)
                if not _is_likely_label(norm):
                    return value
    return None


def _find_labeled_value(rows: list[list[Any]], label_tokens: tuple[str, ...], starts_with: bool = False) -> Any:
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            norm = normalize_text(value)
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
    norm = normalize_text(text)
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
    return any(ch.isalpha() for ch in text)


def _extract_profesional(rows: list[list[Any]]) -> str:
    labels = (
        "profesional asignado reca",
        "profesional asignado",
    )
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            norm = normalize_text(value)
            if not norm or len(norm) > 60:
                continue
            if not any(norm == token or norm.startswith(token + ":") for token in labels):
                continue
            candidate = _first_neighbor_value(rows, r, c)
            if _is_person_candidate(candidate):
                return _clean_text(candidate)
    return ""


def _extract_asistentes_candidates(rows: list[list[Any]]) -> list[str]:
    asist_row = -1
    for r, row in enumerate(rows):
        for value in row:
            norm = normalize_text(value)
            if not norm:
                continue
            if re.search(r"\b\d+\s*\.\s*asistentes\b", norm) or norm == "asistentes" or norm.endswith(" asistentes"):
                asist_row = r
                break
        if asist_row >= 0:
            break

    if asist_row < 0:
        return []

    candidates: list[str] = []
    for rr in range(asist_row + 1, min(asist_row + 30, len(rows))):
        row = rows[rr]
        if not row:
            continue
        row_norm = [normalize_text(cell) for cell in row]
        has_nombre_label = any("nombre completo" in cell for cell in row_norm if cell)
        if not has_nombre_label:
            continue

        for raw in row:
            text = _clean_text(raw)
            norm = normalize_text(raw)
            if "nombre completo" in norm and ":" in text:
                inline = text.split(":", 1)[1].strip()
                if _is_person_candidate(inline):
                    candidates.append(inline)
                    break

        for raw in row:
            value = _clean_text(raw)
            norm = normalize_text(raw)
            if not value:
                continue
            if "nombre completo" in norm:
                continue
            if "cargo" in norm or "firma" in norm:
                continue
            if _is_person_candidate(value) and value not in candidates:
                candidates.append(value)
                break

    return candidates


def _extract_profesional_from_asistentes(rows: list[list[Any]]) -> str:
    candidates = _extract_asistentes_candidates(rows)
    return candidates[0] if candidates else ""


def _extract_nit(rows: list[list[Any]]) -> str:
    nit_labels = ("numero de nit", "nit empresa", "razon social / nit", "nit:")
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            raw = _clean_text(value)
            norm = normalize_text(value)
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
                candidate = re.sub(r"[.\s]", "", _clean_nit(row[cc]))
                if re.fullmatch(r"\d{6,12}(?:-\d)?", candidate):
                    return candidate
            for dr in range(1, 4):
                rr = r + dr
                if rr >= len(rows):
                    break
                candidate = re.sub(r"[.\s]", "", _clean_nit(rows[rr][c] if c < len(rows[rr]) else ""))
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
        "nombre oferente",
    )
    ced_headers = ("cedula", "c.c", "cc", "documento")

    for idx, row in enumerate(rows):
        normalized = [normalize_text(cell) for cell in row]
        name_col = next((i for i, cell in enumerate(normalized) if any(token in cell for token in name_headers)), None)
        if name_col is None:
            continue
        ced_col = next((i for i, cell in enumerate(normalized) if any(token in cell for token in ced_headers)), None)
        if ced_col is None:
            continue
        discapacidad_col = next((i for i, cell in enumerate(normalized) if "discapacidad" in cell), None)
        genero_col = next((i for i, cell in enumerate(normalized) if "genero" in cell or "sexo" in cell), None)

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

            if len(ced) < 5:
                continue

            name_raw = current[name_col] if name_col < len(current) else ""
            discapacidad_raw = current[discapacidad_col] if discapacidad_col is not None and discapacidad_col < len(current) else ""
            genero_raw = current[genero_col] if genero_col is not None and genero_col < len(current) else ""
            participants.append(
                {
                    "nombre_usuario": _clean_name(name_raw),
                    "cedula_usuario": ced,
                    "discapacidad_usuario": _clean_text(discapacidad_raw),
                    "genero_usuario": _clean_text(genero_raw),
                }
            )
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
        unique.append(
            {
                "nombre_usuario": _clean_name(item.get("nombre_usuario", "")),
                "cedula_usuario": ced,
                "discapacidad_usuario": _clean_text(item.get("discapacidad_usuario", "")),
                "genero_usuario": _clean_text(item.get("genero_usuario", "")),
            }
        )
    return unique


def _extract_pdf_text_pages(path: str) -> list[str]:
    try:
        from pypdf import PdfReader
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError("No se pudo importar pypdf.") from exc

    reader = PdfReader(path)
    pages: list[str] = []
    for page in reader.pages:
        raw_text = page.extract_text() or ""
        lines = [_clean_text(line) for line in raw_text.splitlines()]
        page_text = "\n".join(line for line in lines if line)
        pages.append(page_text)
    return pages


def _extract_pdf_value(text: str, pattern: str) -> str:
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return ""
    return _clean_text(match.group(1))


def _split_joined_cedula_percentage(raw_token: str) -> tuple[str, str]:
    match = re.search(r"(?P<digits>\d+)\.(?P<dec>\d{2})%", raw_token)
    if not match:
        return "", ""

    digits = re.sub(r"\D", "", match.group("digits"))
    decimals = match.group("dec")
    candidates: list[tuple[int, int, str, str]] = []
    for pct_len in (2, 1, 3):
        if len(digits) <= pct_len:
            continue
        cedula = digits[:-pct_len]
        percentage_int = digits[-pct_len:]
        if not (6 <= len(cedula) <= 10):
            continue
        percentage_value = int(percentage_int)
        if not (0 <= percentage_value <= 100):
            continue
        score = 0
        if pct_len == 2:
            score += 5
        if len(cedula) >= 8:
            score += 2
        if percentage_value > 0:
            score += 1
        percentage = f"{percentage_value}.{decimals}%"
        candidates.append((score, len(cedula), cedula, percentage))

    if not candidates:
        return "", ""

    _, _, cedula, percentage = max(candidates, key=lambda item: (item[0], item[1]))
    return cedula, percentage


def _extract_pdf_participants(text: str) -> list[dict[str, str]]:
    participants: list[dict[str, str]] = []
    for match in _PDF_BLOCK_RE.finditer(text):
        body = _clean_text(match.group("body"))
        if "discapacidad" not in normalize_text(body):
            continue
        first_line = body.split(" Agente de ", 1)[0]
        first_match = re.search(
            r"^(?P<nombre>.+?)(?P<token>\d{7,13}\.\d{2}%)(?:\s*)Discapacidad\s+(?P<tail>.+)$",
            first_line,
            re.IGNORECASE,
        )
        if not first_match:
            continue

        nombre = _clean_name(first_match.group("nombre"))
        cedula, _pct = _split_joined_cedula_percentage(first_match.group("token"))
        if not cedula:
            continue

        tail = _clean_text(first_match.group("tail"))
        tail_match = re.search(
            r"^(?P<discapacidad>[A-Za-zÁÉÍÓÚÑáéíóúñ ]+?)(?P<telefono>\d[\d ]{6,15})(?P<resultado>Pendiente|Aprobado|No aprobado)",
            tail,
            re.IGNORECASE,
        )
        discapacidad = _clean_text(tail_match.group("discapacidad")) if tail_match else ""
        participants.append(
            {
                "nombre_usuario": nombre,
                "cedula_usuario": cedula,
                "discapacidad_usuario": discapacidad,
                "genero_usuario": "",
            }
        )
    return _dedupe_participants(participants)


def _extract_pdf_asistentes_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    for match in re.finditer(
        r"nombre completo:\s*(.+?)(?=cargo:|nombre completo:|tiene un c[oó]digo de vestimenta|la presente acta|$)",
        text,
        re.IGNORECASE,
    ):
        candidate = _clean_name(match.group(1))
        if candidate and _is_person_candidate(candidate) and candidate not in candidates:
            candidates.append(candidate)
    return candidates


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
    candidatos_profesional: list[str] = []

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
        sheet_candidates = _extract_asistentes_candidates(rows)
        for candidate in sheet_candidates:
            if candidate not in candidatos_profesional:
                candidatos_profesional.append(candidate)
        if not profesional:
            profesional = sheet_candidates[0] if sheet_candidates else _extract_profesional(rows)
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
        "candidatos_profesional": candidatos_profesional,
        "modalidad_servicio": modalidad,
        "participantes": participants,
        "warnings": warnings,
    }


def parse_acta_pdf(file_path: str) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise RuntimeError(f"No existe el archivo: {file_path}")

    pages = _extract_pdf_text_pages(str(path))
    if not pages:
        raise RuntimeError("El PDF no contiene paginas legibles.")

    full_text = "\n".join(page for page in pages if page)
    first_page = pages[0] if pages else ""

    nit = _clean_nit(_extract_pdf_value(first_page, r"n[uú]mero de nit:\s*([0-9.\- ]+)"))
    empresa = _extract_pdf_value(
        first_page,
        r"nombre de la empresa:\s*(.+?)(?:ciudad/municipio:|direccion de la empresa:|numero de nit:)",
    )
    fecha_servicio = _to_iso_date(_extract_pdf_value(first_page, r"fecha de la visita:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})"))
    modalidad = _extract_pdf_value(
        first_page,
        r"modalidad:\s*(.+?)(?:nombre de la empresa:|ciudad/municipio:|direccion de la empresa:)",
    ).rstrip(".")
    asistentes_candidates = _extract_pdf_asistentes_candidates(full_text)
    asesor = _extract_pdf_value(first_page, r"asesor:\s*(.+?)(?:sede compensar:|correo electr[oó]nico:|$)")
    profesional = asistentes_candidates[0] if asistentes_candidates else asesor
    participants = _extract_pdf_participants(full_text)

    warnings: list[str] = []
    if not nit:
        warnings.append("No se detecto NIT en el PDF.")
    if not empresa:
        warnings.append("No se detecto nombre de empresa en el PDF.")
    if not fecha_servicio:
        warnings.append("No se detecto fecha de servicio en formato valido.")
    if not participants:
        warnings.append("No se detectaron oferentes en el PDF.")

    return {
        "file_path": str(path),
        "nit_empresa": nit,
        "nombre_empresa": empresa,
        "fecha_servicio": fecha_servicio,
        "nombre_profesional": profesional,
        "candidatos_profesional": asistentes_candidates or ([asesor] if asesor else []),
        "modalidad_servicio": modalidad,
        "participantes": participants,
        "warnings": warnings,
    }


def _is_google_spreadsheet_reference(source: str) -> bool:
    text = str(source or "").strip().lower()
    return "/spreadsheets/d/" in text


def _is_google_drive_reference(source: str) -> bool:
    text = str(source or "").strip().lower()
    return "drive.google.com" in text and ("/file/d/" in text or "id=" in text)


def _parse_local_source(path: Path) -> dict:
    suffix = path.suffix.lower()
    if suffix in _EXCEL_EXTENSIONS:
        return parse_acta_excel(str(path))
    if suffix in _PDF_EXTENSIONS:
        return parse_acta_pdf(str(path))
    raise RuntimeError("El archivo local no es un Excel compatible (.xlsx/.xlsm) ni un PDF compatible.")


def _normalize_source_type_label(source_type: str, parsed: dict, source_text: str) -> dict:
    parsed["file_path"] = source_text
    parsed["source_type"] = source_type
    return parsed


def parse_acta_source(source: str) -> dict:
    source_text = str(source or "").strip()
    if not source_text:
        raise RuntimeError("Debe indicar la ruta o URL del acta.")

    local_path = Path(source_text).expanduser()
    if local_path.exists():
        return _parse_local_source(local_path)

    temp_path: Path | None = None
    try:
        if _is_google_spreadsheet_reference(source_text):
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                temp_path = Path(tmp.name)
            export_spreadsheet_to_excel(source_text, temp_path)
            parsed = parse_acta_excel(str(temp_path))
            return _normalize_source_type_label("google_sheets", parsed, source_text)

        if _is_google_drive_reference(source_text):
            file_id = extract_drive_file_id(source_text)
            metadata = get_drive_file_metadata(file_id)
            mime_type = str(metadata.get("mimeType") or "").strip().lower()
            name = str(metadata.get("name") or "acta").strip()
            suffix = Path(name).suffix.lower()

            if mime_type == _GOOGLE_SPREADSHEET_MIME:
                with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                    temp_path = Path(tmp.name)
                export_spreadsheet_to_excel(file_id, temp_path)
                parsed = parse_acta_excel(str(temp_path))
                return _normalize_source_type_label("google_sheets", parsed, source_text)

            if suffix not in (_EXCEL_EXTENSIONS | _PDF_EXTENSIONS):
                raise RuntimeError(
                    "El archivo de Drive no es un Google Sheet, un Excel compatible (.xlsx/.xlsm) ni un PDF compatible."
                )

            with tempfile.NamedTemporaryFile(suffix=suffix or ".xlsx", delete=False) as tmp:
                temp_path = Path(tmp.name)
            download_drive_file(file_id, temp_path)
            parser = parse_acta_pdf if suffix in _PDF_EXTENSIONS or mime_type == "application/pdf" else parse_acta_excel
            parsed = parser(str(temp_path))
            return _normalize_source_type_label("google_drive_file", parsed, source_text)
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass

    raise RuntimeError(
        "No se pudo resolver el acta. Usa un archivo Excel/PDF local o una URL valida de Google Drive/Sheets."
    )
