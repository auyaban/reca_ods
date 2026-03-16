from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import threading

from app.automation.models import AutomationStagedCase
from app.paths import app_data_dir


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class AutomationStagingRepository:
    _global_lock: threading.Lock = threading.Lock()

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or (app_data_dir() / "automation_staging.json")

    def _ensure_parent(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _load_raw(self) -> list[dict]:
        if not self._path.exists():
            return []
        text = self._path.read_text(encoding="utf-8").strip()
        if not text:
            return []
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"El archivo de staging local está corrupto y no puede leerse: {exc}") from exc
        if not isinstance(payload, list):
            raise RuntimeError("El staging local de automatizacion tiene un formato invalido.")
        return [dict(item) for item in payload]

    def _save_raw(self, rows: list[dict]) -> None:
        self._ensure_parent()
        content = json.dumps(rows, ensure_ascii=True, indent=2)
        tmp_path = self._path.with_suffix(".tmp")
        try:
            tmp_path.write_text(content, encoding="utf-8")
            tmp_path.replace(self._path)
        except Exception:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise

    def _build_case_id(self, *, message: dict, attachment: dict) -> str:
        raw = "|".join(
            [
                str(message.get("message_id") or "").strip(),
                str(message.get("received_at") or "").strip(),
                str(attachment.get("filename") or "").strip().lower(),
            ]
        )
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
        return f"auto-{digest}"

    def save_case(
        self,
        *,
        message: dict,
        attachment: dict,
        analysis: dict,
        suggestion: dict,
        status: str = "pending_review",
    ) -> AutomationStagedCase:
        with AutomationStagingRepository._global_lock:
            rows = self._load_raw()
            case_id = self._build_case_id(message=message, attachment=attachment)
            now = _utc_now_iso()
            record = {
                "case_id": case_id,
                "status": status,
                "created_at": now,
                "updated_at": now,
                "message": dict(message),
                "attachment": dict(attachment),
                "analysis": dict(analysis),
                "suggestion": dict(suggestion),
            }
            existing_index = next((idx for idx, item in enumerate(rows) if str(item.get("case_id") or "") == case_id), None)
            if existing_index is not None:
                record["created_at"] = str(rows[existing_index].get("created_at") or now)
                rows[existing_index] = record
            else:
                rows.append(record)
            self._save_raw(rows)
        return AutomationStagedCase(**record)

    def list_cases(self) -> list[AutomationStagedCase]:
        rows = self._load_raw()
        rows.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return [AutomationStagedCase(**row) for row in rows]

    def get_case(self, case_id: str) -> AutomationStagedCase | None:
        case_id_clean = str(case_id or "").strip()
        if not case_id_clean:
            return None
        rows = self._load_raw()
        row = next((item for item in rows if str(item.get("case_id") or "") == case_id_clean), None)
        return AutomationStagedCase(**row) if row else None

    def update_case(
        self,
        *,
        case_id: str,
        suggestion_updates: dict | None = None,
        status: str | None = None,
    ) -> AutomationStagedCase:
        case_id_clean = str(case_id or "").strip()
        if not case_id_clean:
            raise RuntimeError("Debe indicar case_id para actualizar staging.")

        with AutomationStagingRepository._global_lock:
            rows = self._load_raw()
            existing_index = next((idx for idx, item in enumerate(rows) if str(item.get("case_id") or "") == case_id_clean), None)
            if existing_index is None:
                raise RuntimeError(f"No existe el caso de staging {case_id_clean}.")

            current = dict(rows[existing_index])
            suggestion = dict(current.get("suggestion") or {})
            if suggestion_updates:
                for key, value in dict(suggestion_updates).items():
                    if value is None:
                        continue
                    suggestion[str(key)] = value
            current["suggestion"] = suggestion
            if status:
                current["status"] = str(status).strip()
            current["updated_at"] = _utc_now_iso()
            rows[existing_index] = current
            self._save_raw(rows)
        return AutomationStagedCase(**current)
