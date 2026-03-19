from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import threading

from app.paths import app_data_dir


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProcessedEmailLog:
    """Persistent log of Gmail message IDs that have already been processed
    by the batch end-of-day automation.  Stored as a JSON list in AppData."""

    _global_lock: threading.Lock = threading.Lock()

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or (app_data_dir() / "automation_processed_emails.json")

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
            raise RuntimeError(
                f"El log de correos procesados esta corrupto y no puede leerse: {exc}"
            ) from exc
        if not isinstance(payload, list):
            raise RuntimeError("El log de correos procesados tiene un formato invalido.")
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

    def is_processed(self, message_id: str) -> bool:
        message_id_clean = str(message_id or "").strip()
        if not message_id_clean:
            return False
        with ProcessedEmailLog._global_lock:
            rows = self._load_raw()
        return any(str(row.get("message_id") or "") == message_id_clean for row in rows)

    def get_processed_ids(self) -> set[str]:
        """Load all processed message IDs at once for efficient batch lookups."""
        with ProcessedEmailLog._global_lock:
            rows = self._load_raw()
        return {
            str(row.get("message_id") or "").strip()
            for row in rows
            if str(row.get("message_id") or "").strip()
        }

    def mark_processed(self, message_id: str, subject: str, processed_at: str | None = None) -> None:
        message_id_clean = str(message_id or "").strip()
        if not message_id_clean:
            return
        now = processed_at or _utc_now_iso()
        with ProcessedEmailLog._global_lock:
            rows = self._load_raw()
            if any(str(row.get("message_id") or "") == message_id_clean for row in rows):
                return
            rows.append({
                "message_id": message_id_clean,
                "subject": str(subject or "").strip(),
                "processed_at": now,
            })
            self._save_raw(rows)

    def list_processed(self) -> list[dict]:
        with ProcessedEmailLog._global_lock:
            rows = self._load_raw()
        rows.sort(key=lambda item: str(item.get("processed_at") or ""), reverse=True)
        return rows
