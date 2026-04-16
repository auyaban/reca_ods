from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from app.logging_utils import LOGGER_BACKEND, get_logger
from app.paths import app_data_dir
from app.supabase_client import execute_with_reauth
from app.utils.cache import ttl_bucket
from app.utils.text import normalize_text

_LOGGER = get_logger(LOGGER_BACKEND)
_DB_FILENAME = "catalog_indexes.sqlite3"
_REMOTE_PAGE_SIZE = 1000
_DETAIL_CACHE_TTL_SECONDS = 300
_FULL_REBUILD_INTERVAL = timedelta(days=7)
_INCREMENTAL_MARGIN = timedelta(minutes=5)
_SUPPORTED_CATALOGS = ("empresas", "profesionales", "usuarios", "tarifas")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db_path() -> Path:
    return app_data_dir() / _DB_FILENAME


def _normalize_interpreter_key(value: Any) -> str:
    return normalize_text(value or "", lowercase=True)


def _normalize_catalogs(catalogs: Iterable[str] | None) -> tuple[str, ...]:
    selected = tuple(dict.fromkeys(str(item or "").strip().lower() for item in (catalogs or _SUPPORTED_CATALOGS)))
    if not selected:
        return _SUPPORTED_CATALOGS
    invalid = [item for item in selected if item not in _SUPPORTED_CATALOGS]
    if invalid:
        raise ValueError(f"Catalogos no soportados: {', '.join(invalid)}")
    return selected


def _parse_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat()


def _sync_cutoff(last_sync: str | None) -> str | None:
    parsed = _parse_datetime(last_sync)
    if parsed is None:
        return None
    return _format_datetime(parsed - _INCREMENTAL_MARGIN)


def _row_sync_value(row: dict[str, Any]) -> str | None:
    value = str(row.get("updated_at") or row.get("created_at") or "").strip()
    return value or None


def _ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            catalog TEXT PRIMARY KEY,
            last_incremental_sync_at TEXT,
            last_full_sync_at TEXT
        );

        CREATE TABLE IF NOT EXISTS empresas_index (
            remote_id TEXT,
            nit_empresa TEXT PRIMARY KEY,
            nombre_empresa TEXT NOT NULL,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS profesionales_index (
            source_kind TEXT NOT NULL,
            remote_key TEXT NOT NULL,
            display_name TEXT NOT NULL,
            correo_profesional TEXT,
            programa TEXT NOT NULL,
            es_interprete INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY (source_kind, remote_key)
        );

        CREATE TABLE IF NOT EXISTS usuarios_index (
            cedula_usuario TEXT PRIMARY KEY,
            nombre_usuario TEXT NOT NULL,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS tarifas_index (
            codigo_servicio TEXT PRIMARY KEY,
            referencia_servicio TEXT,
            descripcion_servicio TEXT NOT NULL,
            modalidad_servicio TEXT,
            valor_base REAL,
            updated_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_empresas_nombre_empresa
        ON empresas_index(nombre_empresa COLLATE NOCASE);

        CREATE INDEX IF NOT EXISTS idx_profesionales_display_name
        ON profesionales_index(display_name COLLATE NOCASE);

        CREATE INDEX IF NOT EXISTS idx_usuarios_nombre_usuario
        ON usuarios_index(nombre_usuario COLLATE NOCASE);

        CREATE INDEX IF NOT EXISTS idx_tarifas_descripcion_servicio
        ON tarifas_index(descripcion_servicio COLLATE NOCASE);
        """
    )


def _open_connection() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        _ensure_schema(connection)
    except sqlite3.DatabaseError:
        connection.close()
        raise
    return connection


@contextmanager
def _connection(reset_if_broken: bool = True):
    connection: sqlite3.Connection | None = None
    try:
        connection = _open_connection()
    except sqlite3.DatabaseError:
        if not reset_if_broken:
            raise
        path = _db_path()
        try:
            if path.exists():
                path.unlink()
        except OSError as exc:
            _LOGGER.warning("No se pudo recrear store local de catalogos en %s: %s", path, exc)
            raise
        connection = _open_connection()

    try:
        assert connection is not None
        yield connection
    finally:
        if connection is not None:
            connection.close()


def _get_sync_state(connection: sqlite3.Connection, catalog: str) -> dict[str, str]:
    row = connection.execute(
        "SELECT last_incremental_sync_at, last_full_sync_at FROM sync_state WHERE catalog = ?",
        (catalog,),
    ).fetchone()
    if row is None:
        return {"last_incremental_sync_at": "", "last_full_sync_at": ""}
    return {
        "last_incremental_sync_at": str(row["last_incremental_sync_at"] or ""),
        "last_full_sync_at": str(row["last_full_sync_at"] or ""),
    }


def _set_sync_state(
    connection: sqlite3.Connection,
    catalog: str,
    *,
    last_incremental_sync_at: str | None = None,
    last_full_sync_at: str | None = None,
) -> None:
    current = _get_sync_state(connection, catalog)
    incremental_value = current["last_incremental_sync_at"]
    full_value = current["last_full_sync_at"]
    if last_incremental_sync_at is not None:
        incremental_value = last_incremental_sync_at
    if last_full_sync_at is not None:
        full_value = last_full_sync_at
    connection.execute(
        """
        INSERT INTO sync_state(catalog, last_incremental_sync_at, last_full_sync_at)
        VALUES(?, ?, ?)
        ON CONFLICT(catalog) DO UPDATE SET
            last_incremental_sync_at = excluded.last_incremental_sync_at,
            last_full_sync_at = excluded.last_full_sync_at
        """,
        (catalog, incremental_value, full_value),
    )


def _catalog_table_name(catalog: str) -> str:
    if catalog == "empresas":
        return "empresas_index"
    if catalog == "profesionales":
        return "profesionales_index"
    if catalog == "usuarios":
        return "usuarios_index"
    if catalog == "tarifas":
        return "tarifas_index"
    raise ValueError(f"Catalogo no soportado: {catalog}")


def _catalog_row_count(connection: sqlite3.Connection, catalog: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {_catalog_table_name(catalog)}").fetchone()
    return int(row["count"] or 0) if row is not None else 0


def _has_synced_catalog(connection: sqlite3.Connection, catalog: str) -> bool:
    state = _get_sync_state(connection, catalog)
    return bool(state["last_full_sync_at"] or state["last_incremental_sync_at"]) and _catalog_row_count(connection, catalog) > 0


def _catalog_needs_full_rebuild(connection: sqlite3.Connection, catalog: str, *, force_full: bool) -> bool:
    if force_full:
        return True
    state = _get_sync_state(connection, catalog)
    last_full_sync = _parse_datetime(state["last_full_sync_at"])
    if last_full_sync is None:
        return True
    if _catalog_row_count(connection, catalog) <= 0:
        return True
    return _utc_now() - last_full_sync >= _FULL_REBUILD_INTERVAL


def _fetch_remote_pages(query_factory, *, context: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        response = execute_with_reauth(
            lambda client, offset=offset: query_factory(client, offset).execute(),
            context=f"{context}.offset_{offset}",
        )
        batch = [dict(item) for item in list(response.data or [])]
        rows.extend(batch)
        if len(batch) < _REMOTE_PAGE_SIZE:
            break
        offset += _REMOTE_PAGE_SIZE
    return rows


def _fetch_empresas_rows(*, updated_after: str | None = None) -> list[dict[str, Any]]:
    def _query(client, offset: int):
        query = (
            client.table("empresas")
            .select("id,nit_empresa,nombre_empresa,updated_at")
            .order("updated_at")
            .order("nit_empresa")
            .range(offset, offset + _REMOTE_PAGE_SIZE - 1)
        )
        if updated_after:
            query = query.gt("updated_at", updated_after)
        return query

    return _fetch_remote_pages(_query, context="catalog_index.empresas")


def _fetch_profesionales_rows(*, updated_after: str | None = None) -> list[dict[str, Any]]:
    def _query(client, offset: int):
        query = (
            client.table("profesionales")
            .select("id,nombre_profesional,correo_profesional,programa")
            .order("id")
            .range(offset, offset + _REMOTE_PAGE_SIZE - 1)
        )
        return query

    return _fetch_remote_pages(_query, context="catalog_index.profesionales")


def _fetch_interpretes_rows(*, updated_after: str | None = None) -> list[dict[str, Any]]:
    def _query(client, offset: int):
        query = (
            client.table("interpretes")
            .select("nombre,created_at")
            .order("created_at")
            .order("nombre")
            .range(offset, offset + _REMOTE_PAGE_SIZE - 1)
        )
        if updated_after:
            query = query.gt("created_at", updated_after)
        return query

    return _fetch_remote_pages(_query, context="catalog_index.interpretes")


def _fetch_usuarios_rows(*, updated_after: str | None = None) -> list[dict[str, Any]]:
    def _query(client, offset: int):
        query = (
            client.table("usuarios_reca")
            .select("cedula_usuario,nombre_usuario,created_at")
            .order("created_at")
            .order("cedula_usuario")
            .range(offset, offset + _REMOTE_PAGE_SIZE - 1)
        )
        if updated_after:
            query = query.gt("created_at", updated_after)
        return query

    return _fetch_remote_pages(_query, context="catalog_index.usuarios")


def _fetch_tarifas_rows(*, updated_after: str | None = None) -> list[dict[str, Any]]:
    def _query(client, offset: int):
        query = (
            client.table("tarifas")
            .select(
                "codigo_servicio,referencia_servicio,descripcion_servicio,modalidad_servicio,valor_base,updated_at"
            )
            .order("updated_at")
            .order("codigo_servicio")
            .range(offset, offset + _REMOTE_PAGE_SIZE - 1)
        )
        if updated_after:
            query = query.gt("updated_at", updated_after)
        return query

    return _fetch_remote_pages(_query, context="catalog_index.tarifas")


def _replace_empresas(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    payload_by_nit: dict[str, tuple[str | None, str, str, str | None]] = {}
    for row in rows:
        nit = str(row.get("nit_empresa") or "").strip()
        nombre = str(row.get("nombre_empresa") or "").strip()
        if not nit or not nombre:
            continue
        payload_by_nit[nit] = (
            str(row.get("id") or "") or None,
            nit,
            nombre,
            _row_sync_value(row),
        )
    connection.execute("DELETE FROM empresas_index")
    connection.executemany(
        """
        INSERT INTO empresas_index(remote_id, nit_empresa, nombre_empresa, updated_at)
        VALUES(?, ?, ?, ?)
        """,
        list(payload_by_nit.values()),
    )


def _upsert_empresas(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    payload_by_nit: dict[str, tuple[str | None, str, str, str | None]] = {}
    for row in rows:
        nit = str(row.get("nit_empresa") or "").strip()
        nombre = str(row.get("nombre_empresa") or "").strip()
        if not nit or not nombre:
            continue
        payload_by_nit[nit] = (
            str(row.get("id") or "") or None,
            nit,
            nombre,
            _row_sync_value(row),
        )
    connection.executemany(
        """
        INSERT INTO empresas_index(remote_id, nit_empresa, nombre_empresa, updated_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(nit_empresa) DO UPDATE SET
            remote_id = excluded.remote_id,
            nombre_empresa = excluded.nombre_empresa,
            updated_at = excluded.updated_at
        """,
        list(payload_by_nit.values()),
    )


def _professional_index_rows(rows: list[dict[str, Any]]) -> list[tuple[str, str, str, str | None, str, int, str | None]]:
    payload: list[tuple[str, str, str, str | None, str, int, str | None]] = []
    for row in rows:
        remote_id = str(row.get("id") or "").strip()
        display_name = str(row.get("nombre_profesional") or "").strip()
        if not remote_id or not display_name:
            continue
        payload.append(
            (
                "profesional",
                remote_id,
                display_name,
                str(row.get("correo_profesional") or "").strip() or None,
                str(row.get("programa") or "").strip() or "Inclusión Laboral",
                0,
                _row_sync_value(row),
            )
        )
    return payload


def _interpreter_index_rows(rows: list[dict[str, Any]]) -> list[tuple[str, str, str, str | None, str, int, str | None]]:
    payload: list[tuple[str, str, str, str | None, str, int, str | None]] = []
    for row in rows:
        display_name = str(row.get("nombre") or "").strip()
        remote_key = _normalize_interpreter_key(display_name)
        if not remote_key or not display_name:
            continue
        payload.append(
            (
                "interprete",
                remote_key,
                display_name,
                None,
                "Interprete",
                1,
                _row_sync_value(row),
            )
        )
    return payload


def _replace_profesionales(
    connection: sqlite3.Connection,
    profesionales_rows: list[dict[str, Any]],
    interpretes_rows: list[dict[str, Any]],
) -> None:
    connection.execute("DELETE FROM profesionales_index")
    connection.executemany(
        """
        INSERT INTO profesionales_index(
            source_kind, remote_key, display_name, correo_profesional, programa, es_interprete, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        [*_professional_index_rows(profesionales_rows), *_interpreter_index_rows(interpretes_rows)],
    )


def _upsert_profesionales(
    connection: sqlite3.Connection,
    profesionales_rows: list[dict[str, Any]],
    interpretes_rows: list[dict[str, Any]],
) -> None:
    connection.executemany(
        """
        INSERT INTO profesionales_index(
            source_kind, remote_key, display_name, correo_profesional, programa, es_interprete, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_kind, remote_key) DO UPDATE SET
            display_name = excluded.display_name,
            correo_profesional = excluded.correo_profesional,
            programa = excluded.programa,
            es_interprete = excluded.es_interprete,
            updated_at = excluded.updated_at
        """,
        [*_professional_index_rows(profesionales_rows), *_interpreter_index_rows(interpretes_rows)],
    )


def _replace_usuarios(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    payload_by_cedula: dict[str, tuple[str, str, str | None]] = {}
    for row in rows:
        cedula = str(row.get("cedula_usuario") or "").strip()
        nombre = str(row.get("nombre_usuario") or "").strip()
        if not cedula or not nombre:
            continue
        payload_by_cedula[cedula] = (
            cedula,
            nombre,
            _row_sync_value(row),
        )
    connection.execute("DELETE FROM usuarios_index")
    connection.executemany(
        """
        INSERT INTO usuarios_index(cedula_usuario, nombre_usuario, updated_at)
        VALUES(?, ?, ?)
        """,
        list(payload_by_cedula.values()),
    )


def _upsert_usuarios(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    payload_by_cedula: dict[str, tuple[str, str, str | None]] = {}
    for row in rows:
        cedula = str(row.get("cedula_usuario") or "").strip()
        nombre = str(row.get("nombre_usuario") or "").strip()
        if not cedula or not nombre:
            continue
        payload_by_cedula[cedula] = (
            cedula,
            nombre,
            _row_sync_value(row),
        )
    connection.executemany(
        """
        INSERT INTO usuarios_index(cedula_usuario, nombre_usuario, updated_at)
        VALUES(?, ?, ?)
        ON CONFLICT(cedula_usuario) DO UPDATE SET
            nombre_usuario = excluded.nombre_usuario,
            updated_at = excluded.updated_at
        """,
        list(payload_by_cedula.values()),
    )


def _replace_tarifas(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    payload_by_codigo: dict[str, tuple[str, str | None, str, str | None, Any, str | None]] = {}
    for row in rows:
        codigo = str(row.get("codigo_servicio") or "").strip()
        descripcion = str(row.get("descripcion_servicio") or "").strip()
        if not codigo or not descripcion:
            continue
        payload_by_codigo[codigo] = (
            codigo,
            str(row.get("referencia_servicio") or "").strip() or None,
            descripcion,
            str(row.get("modalidad_servicio") or "").strip() or None,
            row.get("valor_base"),
            _row_sync_value(row),
        )
    connection.execute("DELETE FROM tarifas_index")
    connection.executemany(
        """
        INSERT INTO tarifas_index(
            codigo_servicio, referencia_servicio, descripcion_servicio, modalidad_servicio, valor_base, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?)
        """,
        list(payload_by_codigo.values()),
    )


def _upsert_tarifas(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    payload_by_codigo: dict[str, tuple[str, str | None, str, str | None, Any, str | None]] = {}
    for row in rows:
        codigo = str(row.get("codigo_servicio") or "").strip()
        descripcion = str(row.get("descripcion_servicio") or "").strip()
        if not codigo or not descripcion:
            continue
        payload_by_codigo[codigo] = (
            codigo,
            str(row.get("referencia_servicio") or "").strip() or None,
            descripcion,
            str(row.get("modalidad_servicio") or "").strip() or None,
            row.get("valor_base"),
            _row_sync_value(row),
        )
    connection.executemany(
        """
        INSERT INTO tarifas_index(
            codigo_servicio, referencia_servicio, descripcion_servicio, modalidad_servicio, valor_base, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(codigo_servicio) DO UPDATE SET
            referencia_servicio = excluded.referencia_servicio,
            descripcion_servicio = excluded.descripcion_servicio,
            modalidad_servicio = excluded.modalidad_servicio,
            valor_base = excluded.valor_base,
            updated_at = excluded.updated_at
        """,
        list(payload_by_codigo.values()),
    )


def _sync_empresas(connection: sqlite3.Connection, *, force_full: bool) -> dict[str, Any]:
    full_rebuild = _catalog_needs_full_rebuild(connection, "empresas", force_full=force_full)
    synced_at = _format_datetime(_utc_now())
    if full_rebuild:
        rows = _fetch_empresas_rows()
        with connection:
            _replace_empresas(connection, rows)
            _set_sync_state(
                connection,
                "empresas",
                last_incremental_sync_at=synced_at,
                last_full_sync_at=synced_at,
            )
        return {"catalog": "empresas", "mode": "full", "rows": len(rows)}

    state = _get_sync_state(connection, "empresas")
    rows = _fetch_empresas_rows(updated_after=_sync_cutoff(state["last_incremental_sync_at"]))
    with connection:
        _upsert_empresas(connection, rows)
        _set_sync_state(connection, "empresas", last_incremental_sync_at=synced_at)
    return {"catalog": "empresas", "mode": "incremental", "rows": len(rows)}


def _sync_profesionales(connection: sqlite3.Connection, *, force_full: bool) -> dict[str, Any]:
    synced_at = _format_datetime(_utc_now())
    profesionales_rows = _fetch_profesionales_rows()
    interpretes_rows = _fetch_interpretes_rows()
    with connection:
        _replace_profesionales(connection, profesionales_rows, interpretes_rows)
        _set_sync_state(
            connection,
            "profesionales",
            last_incremental_sync_at=synced_at,
            last_full_sync_at=synced_at,
        )
    return {
        "catalog": "profesionales",
        "mode": "full",
        "rows": len(profesionales_rows) + len(interpretes_rows),
    }


def _sync_usuarios(connection: sqlite3.Connection, *, force_full: bool) -> dict[str, Any]:
    full_rebuild = _catalog_needs_full_rebuild(connection, "usuarios", force_full=force_full)
    synced_at = _format_datetime(_utc_now())
    if full_rebuild:
        rows = _fetch_usuarios_rows()
        with connection:
            _replace_usuarios(connection, rows)
            _set_sync_state(
                connection,
                "usuarios",
                last_incremental_sync_at=synced_at,
                last_full_sync_at=synced_at,
            )
        return {"catalog": "usuarios", "mode": "full", "rows": len(rows)}

    state = _get_sync_state(connection, "usuarios")
    rows = _fetch_usuarios_rows(updated_after=_sync_cutoff(state["last_incremental_sync_at"]))
    with connection:
        _upsert_usuarios(connection, rows)
        _set_sync_state(connection, "usuarios", last_incremental_sync_at=synced_at)
    return {"catalog": "usuarios", "mode": "incremental", "rows": len(rows)}


def _sync_tarifas(connection: sqlite3.Connection, *, force_full: bool) -> dict[str, Any]:
    full_rebuild = _catalog_needs_full_rebuild(connection, "tarifas", force_full=force_full)
    synced_at = _format_datetime(_utc_now())
    if full_rebuild:
        rows = _fetch_tarifas_rows()
        with connection:
            _replace_tarifas(connection, rows)
            _set_sync_state(
                connection,
                "tarifas",
                last_incremental_sync_at=synced_at,
                last_full_sync_at=synced_at,
            )
        return {"catalog": "tarifas", "mode": "full", "rows": len(rows)}

    state = _get_sync_state(connection, "tarifas")
    rows = _fetch_tarifas_rows(updated_after=_sync_cutoff(state["last_incremental_sync_at"]))
    with connection:
        _upsert_tarifas(connection, rows)
        _set_sync_state(connection, "tarifas", last_incremental_sync_at=synced_at)
    return {"catalog": "tarifas", "mode": "incremental", "rows": len(rows)}


def sync_local_catalog_indexes(
    *,
    force_full: bool = False,
    catalogs: Iterable[str] | None = None,
    status_callback=None,
    allow_stale: bool = False,
) -> dict[str, Any]:
    selected = _normalize_catalogs(catalogs)
    results: dict[str, Any] = {"catalogs": {}}
    syncers = {
        "empresas": _sync_empresas,
        "profesionales": _sync_profesionales,
        "usuarios": _sync_usuarios,
        "tarifas": _sync_tarifas,
    }

    with _connection() as connection:
        total = len(selected)
        for index, catalog in enumerate(selected, start=1):
            if status_callback:
                progress = min(55, int(((index - 1) / max(total, 1)) * 55))
                status_callback(f"Sincronizando catalogo local de {catalog}...", progress)
            try:
                result = syncers[catalog](connection, force_full=force_full)
            except Exception as exc:
                if allow_stale and _has_synced_catalog(connection, catalog):
                    _LOGGER.warning(
                        "No se pudo sincronizar catalogo %s; se usara el indice local existente: %s",
                        catalog,
                        exc,
                    )
                    result = {"catalog": catalog, "mode": "stale", "rows": 0, "error": str(exc)}
                else:
                    raise
            results["catalogs"][catalog] = result
    if status_callback:
        status_callback("Catalogos locales sincronizados.", 60)
    clear_runtime_caches()
    return results


def catalog_indexes_ready(*, catalogs: Iterable[str] | None = None) -> bool:
    selected = _normalize_catalogs(catalogs)
    with _connection() as connection:
        return all(_has_synced_catalog(connection, catalog) for catalog in selected)


def get_indexed_empresas() -> list[dict[str, str]]:
    with _connection() as connection:
        rows = connection.execute(
            """
            SELECT remote_id, nit_empresa, nombre_empresa, updated_at
            FROM empresas_index
            ORDER BY nombre_empresa COLLATE NOCASE, nit_empresa COLLATE NOCASE
            """
        ).fetchall()
    return [
        {
            "remote_id": str(row["remote_id"] or "").strip(),
            "nit_empresa": str(row["nit_empresa"] or "").strip(),
            "nombre_empresa": str(row["nombre_empresa"] or "").strip(),
            "updated_at": str(row["updated_at"] or "").strip(),
        }
        for row in rows
    ]


def get_indexed_profesionales() -> list[dict[str, Any]]:
    with _connection() as connection:
        rows = connection.execute(
            """
            SELECT source_kind, remote_key, display_name, correo_profesional, programa, es_interprete, updated_at
            FROM profesionales_index
            ORDER BY display_name COLLATE NOCASE
            """
        ).fetchall()
    return [
        {
            "source_kind": str(row["source_kind"] or "").strip(),
            "remote_key": str(row["remote_key"] or "").strip(),
            "nombre_profesional": str(row["display_name"] or "").strip(),
            "correo_profesional": str(row["correo_profesional"] or "").strip(),
            "programa": str(row["programa"] or "").strip(),
            "es_interprete": bool(int(row["es_interprete"] or 0)),
            "updated_at": str(row["updated_at"] or "").strip(),
        }
        for row in rows
    ]


def get_indexed_usuarios() -> list[dict[str, str]]:
    with _connection() as connection:
        rows = connection.execute(
            """
            SELECT cedula_usuario, nombre_usuario, updated_at
            FROM usuarios_index
            ORDER BY cedula_usuario COLLATE NOCASE
            """
        ).fetchall()
    return [
        {
            "cedula_usuario": str(row["cedula_usuario"] or "").strip(),
            "nombre_usuario": str(row["nombre_usuario"] or "").strip(),
            "updated_at": str(row["updated_at"] or "").strip(),
        }
        for row in rows
    ]


def get_indexed_tarifas() -> list[dict[str, Any]]:
    with _connection() as connection:
        rows = connection.execute(
            """
            SELECT codigo_servicio, referencia_servicio, descripcion_servicio, modalidad_servicio, valor_base, updated_at
            FROM tarifas_index
            ORDER BY codigo_servicio COLLATE NOCASE
            """
        ).fetchall()
    return [
        {
            "codigo_servicio": str(row["codigo_servicio"] or "").strip(),
            "referencia_servicio": str(row["referencia_servicio"] or "").strip(),
            "descripcion_servicio": str(row["descripcion_servicio"] or "").strip(),
            "modalidad_servicio": str(row["modalidad_servicio"] or "").strip(),
            "valor_base": row["valor_base"],
            "updated_at": str(row["updated_at"] or "").strip(),
        }
        for row in rows
    ]


def get_tarifa_by_codigo(codigo: str) -> dict[str, Any] | None:
    codigo_clean = str(codigo or "").strip()
    if not codigo_clean:
        return None
    with _connection() as connection:
        row = connection.execute(
            """
            SELECT codigo_servicio, referencia_servicio, descripcion_servicio, modalidad_servicio, valor_base, updated_at
            FROM tarifas_index
            WHERE codigo_servicio = ?
            LIMIT 1
            """,
            (codigo_clean,),
        ).fetchone()
    if row is None:
        return None
    return {
        "codigo_servicio": str(row["codigo_servicio"] or "").strip(),
        "referencia_servicio": str(row["referencia_servicio"] or "").strip(),
        "descripcion_servicio": str(row["descripcion_servicio"] or "").strip(),
        "modalidad_servicio": str(row["modalidad_servicio"] or "").strip(),
        "valor_base": row["valor_base"],
        "updated_at": str(row["updated_at"] or "").strip(),
    }


@lru_cache
def _get_company_detail_cached(nit: str, _ttl_bucket: int) -> dict[str, Any] | None:
    nit_clean = str(nit or "").strip()
    if not nit_clean:
        return None
    response = execute_with_reauth(
        lambda client: (
            client.table("empresas")
            .select("nit_empresa,nombre_empresa,caja_compensacion,asesor,zona_empresa,sede_empresa,ciudad_empresa")
            .eq("nit_empresa", nit_clean)
            .limit(1)
            .execute()
        ),
        context="catalog_index.company_detail",
    )
    rows = list(response.data or [])
    if not rows:
        return None
    return dict(rows[0])


def get_company_detail_by_nit(nit: str) -> dict[str, Any] | None:
    return _get_company_detail_cached(str(nit or "").strip(), ttl_bucket(_DETAIL_CACHE_TTL_SECONDS))


@lru_cache
def _get_user_detail_cached(cedula: str, _ttl_bucket: int) -> dict[str, Any] | None:
    cedula_clean = str(cedula or "").strip()
    if not cedula_clean:
        return None
    response = execute_with_reauth(
        lambda client: (
            client.table("usuarios_reca")
            .select(
                "nombre_usuario,cedula_usuario,discapacidad_usuario,genero_usuario,"
                "tipo_contrato,fecha_firma_contrato,cargo_oferente"
            )
            .eq("cedula_usuario", cedula_clean)
            .limit(1)
            .execute()
        ),
        context="catalog_index.user_detail",
    )
    rows = list(response.data or [])
    if not rows:
        return None
    return dict(rows[0])


def get_user_detail_by_cedula(cedula: str) -> dict[str, Any] | None:
    return _get_user_detail_cached(str(cedula or "").strip(), ttl_bucket(_DETAIL_CACHE_TTL_SECONDS))


def get_user_details_by_cedulas(cedulas: Iterable[str]) -> dict[str, dict[str, Any]]:
    cedulas_clean = sorted({str(item or "").strip() for item in cedulas if str(item or "").strip()})
    if not cedulas_clean:
        return {}
    if len(cedulas_clean) == 1:
        detail = get_user_detail_by_cedula(cedulas_clean[0])
        if not detail:
            return {}
        return {cedulas_clean[0]: detail}

    response = execute_with_reauth(
        lambda client: (
            client.table("usuarios_reca")
            .select(
                "nombre_usuario,cedula_usuario,discapacidad_usuario,genero_usuario,"
                "tipo_contrato,fecha_firma_contrato,cargo_oferente"
            )
            .in_("cedula_usuario", cedulas_clean)
            .execute()
        ),
        context="catalog_index.user_detail_batch",
    )
    rows = [dict(item) for item in list(response.data or [])]
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        cedula = str(row.get("cedula_usuario") or "").strip()
        if cedula:
            result[cedula] = row
    return result


def clear_runtime_caches() -> None:
    _get_company_detail_cached.cache_clear()
    _get_user_detail_cached.cache_clear()
