from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import catalog_index


def _empresa(*, nit: str, nombre: str, updated_at: str, remote_id: str = "") -> dict[str, str]:
    return {
        "id": remote_id or nit,
        "nit_empresa": nit,
        "nombre_empresa": nombre,
        "updated_at": updated_at,
    }


def _profesional(*, record_id: str, nombre: str, updated_at: str, correo: str = "", programa: str = "Inclusión Laboral") -> dict[str, str]:
    return {
        "id": record_id,
        "nombre_profesional": nombre,
        "correo_profesional": correo,
        "programa": programa,
        "updated_at": updated_at,
    }


def _interprete(*, nombre: str, updated_at: str) -> dict[str, str]:
    return {
        "nombre": nombre,
        "updated_at": updated_at,
    }


def _usuario(*, cedula: str, nombre: str, updated_at: str) -> dict[str, str]:
    return {
        "cedula_usuario": cedula,
        "nombre_usuario": nombre,
        "updated_at": updated_at,
    }


def _tarifa(
    *,
    codigo: str,
    referencia: str,
    descripcion: str,
    modalidad: str,
    valor_base: float,
    updated_at: str,
) -> dict[str, str | float]:
    return {
        "codigo_servicio": codigo,
        "referencia_servicio": referencia,
        "descripcion_servicio": descripcion,
        "modalidad_servicio": modalidad,
        "valor_base": valor_base,
        "updated_at": updated_at,
    }


class CatalogIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._appdata = Path(self._tmpdir.name)
        self._patcher = patch("app.catalog_index.app_data_dir", return_value=self._appdata)
        self._patcher.start()

    def tearDown(self) -> None:
        self._patcher.stop()
        self._tmpdir.cleanup()

    def test_first_sync_creates_local_indexes(self) -> None:
        with (
            patch(
                "app.catalog_index._fetch_empresas_rows",
                return_value=[_empresa(nit="9001", nombre="Empresa Demo", updated_at="2026-04-01T10:00:00+00:00")],
            ),
            patch(
                "app.catalog_index._fetch_profesionales_rows",
                return_value=[
                    _profesional(
                        record_id="1",
                        nombre="Ana Perez",
                        correo="ana@example.com",
                        programa="Inclusión Laboral",
                        updated_at="2026-04-01T10:05:00+00:00",
                    )
                ],
            ),
            patch(
                "app.catalog_index._fetch_interpretes_rows",
                return_value=[_interprete(nombre="Luis Gomez", updated_at="2026-04-01T10:10:00+00:00")],
            ),
            patch(
                "app.catalog_index._fetch_usuarios_rows",
                return_value=[_usuario(cedula="123", nombre="Carlos Ruiz", updated_at="2026-04-01T10:15:00+00:00")],
            ),
            patch(
                "app.catalog_index._fetch_tarifas_rows",
                return_value=[
                    _tarifa(
                        codigo="S-001",
                        referencia="REF-1",
                        descripcion="Servicio demo",
                        modalidad="Virtual",
                        valor_base=120000,
                        updated_at="2026-04-01T10:20:00+00:00",
                    )
                ],
            ),
        ):
            result = catalog_index.sync_local_catalog_indexes()

        self.assertEqual(result["catalogs"]["empresas"]["mode"], "full")
        self.assertTrue(catalog_index.catalog_indexes_ready())
        self.assertEqual(
            catalog_index.get_indexed_empresas(),
            [
                {
                    "remote_id": "9001",
                    "nit_empresa": "9001",
                    "nombre_empresa": "Empresa Demo",
                    "updated_at": "2026-04-01T10:00:00+00:00",
                }
            ],
        )
        self.assertEqual(
            catalog_index.get_indexed_profesionales(),
            [
                {
                    "source_kind": "profesional",
                    "remote_key": "1",
                    "nombre_profesional": "Ana Perez",
                    "correo_profesional": "ana@example.com",
                    "programa": "Inclusión Laboral",
                    "es_interprete": False,
                    "updated_at": "2026-04-01T10:05:00+00:00",
                },
                {
                    "source_kind": "interprete",
                    "remote_key": "luis gomez",
                    "nombre_profesional": "Luis Gomez",
                    "correo_profesional": "",
                    "programa": "Interprete",
                    "es_interprete": True,
                    "updated_at": "2026-04-01T10:10:00+00:00",
                },
            ],
        )
        self.assertEqual(
            catalog_index.get_indexed_usuarios(),
            [
                {
                    "cedula_usuario": "123",
                    "nombre_usuario": "Carlos Ruiz",
                    "updated_at": "2026-04-01T10:15:00+00:00",
                }
            ],
        )
        self.assertEqual(
            catalog_index.get_indexed_tarifas(),
            [
                {
                    "codigo_servicio": "S-001",
                    "referencia_servicio": "REF-1",
                    "descripcion_servicio": "Servicio demo",
                    "modalidad_servicio": "Virtual",
                    "valor_base": 120000,
                    "updated_at": "2026-04-01T10:20:00+00:00",
                }
            ],
        )

    def test_incremental_sync_upserts_only_changed_rows(self) -> None:
        with (
            patch(
                "app.catalog_index._fetch_empresas_rows",
                return_value=[_empresa(nit="9001", nombre="Empresa Demo", updated_at="2026-04-01T10:00:00+00:00")],
            ),
            patch(
                "app.catalog_index._fetch_profesionales_rows",
                return_value=[_profesional(record_id="1", nombre="Ana Perez", updated_at="2026-04-01T10:05:00+00:00")],
            ),
            patch("app.catalog_index._fetch_interpretes_rows", return_value=[]),
            patch(
                "app.catalog_index._fetch_usuarios_rows",
                return_value=[_usuario(cedula="123", nombre="Carlos Ruiz", updated_at="2026-04-01T10:15:00+00:00")],
            ),
            patch(
                "app.catalog_index._fetch_tarifas_rows",
                return_value=[
                    _tarifa(
                        codigo="S-001",
                        referencia="REF-1",
                        descripcion="Servicio demo",
                        modalidad="Virtual",
                        valor_base=120000,
                        updated_at="2026-04-01T10:20:00+00:00",
                    )
                ],
            ),
        ):
            catalog_index.sync_local_catalog_indexes()

        with (
            patch(
                "app.catalog_index._fetch_empresas_rows",
                return_value=[_empresa(nit="9001", nombre="Empresa Demo Renombrada", updated_at="2026-04-01T11:00:00+00:00")],
            ) as mock_empresas,
            patch(
                "app.catalog_index._fetch_profesionales_rows",
                return_value=[_profesional(record_id="1", nombre="Ana Perez", updated_at="2026-04-01T10:05:00+00:00")],
            ) as mock_profesionales,
            patch("app.catalog_index._fetch_interpretes_rows", return_value=[]) as mock_interpretes,
            patch("app.catalog_index._fetch_usuarios_rows", return_value=[]) as mock_usuarios,
            patch(
                "app.catalog_index._fetch_tarifas_rows",
                return_value=[
                    _tarifa(
                        codigo="S-001",
                        referencia="REF-1B",
                        descripcion="Servicio demo actualizado",
                        modalidad="Bogotá",
                        valor_base=125000,
                        updated_at="2026-04-01T11:20:00+00:00",
                    )
                ],
            ) as mock_tarifas,
        ):
            result = catalog_index.sync_local_catalog_indexes()

        self.assertEqual(result["catalogs"]["empresas"]["mode"], "incremental")
        self.assertEqual(result["catalogs"]["profesionales"]["mode"], "full")
        self.assertEqual(catalog_index.get_indexed_empresas()[0]["nombre_empresa"], "Empresa Demo Renombrada")
        self.assertEqual(catalog_index.get_indexed_profesionales()[0]["nombre_profesional"], "Ana Perez")
        self.assertEqual(catalog_index.get_indexed_usuarios()[0]["cedula_usuario"], "123")
        self.assertEqual(catalog_index.get_indexed_tarifas()[0]["descripcion_servicio"], "Servicio demo actualizado")
        mock_empresas.assert_called_once()
        self.assertIsNotNone(mock_empresas.call_args.kwargs.get("updated_after"))
        self.assertEqual(mock_profesionales.call_args.kwargs, {})
        self.assertEqual(mock_interpretes.call_args.kwargs, {})
        self.assertIsNotNone(mock_usuarios.call_args.kwargs.get("updated_after"))
        self.assertIsNotNone(mock_tarifas.call_args.kwargs.get("updated_after"))

    def test_corrupt_local_db_is_recreated(self) -> None:
        db_path = self._appdata / "catalog_indexes.sqlite3"
        db_path.write_text("not-a-sqlite-db", encoding="utf-8")

        with (
            patch(
                "app.catalog_index._fetch_empresas_rows",
                return_value=[_empresa(nit="9001", nombre="Empresa Demo", updated_at="2026-04-01T10:00:00+00:00")],
            ),
            patch("app.catalog_index._fetch_profesionales_rows", return_value=[]),
            patch("app.catalog_index._fetch_interpretes_rows", return_value=[]),
            patch("app.catalog_index._fetch_usuarios_rows", return_value=[]),
            patch("app.catalog_index._fetch_tarifas_rows", return_value=[]),
        ):
            catalog_index.sync_local_catalog_indexes(force_full=True)

        self.assertTrue(catalog_index.catalog_indexes_ready(catalogs=("empresas",)))
        self.assertEqual(catalog_index.get_indexed_empresas()[0]["nit_empresa"], "9001")


if __name__ == "__main__":
    unittest.main()
