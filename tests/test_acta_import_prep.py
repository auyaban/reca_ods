from __future__ import annotations

import unittest
from types import SimpleNamespace

from main_gui import WizardApp


class _ApiStub:
    def __init__(self) -> None:
        self._empresas = [
            {
                "nit_empresa": "900499362-8",
                "nombre_empresa": "Falabella.com",
            }
        ]
        self._usuarios = []

    def get_cached(self, path: str, params: dict | None = None) -> dict:
        if path == "/wizard/seccion-2/empresas":
            return {"data": self._empresas}
        if path == "/wizard/seccion-4/usuarios":
            return {"data": self._usuarios}
        raise AssertionError(f"Unexpected get_cached path: {path}")

    def get(self, path: str, params: dict | None = None) -> dict:
        if path == "/wizard/seccion-2/empresa":
            nit = str((params or {}).get("nit") or "").strip()
            return {"data": [item for item in self._empresas if item["nit_empresa"] == nit]}
        raise AssertionError(f"Unexpected get path: {path}")


class ActaImportPreparationTests(unittest.TestCase):
    def _build_app(self) -> WizardApp:
        app = WizardApp.__new__(WizardApp)
        app.api = _ApiStub()
        app.state = SimpleNamespace(usuarios_nuevos=[])
        app.seccion1 = SimpleNamespace(
            prof_combo=SimpleNamespace(
                _all_values=[
                    "Gabriela Rubiano Isaza",
                    "Lina Maria Guevara Bautista",
                    "Carlos Perez",
                ]
            )
        )
        app.seccion4 = SimpleNamespace(
            discapacidades=["Intelectual", "Fisica", "Visual", "Auditiva", "Psicosocial", "N/A"],
            generos=["Hombre", "Mujer", "Otro"],
        )
        return app

    def test_preparar_importacion_creates_missing_users_with_minimum_values(self) -> None:
        app = self._build_app()
        parsed = {
            "nit_empresa": "900499362-8",
            "nombre_empresa": "FALABELLA.COM",
            "participantes": [
                {
                    "nombre_usuario": "Leydi Marcela Avila Ardila",
                    "cedula_usuario": "1072922214",
                    "discapacidad_usuario": "Discapacidad fisica",
                    "genero_usuario": "",
                }
            ],
            "warnings": [],
        }

        prepared, participantes = app._preparar_importacion_acta(parsed)

        self.assertTrue(prepared["_nit_validado_bd"])
        self.assertEqual(prepared["_empresa_bd_nombre"], "Falabella.com")
        self.assertEqual(len(participantes), 1)
        self.assertEqual(participantes[0]["_usuario_accion"], "crear")
        self.assertEqual(participantes[0]["nombre_usuario"], "Leydi Marcela Avila Ardila")
        self.assertEqual(participantes[0]["cedula_usuario"], "1072922214")
        self.assertEqual(participantes[0]["discapacidad_usuario"], "Fisica")
        self.assertEqual(participantes[0]["genero_usuario"], "Otro")
        self.assertTrue(any("usuarios nuevos" in item.lower() for item in prepared["warnings"]))

    def test_preparar_importacion_fails_when_company_name_does_not_match(self) -> None:
        app = self._build_app()
        parsed = {
            "nit_empresa": "900499362-8",
            "nombre_empresa": "Empresa Incorrecta",
            "participantes": [],
            "warnings": [],
        }

        with self.assertRaisesRegex(RuntimeError, "no coincide con la empresa registrada"):
            app._preparar_importacion_acta(parsed)

    def test_preparar_importacion_warns_when_no_valid_ids_are_found(self) -> None:
        app = self._build_app()
        parsed = {
            "nit_empresa": "900499362-8",
            "nombre_empresa": "FALABELLA.COM",
            "participantes": [],
            "warnings": [],
        }

        prepared, participantes = app._preparar_importacion_acta(parsed)

        self.assertEqual(participantes, [])
        self.assertTrue(any("deben completarse manualmente" in item.lower() for item in prepared["warnings"]))

    def test_resolve_profesional_import_uses_closest_candidate_from_pdf(self) -> None:
        app = self._build_app()

        resolved = app._resolve_profesional_import(
            "Lina Maria Guevara Bautista",
            [
                "Gabriela Rubiano Isaza",
                "Lissette Lorena Castaneda",
            ],
        )

        self.assertEqual(resolved, "Gabriela Rubiano Isaza")


if __name__ == "__main__":
    unittest.main()
