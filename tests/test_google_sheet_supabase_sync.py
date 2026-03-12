from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.google_sheet_supabase_sync import (
    ODS_CALCULADA_HEADERS,
    apply_google_sheet_supabase_sync,
    preview_google_sheet_supabase_sync,
)


def _sheet_row(
    *,
    record_id: str = "ods-1",
    valor_total: str = "120",
    valor_virtual: str = "120",
    horas_interprete: str = "2",
) -> list[str]:
    return [
        record_id,
        "Ana Perez",
        "COD-001",
        "Empresa Demo",
        "900123456",
        "Compensar",
        "2026-03-12",
        "REF-1",
        "Servicio Demo",
        "Carlos Ruiz",
        "123456",
        "Auditiva",
        "2026-03-01",
        valor_virtual,
        "0",
        "0",
        "0",
        horas_interprete,
        "0",
        valor_total,
        "Observacion",
        "Asesor",
        "Principal",
        "Virtual",
        "Ninguna",
        "SI",
        "3",
        "Masculino",
        "Laboral",
        "Pendiente",
        "Analista",
        "1",
        "2026",
    ]


def _supabase_row(
    *,
    record_id: str = "ods-1",
    valor_total: float = 100.0,
    valor_virtual: float = 100.0,
    horas_interprete: float = 1.0,
) -> dict:
    return {
        "id": record_id,
        "codigo_servicio": "COD-001",
        "referencia_servicio": "REF-1",
        "descripcion_servicio": "Servicio Demo",
        "nombre_profesional": "Ana Perez",
        "nombre_empresa": "Empresa Demo",
        "nit_empresa": "900123456",
        "caja_compensacion": "Compensar",
        "asesor_empresa": "Asesor",
        "sede_empresa": "Principal",
        "fecha_servicio": "2026-03-12",
        "fecha_ingreso": "2026-03-01",
        "mes_servicio": 3,
        "ano_servicio": 2026,
        "nombre_usuario": "Carlos Ruiz",
        "cedula_usuario": "123456",
        "discapacidad_usuario": "Auditiva",
        "genero_usuario": "Masculino",
        "modalidad_servicio": "Virtual",
        "todas_modalidades": 0.0,
        "horas_interprete": horas_interprete,
        "valor_virtual": valor_virtual,
        "valor_bogota": 0.0,
        "valor_otro": 0.0,
        "valor_interprete": 0.0,
        "valor_total": valor_total,
        "tipo_contrato": "Laboral",
        "cargo_servicio": "Analista",
        "seguimiento_servicio": "Pendiente",
        "orden_clausulada": True,
        "total_personas": 1,
        "observaciones": "Observacion",
        "observacion_agencia": "Ninguna",
    }


class GoogleSheetSupabaseSyncTests(unittest.TestCase):
    @patch("app.google_sheet_supabase_sync.execute_with_reauth")
    @patch("app.google_sheet_supabase_sync.read_sheet_values")
    @patch("app.google_sheet_supabase_sync.get_existing_monthly_spreadsheet")
    def test_preview_reports_changed_records(
        self,
        mock_get_monthly,
        mock_read_sheet_values,
        mock_execute_with_reauth,
    ) -> None:
        mock_get_monthly.return_value = {"id": "sheet-1", "name": "ODS_MAR_2026"}
        mock_read_sheet_values.return_value = [ODS_CALCULADA_HEADERS, _sheet_row()]
        mock_execute_with_reauth.return_value = SimpleNamespace(data=[_supabase_row()])

        report = preview_google_sheet_supabase_sync(3, 2026)

        self.assertEqual(report["spreadsheet_name"], "ODS_MAR_2026")
        self.assertEqual(report["changed_record_count"], 1)
        self.assertEqual(report["only_in_sheet_count"], 0)
        self.assertEqual(report["only_in_supabase_count"], 0)
        changed = report["changed_records"][0]
        self.assertEqual(changed["id"], "ods-1")
        self.assertEqual(changed["update_payload"], {"horas_interprete": 2.0, "valor_total": 120.0, "valor_virtual": 120.0})

    @patch("app.google_sheet_supabase_sync.execute_with_reauth")
    @patch("app.google_sheet_supabase_sync.read_sheet_values")
    @patch("app.google_sheet_supabase_sync.get_existing_monthly_spreadsheet")
    def test_preview_reports_only_in_sheet_and_supabase(
        self,
        mock_get_monthly,
        mock_read_sheet_values,
        mock_execute_with_reauth,
    ) -> None:
        mock_get_monthly.return_value = {"id": "sheet-1", "name": "ODS_MAR_2026"}
        mock_read_sheet_values.return_value = [
            ODS_CALCULADA_HEADERS,
            _sheet_row(record_id="ods-1"),
            _sheet_row(record_id="ods-2"),
        ]
        mock_execute_with_reauth.return_value = SimpleNamespace(data=[_supabase_row(record_id="ods-1"), _supabase_row(record_id="ods-3")])

        report = preview_google_sheet_supabase_sync(3, 2026)

        self.assertEqual(report["only_in_sheet_count"], 1)
        self.assertEqual(report["only_in_sheet"][0]["id"], "ods-2")
        self.assertEqual(report["only_in_supabase_count"], 1)
        self.assertEqual(report["only_in_supabase"][0]["id"], "ods-3")

    @patch("app.google_sheet_supabase_sync.execute_with_reauth")
    @patch("app.google_sheet_supabase_sync.read_sheet_values")
    @patch("app.google_sheet_supabase_sync.get_existing_monthly_spreadsheet")
    def test_preview_reports_rows_without_id_and_duplicates(
        self,
        mock_get_monthly,
        mock_read_sheet_values,
        mock_execute_with_reauth,
    ) -> None:
        mock_get_monthly.return_value = {"id": "sheet-1", "name": "ODS_MAR_2026"}
        no_id_row = _sheet_row(record_id="")
        dup1 = _sheet_row(record_id="ods-dup")
        dup2 = _sheet_row(record_id="ods-dup")
        mock_read_sheet_values.return_value = [ODS_CALCULADA_HEADERS, no_id_row, dup1, dup2]
        mock_execute_with_reauth.return_value = SimpleNamespace(data=[])

        report = preview_google_sheet_supabase_sync(3, 2026)

        self.assertEqual(len(report["ignored_rows_without_id"]), 1)
        self.assertEqual(len(report["invalid_rows"]), 2)
        self.assertTrue(all(item["reason"] == "duplicate_id" for item in report["invalid_rows"]))

    @patch("app.google_sheet_supabase_sync.get_existing_monthly_spreadsheet")
    def test_preview_raises_when_spreadsheet_not_found(self, mock_get_monthly) -> None:
        mock_get_monthly.side_effect = RuntimeError("No existe el spreadsheet mensual 'ODS_MAR_2026' en el Shared Drive.")
        with self.assertRaises(RuntimeError):
            preview_google_sheet_supabase_sync(3, 2026)

    @patch("app.google_sheet_supabase_sync._apply_update")
    @patch("app.google_sheet_supabase_sync.preview_google_sheet_supabase_sync")
    def test_apply_updates_only_selected_ids(self, mock_preview, mock_apply_update) -> None:
        mock_preview.return_value = {
            "spreadsheet_id": "sheet-1",
            "spreadsheet_name": "ODS_MAR_2026",
            "sheet_name": "ODS_CALCULADA",
            "changed_record_count": 2,
            "changed_records": [
                {"id": "ods-1", "sheet_row": 2, "update_payload": {"valor_total": 120.0}},
                {"id": "ods-2", "sheet_row": 3, "update_payload": {"valor_total": 130.0, "valor_virtual": 130.0}},
            ],
            "only_in_sheet_count": 0,
            "only_in_supabase_count": 0,
            "invalid_rows": [],
            "ignored_rows_without_id": [],
        }

        result = apply_google_sheet_supabase_sync(3, 2026, selected_ids=["ods-2"])

        self.assertEqual(result["applied_record_count"], 1)
        self.assertEqual(result["applied_field_count"], 2)
        mock_apply_update.assert_called_once_with("ods-2", {"valor_total": 130.0, "valor_virtual": 130.0})

    @patch("app.google_sheet_supabase_sync._apply_update")
    @patch("app.google_sheet_supabase_sync.preview_google_sheet_supabase_sync")
    def test_apply_collects_partial_failures(self, mock_preview, mock_apply_update) -> None:
        mock_preview.return_value = {
            "spreadsheet_id": "sheet-1",
            "spreadsheet_name": "ODS_MAR_2026",
            "sheet_name": "ODS_CALCULADA",
            "changed_record_count": 2,
            "changed_records": [
                {"id": "ods-1", "sheet_row": 2, "update_payload": {"valor_total": 120.0}},
                {"id": "ods-2", "sheet_row": 3, "update_payload": {"valor_total": 130.0}},
            ],
            "only_in_sheet_count": 0,
            "only_in_supabase_count": 0,
            "invalid_rows": [],
            "ignored_rows_without_id": [],
        }
        mock_apply_update.side_effect = [None, RuntimeError("write failed")]

        result = apply_google_sheet_supabase_sync(3, 2026)

        self.assertEqual(result["applied_record_count"], 1)
        self.assertEqual(result["failed_record_count"], 1)
        self.assertEqual(result["failed_records"][0]["id"], "ods-2")
        self.assertIn("write failed", result["failed_records"][0]["error"])

    @patch("app.google_sheet_supabase_sync.execute_with_reauth")
    @patch("app.google_sheet_supabase_sync.read_sheet_values")
    @patch("app.google_sheet_supabase_sync.get_existing_monthly_spreadsheet")
    def test_preview_fails_on_invalid_headers(
        self,
        mock_get_monthly,
        mock_read_sheet_values,
        mock_execute_with_reauth,
    ) -> None:
        mock_get_monthly.return_value = {"id": "sheet-1", "name": "ODS_MAR_2026"}
        mock_read_sheet_values.return_value = [["BAD", "HEADERS"]]
        mock_execute_with_reauth.return_value = SimpleNamespace(data=[])

        with self.assertRaises(RuntimeError):
            preview_google_sheet_supabase_sync(3, 2026)


if __name__ == "__main__":
    unittest.main()
