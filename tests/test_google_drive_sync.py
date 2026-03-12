from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.google_drive_sync import (
    GoogleDriveSyncWarningError,
    resolve_monthly_spreadsheet_name,
    sync_new_ods_record,
)
from app.google_sheet_layouts import ODS_INPUT_HEADERS


def _sample_ods() -> dict:
    return {
        "id": "ods-1",
        "nombre_profesional": "Ana Perez",
        "codigo_servicio": "COD-001",
        "nombre_empresa": "Empresa Demo",
        "nit_empresa": "900123456",
        "caja_compensacion": "Compensar",
        "fecha_servicio": "2026-03-12",
        "nombre_usuario": "Carlos Ruiz",
        "cedula_usuario": "123456",
        "discapacidad_usuario": "Auditiva",
        "fecha_ingreso": "2026-03-01",
        "observaciones": "Sin novedad",
        "modalidad_servicio": "Virtual",
        "orden_clausulada": True,
        "genero_usuario": "Masculino",
        "tipo_contrato": "Laboral",
        "asesor_empresa": "Asesor Demo",
        "sede_empresa": "Principal",
        "observacion_agencia": "Ninguna",
        "seguimiento_servicio": "Pendiente",
        "cargo_servicio": "Analista",
        "total_personas": 1,
        "horas_interprete": 2.5,
        "mes_servicio": 3,
        "ano_servicio": 2026,
    }


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        google_service_account_file="C:/fake/service-account.json",
        google_drive_shared_folder_id="0AObDKzLYf4dYUk9PVA",
        google_drive_template_spreadsheet_name="ODS_FEB_2026",
    )


class GoogleDriveSyncTests(unittest.TestCase):
    def test_resolve_monthly_spreadsheet_name(self) -> None:
        self.assertEqual(resolve_monthly_spreadsheet_name(3, 2026), "ODS_MAR_2026")

    @patch("app.google_drive_sync.write_sheet_values")
    @patch("app.google_drive_sync.read_sheet_values")
    @patch("app.google_drive_sync.get_spreadsheet")
    @patch("app.google_drive_sync.list_drive_files")
    @patch("app.google_drive_sync.get_settings")
    def test_uses_existing_monthly_sheet(
        self,
        mock_get_settings,
        mock_list_drive_files,
        mock_get_spreadsheet,
        mock_read_sheet_values,
        mock_write_sheet_values,
    ) -> None:
        mock_get_settings.return_value = _settings()
        mock_list_drive_files.return_value = [{"id": "sheet-1", "name": "ODS_MAR_2026"}]
        mock_get_spreadsheet.return_value = {
            "sheets": [{"properties": {"title": "input"}}],
        }
        mock_read_sheet_values.side_effect = [[ODS_INPUT_HEADERS], []]

        result = sync_new_ods_record(_sample_ods())

        self.assertEqual(result["sync_status"], "ok")
        self.assertEqual(result["sync_target"], "ODS_MAR_2026")
        mock_write_sheet_values.assert_called_once()
        _, write_range, write_values = mock_write_sheet_values.call_args.args
        self.assertEqual(write_range, "'input'!A2:Y2")
        self.assertEqual(write_values[0][0], "ods-1")

    @patch("app.google_drive_sync.clear_sheet_values")
    @patch("app.google_drive_sync.copy_drive_file")
    @patch("app.google_drive_sync.write_sheet_values")
    @patch("app.google_drive_sync.read_sheet_values")
    @patch("app.google_drive_sync.get_spreadsheet")
    @patch("app.google_drive_sync.list_drive_files")
    @patch("app.google_drive_sync.get_settings")
    def test_creates_monthly_copy_from_template(
        self,
        mock_get_settings,
        mock_list_drive_files,
        mock_get_spreadsheet,
        mock_read_sheet_values,
        mock_write_sheet_values,
        mock_copy_drive_file,
        mock_clear_sheet_values,
    ) -> None:
        mock_get_settings.return_value = _settings()
        mock_list_drive_files.side_effect = [
            [],
            [{"id": "template-1", "name": "ODS_FEB_2026"}],
        ]
        mock_copy_drive_file.return_value = {"id": "sheet-2", "name": "ODS_MAR_2026"}
        mock_get_spreadsheet.return_value = {
            "sheets": [{"properties": {"title": "Input"}}],
        }
        mock_read_sheet_values.side_effect = [[ODS_INPUT_HEADERS], []]

        result = sync_new_ods_record(_sample_ods())

        self.assertEqual(result["sync_status"], "ok")
        mock_copy_drive_file.assert_called_once_with(
            "template-1",
            new_name="ODS_MAR_2026",
            parent_folder_id="0AObDKzLYf4dYUk9PVA",
        )
        mock_clear_sheet_values.assert_called_once_with("sheet-2", "'Input'!A2:Y")
        mock_write_sheet_values.assert_called_once()

    @patch("app.google_drive_sync.write_sheet_values")
    @patch("app.google_drive_sync.read_sheet_values")
    @patch("app.google_drive_sync.get_spreadsheet")
    @patch("app.google_drive_sync.list_drive_files")
    @patch("app.google_drive_sync.get_settings")
    def test_skips_insert_when_id_already_exists(
        self,
        mock_get_settings,
        mock_list_drive_files,
        mock_get_spreadsheet,
        mock_read_sheet_values,
        mock_write_sheet_values,
    ) -> None:
        ods = _sample_ods()
        mock_get_settings.return_value = _settings()
        mock_list_drive_files.return_value = [{"id": "sheet-1", "name": "ODS_MAR_2026"}]
        mock_get_spreadsheet.return_value = {
            "sheets": [{"properties": {"title": "input"}}],
        }
        mock_read_sheet_values.side_effect = [[ODS_INPUT_HEADERS], [[ods["id"]]]]

        result = sync_new_ods_record(ods)

        self.assertEqual(result["sync_status"], "ok")
        mock_write_sheet_values.assert_not_called()

    @patch("app.google_drive_sync.queue_google_drive_sync")
    @patch("app.google_drive_sync._sync_new_ods_record_once")
    def test_retryable_error_queues_pending_sync(
        self,
        mock_sync_once,
        mock_queue,
    ) -> None:
        mock_sync_once.side_effect = OSError("temporary network failure")

        result = sync_new_ods_record(_sample_ods())

        self.assertEqual(result["sync_status"], "pending")
        mock_queue.assert_called_once()

    @patch("app.google_drive_sync.queue_google_drive_sync")
    @patch("app.google_drive_sync.get_settings")
    def test_missing_config_returns_warning_without_queue(
        self,
        mock_get_settings,
        mock_queue,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            google_service_account_file="",
            google_drive_shared_folder_id="",
            google_drive_template_spreadsheet_name="",
        )

        result = sync_new_ods_record(_sample_ods())

        self.assertEqual(result["sync_status"], "warning")
        mock_queue.assert_not_called()

    @patch("app.google_drive_sync.queue_google_drive_sync")
    @patch("app.google_drive_sync.list_drive_files")
    @patch("app.google_drive_sync.get_settings")
    def test_duplicate_monthly_file_returns_warning(
        self,
        mock_get_settings,
        mock_list_drive_files,
        mock_queue,
    ) -> None:
        mock_get_settings.return_value = _settings()
        mock_list_drive_files.return_value = [
            {"id": "sheet-1", "name": "ODS_MAR_2026"},
            {"id": "sheet-2", "name": "ODS_MAR_2026"},
        ]

        result = sync_new_ods_record(_sample_ods())

        self.assertEqual(result["sync_status"], "warning")
        self.assertIn("multiples", result["sync_error"])
        mock_queue.assert_not_called()

    def test_queue_file_tracks_pending_entries(self) -> None:
        from app import google_drive_sync

        with tempfile.TemporaryDirectory() as tmpdir:
            queue_path = Path(tmpdir) / "pending.jsonl"
            with patch.object(google_drive_sync, "_QUEUE_FILE", queue_path):
                google_drive_sync.queue_google_drive_sync(_sample_ods(), reason="retryable_error")
                status = google_drive_sync.get_google_drive_queue_status()
        self.assertEqual(status["pendientes"], 1)

    @patch("app.google_drive_sync.read_sheet_values")
    @patch("app.google_drive_sync.get_spreadsheet")
    def test_accepts_ods_input_alias(self, mock_get_spreadsheet, mock_read_sheet_values) -> None:
        from app.google_drive_sync import _find_input_sheet

        mock_get_spreadsheet.return_value = {
            "sheets": [{"properties": {"title": "ODS_INPUT"}}],
        }
        mock_read_sheet_values.return_value = [ODS_INPUT_HEADERS]

        title, width = _find_input_sheet("sheet-1")

        self.assertEqual(title, "ODS_INPUT")
        self.assertEqual(width, len(ODS_INPUT_HEADERS))


if __name__ == "__main__":
    unittest.main()
