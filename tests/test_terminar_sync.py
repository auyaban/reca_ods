from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.models.payloads import OdsPayload, TerminarServicioRequest
from app.services.sections import terminar


def _request() -> TerminarServicioRequest:
    ods = OdsPayload(
        orden_clausulada="si",
        nombre_profesional="Ana Perez",
        nit_empresa="900123456",
        nombre_empresa="Empresa Demo",
        caja_compensacion="Compensar",
        asesor_empresa="Asesor Demo",
        sede_empresa="Principal",
        fecha_servicio="2026-03-12",
        codigo_servicio="COD-001",
        referencia_servicio="REF-1",
        descripcion_servicio="Servicio Demo",
        modalidad_servicio="Virtual",
        valor_virtual=100.0,
        valor_bogota=0.0,
        valor_otro=0.0,
        todas_modalidades=0.0,
        horas_interprete=2.5,
        valor_interprete=0.0,
        valor_total=100.0,
        nombre_usuario="Carlos Ruiz",
        cedula_usuario="123456",
        discapacidad_usuario="Auditiva",
        genero_usuario="Masculino",
        fecha_ingreso="2026-03-01",
        tipo_contrato="Laboral",
        cargo_servicio="Analista",
        total_personas=1,
        observaciones="Sin novedad",
        observacion_agencia="Ninguna",
        seguimiento_servicio="Pendiente",
        mes_servicio=3,
        ano_servicio=2026,
        session_id="bce90f35-190b-44a0-a16c-aeafec190742",
        started_at="2026-03-12T08:00:00+00:00",
        submitted_at="2026-03-12T08:12:30+00:00",
    )
    return TerminarServicioRequest(ods=ods, usuarios_nuevos=[])


def _inserted_row() -> dict:
    payload = _request().ods.model_dump()
    payload["id"] = "ods-1"
    payload["orden_clausulada"] = True
    return payload


class TerminarServicioSyncTests(unittest.TestCase):
    @patch("app.services.sections.terminar._fetch_ods_schema_cached", return_value={})
    @patch("app.services.sections.terminar.get_settings")
    def test_fetch_schema_uses_local_fallback_when_remote_schema_unavailable(
        self,
        mock_get_settings,
        _mock_fetch_cached,
    ) -> None:
        mock_get_settings.return_value = SimpleNamespace(
            supabase_url="https://example.supabase.co",
            supabase_anon_key="anon-key",
        )

        schema = terminar._fetch_ods_schema()

        self.assertEqual(schema["orden_clausulada"]["type"], "boolean")
        self.assertEqual(schema["a\u00f1o_servicio"]["type"], "integer")

    @patch(
        "app.services.sections.terminar._fetch_ods_schema",
        return_value=dict(terminar._ODS_FALLBACK_SCHEMA),
    )
    def test_apply_schema_maps_year_field_and_boolean_with_local_schema(self, _mock_schema) -> None:
        applied = terminar._apply_schema(_request().ods.model_dump())

        self.assertTrue(applied["orden_clausulada"])
        self.assertNotIn("ano_servicio", applied)
        self.assertEqual(applied["a\u00f1o_servicio"], 2026)

    @patch("app.services.sections.terminar.sync_new_ods_record")
    @patch("app.services.sections.terminar.execute_with_reauth")
    @patch("app.services.sections.terminar._fetch_ods_schema")
    def test_returns_ok_sync_status(
        self,
        mock_schema,
        mock_execute_with_reauth,
        mock_sync_new_ods_record,
    ) -> None:
        mock_schema.return_value = dict(terminar._ODS_FALLBACK_SCHEMA)
        mock_execute_with_reauth.return_value = SimpleNamespace(data=[_inserted_row()])
        mock_sync_new_ods_record.return_value = {
            "sync_status": "ok",
            "sync_error": None,
            "sync_target": "ODS_MAR_2026",
        }

        response = terminar.terminar_servicio(_request(), background_tasks=None)

        self.assertEqual(response["sync_status"], "ok")
        self.assertEqual(response["sync_target"], "ODS_MAR_2026")

    @patch("app.services.sections.terminar.sync_new_ods_record")
    @patch("app.services.sections.terminar.execute_with_reauth")
    @patch("app.services.sections.terminar._fetch_ods_schema")
    def test_returns_pending_sync_status(
        self,
        mock_schema,
        mock_execute_with_reauth,
        mock_sync_new_ods_record,
    ) -> None:
        mock_schema.return_value = dict(terminar._ODS_FALLBACK_SCHEMA)
        mock_execute_with_reauth.return_value = SimpleNamespace(data=[_inserted_row()])
        mock_sync_new_ods_record.return_value = {
            "sync_status": "pending",
            "sync_error": "retry later",
            "sync_target": "ODS_MAR_2026",
        }

        response = terminar.terminar_servicio(_request(), background_tasks=None)

        self.assertEqual(response["sync_status"], "pending")
        self.assertEqual(response["sync_error"], "retry later")

    @patch("app.services.sections.terminar.sync_new_ods_record")
    @patch("app.services.sections.terminar.execute_with_reauth")
    @patch("app.services.sections.terminar._fetch_ods_schema")
    def test_returns_warning_sync_status(
        self,
        mock_schema,
        mock_execute_with_reauth,
        mock_sync_new_ods_record,
    ) -> None:
        mock_schema.return_value = dict(terminar._ODS_FALLBACK_SCHEMA)
        mock_execute_with_reauth.return_value = SimpleNamespace(data=[_inserted_row()])
        mock_sync_new_ods_record.return_value = {
            "sync_status": "warning",
            "sync_error": "template missing",
            "sync_target": "ODS_MAR_2026",
        }

        response = terminar.terminar_servicio(_request(), background_tasks=None)

        self.assertEqual(response["sync_status"], "warning")
        self.assertEqual(response["sync_error"], "template missing")

    def test_validates_submission_timestamps_order(self) -> None:
        with self.assertRaisesRegex(ValueError, "submitted_at no puede ser anterior a started_at"):
            OdsPayload(
                orden_clausulada="si",
                nombre_profesional="Ana Perez",
                nit_empresa="900123456",
                nombre_empresa="Empresa Demo",
                caja_compensacion="Compensar",
                asesor_empresa="Asesor Demo",
                sede_empresa="Principal",
                fecha_servicio="2026-03-12",
                codigo_servicio="COD-001",
                referencia_servicio="REF-1",
                descripcion_servicio="Servicio Demo",
                modalidad_servicio="Virtual",
                valor_virtual=100.0,
                valor_bogota=0.0,
                valor_otro=0.0,
                todas_modalidades=0.0,
                horas_interprete=2.5,
                valor_interprete=0.0,
                valor_total=100.0,
                nombre_usuario="Carlos Ruiz",
                cedula_usuario="123456",
                discapacidad_usuario="Auditiva",
                genero_usuario="Masculino",
                fecha_ingreso="2026-03-01",
                tipo_contrato="Laboral",
                cargo_servicio="Analista",
                total_personas=1,
                observaciones="Sin novedad",
                observacion_agencia="Ninguna",
                seguimiento_servicio="Pendiente",
                mes_servicio=3,
                ano_servicio=2026,
                session_id="bce90f35-190b-44a0-a16c-aeafec190742",
                started_at="2026-03-12T08:12:30+00:00",
                submitted_at="2026-03-12T08:00:00+00:00",
            )


if __name__ == "__main__":
    unittest.main()
