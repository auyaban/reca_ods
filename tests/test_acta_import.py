from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.services.excel_acta_import import (
    _extract_pdf_asistentes_candidates,
    _extract_pdf_participants,
    parse_acta_pdf,
    parse_acta_source,
)


class ActaImportTests(unittest.TestCase):
    def test_parse_acta_source_rejects_empty_source(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Debe indicar la ruta o URL del acta"):
            parse_acta_source("")

        with self.assertRaisesRegex(RuntimeError, "Debe indicar la ruta o URL del acta"):
            parse_acta_source(None)

    @patch("app.services.excel_acta_import.parse_acta_excel")
    def test_parse_acta_source_uses_local_file(self, mock_parse_excel) -> None:
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_source(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900123456")
        mock_parse_excel.assert_called_once_with(str(temp_path))

    @patch("app.services.excel_acta_import.parse_acta_pdf")
    def test_parse_acta_source_uses_local_pdf(self, mock_parse_pdf) -> None:
        mock_parse_pdf.return_value = {"nit_empresa": "900123456"}

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_source(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900123456")
        mock_parse_pdf.assert_called_once_with(str(temp_path))

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.export_spreadsheet_to_excel")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_google_sheets_url(
        self,
        mock_get_metadata,
        mock_export_spreadsheet,
        mock_parse_excel,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "sheet-123",
            "name": "Acta marzo",
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0")

        self.assertEqual(result["source_type"], "google_sheets")
        self.assertEqual(result["file_path"], "https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0")
        mock_export_spreadsheet.assert_called_once()
        mock_parse_excel.assert_called_once()

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.download_drive_file")
    @patch("app.services.excel_acta_import.export_spreadsheet_to_excel")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_google_spreadsheets_url_backed_by_excel_file(
        self,
        mock_get_metadata,
        mock_export_spreadsheet,
        mock_download,
        mock_parse_excel,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "sheet-123",
            "name": "Acta marzo.xlsx",
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://docs.google.com/spreadsheets/d/sheet-123/edit?usp=drivesdk")

        self.assertEqual(result["source_type"], "google_drive_file")
        mock_export_spreadsheet.assert_not_called()
        mock_download.assert_called_once()
        mock_parse_excel.assert_called_once()

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.export_spreadsheet_to_excel")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_drive_google_sheet_link(
        self,
        mock_get_metadata,
        mock_export_spreadsheet,
        mock_parse_excel,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo",
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://drive.google.com/file/d/file-123/view")

        self.assertEqual(result["source_type"], "google_sheets")
        mock_export_spreadsheet.assert_called_once()
        mock_parse_excel.assert_called_once()

    @patch("app.services.excel_acta_import.parse_acta_excel")
    @patch("app.services.excel_acta_import.download_drive_file")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_drive_excel_link(
        self,
        mock_get_metadata,
        mock_download,
        mock_parse_excel,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.xlsx",
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        mock_parse_excel.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://drive.google.com/file/d/file-123/view")

        self.assertEqual(result["source_type"], "google_drive_file")
        mock_download.assert_called_once()
        mock_parse_excel.assert_called_once()

    @patch("app.services.excel_acta_import.parse_acta_pdf")
    @patch("app.services.excel_acta_import.download_drive_file")
    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_supports_drive_pdf_link(
        self,
        mock_get_metadata,
        mock_download,
        mock_parse_pdf,
    ) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.pdf",
            "mimeType": "application/pdf",
        }
        mock_parse_pdf.return_value = {"nit_empresa": "900123456"}

        result = parse_acta_source("https://drive.google.com/file/d/file-123/view")

        self.assertEqual(result["source_type"], "google_drive_file")
        mock_download.assert_called_once()
        mock_parse_pdf.assert_called_once()

    def test_parse_acta_source_rejects_unknown_url(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "No se pudo resolver el acta"):
            parse_acta_source("https://someotherdomain.com/file")

    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_rejects_unsupported_drive_file_type(self, mock_get_metadata) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.docx",
            "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        }

        with self.assertRaisesRegex(RuntimeError, "no es un Google Sheet, un Excel compatible"):
            parse_acta_source("https://drive.google.com/file/d/file-123/view")

    @patch("app.services.excel_acta_import.get_drive_file_metadata")
    def test_parse_acta_source_rejects_legacy_xls_drive_file(self, mock_get_metadata) -> None:
        mock_get_metadata.return_value = {
            "id": "file-123",
            "name": "Acta marzo.xls",
            "mimeType": "application/vnd.ms-excel",
        }

        with self.assertRaisesRegex(RuntimeError, "no es un Google Sheet, un Excel compatible"):
            parse_acta_source("https://drive.google.com/file/d/file-123/view")

    def test_extract_pdf_participants_reads_joined_rows(self) -> None:
        text = (
            "1 Leydi Marcela Ávila Ardila107292221470.00%Discapacidad física316 6253584Pendiente "
            "Agente de CatálogoMartha Aurora Ardila RiosMadre 3223997748 15/06/1995 30 años.\n"
            "2 Edward Mauricio Riaño Zamora8075054626.30%Discapacidad física310 2691234Pendiente "
            "Agente de CatálogoLeonardo ZamoraHermano 320 4167513 28/07/1985 40 años."
        )

        participants = _extract_pdf_participants(text)

        self.assertEqual(
            participants,
            [
                {
                    "nombre_usuario": "Leydi Marcela Ávila Ardila",
                    "cedula_usuario": "1072922214",
                    "discapacidad_usuario": "física",
                    "genero_usuario": "",
                },
                {
                    "nombre_usuario": "Edward Mauricio Riaño Zamora",
                    "cedula_usuario": "80750546",
                    "discapacidad_usuario": "física",
                    "genero_usuario": "",
                },
            ],
        )


    def test_extract_pdf_participants_reads_inline_oferente_section_without_decimal_percentage(self) -> None:
        text = (
            "Sede Compensar:Mosquera2. DATOS DEL OFERENTE"
            "CITADO A ENTREVISTA CERTIFICADOTELÃ‰FONORESULTADO: No NOMBRE OFERENTECÃ‰DULA % DISCAPACIDAD"
            "1 Juan Camilo Villa Hernadez 107352067650%Discapacidad auditiva hipoacusia3176819904No aprobado"
            "CARGOCONTACTO DE EMERGENCIAPARENTESCOTELÃ‰FONO FECHA DE NACIMIENTOEDAD"
            "Auxiliar operativo Maria victoriaAmiga 312546789 12 /11/1996 30 aÃ±os\n"
            "Â¿Pendiente otros oferentes para entrevista?Ninguno\n"
            "3. DESARROLLO DE LA ACTIVIDAD"
        )

        participants = _extract_pdf_participants(text)

        self.assertEqual(
            participants,
            [
                {
                    "nombre_usuario": "Juan Camilo Villa Hernadez",
                    "cedula_usuario": "1073520676",
                    "discapacidad_usuario": "auditiva hipoacusia",
                    "genero_usuario": "",
                }
            ],
        )

    def test_extract_pdf_participants_reads_groupal_vinculados_layout(self) -> None:
        text = (
            "3. DATOS DEL VINCULADO\n"
            "1 Cesar Nayid Roncancio Perdomo102392909725,5 Discapacidad física3223486437\n"
            "Masculino ccuervoroncanciom@gmail.com 09/11/1993 32 años\n"
            "2 Zuly Paola Ramirez Maldonado107370045820,9 Discapacidad física3202157970\n"
            "Femenino jpao2701@hotmail.com 27/02/1994 32 años\n"
        )

        participants = _extract_pdf_participants(text)

        self.assertEqual(
            participants,
            [
                {
                    "nombre_usuario": "Cesar Nayid Roncancio Perdomo",
                    "cedula_usuario": "1023929097",
                    "discapacidad_usuario": "física",
                    "genero_usuario": "",
                },
                {
                    "nombre_usuario": "Zuly Paola Ramirez Maldonado",
                    "cedula_usuario": "1073700458",
                    "discapacidad_usuario": "física",
                    "genero_usuario": "",
                },
            ],
        )

    def test_extract_pdf_participants_reads_selection_row_outside_expected_section(self) -> None:
        text = (
            "4. CARACTERIZACION DEL OFERENTE\n"
            "Por confirmar Fecha firma de contrato:\n"
            "Angie Lorena Avellaneda Chaparro 1034657640 Discapacidad visual baja vision 3112544990 Pendiente\n"
            "2. DATOS DEL OFERENTE\n"
            "CITADO A ENTREVISTA CERTIFICADO TELEFONO RESULTADO: No DISCAPACIDAD NOMBRE OFERENTE CEDULA\n"
        )

        participants = _extract_pdf_participants(text)

        self.assertEqual(
            participants,
            [
                {
                    "nombre_usuario": "Angie Lorena Avellaneda Chaparro",
                    "cedula_usuario": "1034657640",
                    "discapacidad_usuario": "visual baja vision",
                    "genero_usuario": "",
                }
            ],
        )

    def test_extract_pdf_asistentes_candidates_prefers_nombre_completo_order(self) -> None:
        text = (
            "3. Asistentes\n"
            "Nombre completo: Gabriela Rubiano Isaza Cargo: Profesional de inclusion laboral\n"
            "Nombre completo: Lissette Lorena Castaneda Cargo: Psicologa\n"
            "La presente acta deja constancia del proceso.\n"
        )

        candidates = _extract_pdf_asistentes_candidates(text)

        self.assertEqual(
            candidates,
            [
                "Gabriela Rubiano Isaza",
                "Lissette Lorena Castaneda",
            ],
        )

    def test_extract_pdf_asistentes_candidates_reads_multiline_asistentes_block(self) -> None:
        text = (
            "8.ASISTENTES\n"
            "Nombre completo: Sandra Milena Pachon Rojas\n"
            "Nombre completo: Ana Maria Malagon\n"
            "Coordinacion de inclusion laboral\n"
            "Lider Desarrollo Talento\n"
            "Nombre completo: Francia Palacios\n"
        )

        candidates = _extract_pdf_asistentes_candidates(text)

        self.assertEqual(
            candidates,
            [
                "Sandra Milena Pachon Rojas",
                "Ana Maria Malagon",
                "Francia Palacios",
            ],
        )

    def test_extract_pdf_asistentes_candidates_falls_back_to_full_text_when_asistentes_block_is_late(self) -> None:
        text = (
            "Nombre completo: Leidy Novoa Profesional de apoyo\n"
            "Nombre completo: Silvana Pomarico\n"
            "8. ASISTENTES\n"
            "Sin firmas registradas.\n"
        )

        candidates = _extract_pdf_asistentes_candidates(text)

        self.assertEqual(
            candidates,
            [
                "Leidy Novoa",
                "Silvana Pomarico",
            ],
        )

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_supports_layout_with_values_before_labels(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "1.DATOS GENERALES",
                    "Número de NIT: 900439301-2",
                    "05/03/2026 Modalidad: Virtual",
                    "INVERSIONES INT COLOMBIA SAS Ciudad/Municipio: Bogotá",
                    "Cra 22 # 83 - 31",
                    "Fecha de la Visita:",
                    "Nombre de la Empresa:",
                    "Dirección de la Empresa:",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900439301-2")
        self.assertEqual(result["fecha_servicio"], "2026-03-05")
        self.assertEqual(result["modalidad_servicio"], "Virtual")
        self.assertEqual(result["nombre_empresa"], "INVERSIONES INT COLOMBIA SAS")
        self.assertNotIn("No se detecto nombre de empresa en el PDF.", result["warnings"])
        self.assertNotIn("No se detecto fecha de servicio en formato valido.", result["warnings"])

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_supports_groupal_vinculados_layout(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "PROCESO CONTRATACION INCLUYENTE GRUPAL - 2 A 4 OFERENTES",
                    "Fecha de la Visita: 03-03-2026 Modalidad:Virtual",
                    "Nombre de la Empresa:SIS VIDA SAS Ciudad/Municipio:Bogotá",
                    "Dirección de la Empresa:Cra. 23 #166-36 Número de NIT: 830132432-6",
                    "Asesor: Andrea Carolina Guevara Gonzalez",
                    "Profesional asignadoRECA: Adriana González Moreno",
                    "1 Cesar Nayid Roncancio Perdomo102392909725,5 Discapacidad física3223486437",
                    "Masculino ccuervoroncanciom@gmail.com 09/11/1993 32 años",
                ]
            ),
            "\n".join(
                [
                    "2 Zuly Paola Ramirez Maldonado107370045820,9 Discapacidad física3202157970",
                    "Femenino jpao2701@hotmail.com 27/02/1994 32 años",
                ]
            ),
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["fecha_servicio"], "2026-03-03")
        self.assertEqual(result["nombre_profesional"], "Adriana González Moreno")
        self.assertEqual(
            result["participantes"],
            [
                {
                    "nombre_usuario": "Cesar Nayid Roncancio Perdomo",
                    "cedula_usuario": "1023929097",
                    "discapacidad_usuario": "física",
                    "genero_usuario": "",
                },
                {
                    "nombre_usuario": "Zuly Paola Ramirez Maldonado",
                    "cedula_usuario": "1073700458",
                    "discapacidad_usuario": "física",
                    "genero_usuario": "",
                },
            ],
        )

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_extracts_selection_candidate_when_row_is_before_section(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "4. CARACTERIZACION DEL OFERENTE",
                    "Por confirmar Fecha firma de contrato:",
                    "Angie Lorena Avellaneda Chaparro 1034657640 Discapacidad visual baja vision 3112544990 Pendiente",
                    "2. DATOS DEL OFERENTE",
                    "Asesor: Dennis Katherin Lozano Hoyos Profesional asignado RECA:",
                    "Modalidad: Virtual",
                    "Nombre completo: Leidy Novoa Profesional de apoyo",
                    "Nombre completo: Silvana Pomarico",
                    "8. ASISTENTES",
                    "1. DATOS DE LA EMPRESA",
                    "Fecha de la Visita: 02/03/2026",
                    "Nombre de la Empresa: GALLAGHER CONSULTING LTDA Ciudad/Municipio: Bogota",
                    "Numero de NIT: 901024978-1",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "901024978-1")
        self.assertEqual(result["nombre_empresa"], "GALLAGHER CONSULTING LTDA")
        self.assertEqual(result["fecha_servicio"], "2026-03-02")
        self.assertEqual(result["modalidad_servicio"], "Virtual")
        self.assertEqual(result["nombre_profesional"], "Leidy Novoa")
        self.assertEqual(len(result["participantes"]), 1)

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_supports_accessibility_layout_without_false_oferentes(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "EVALUACIÓN DE ACCESIBILIDAD",
                    "Fecha de la Visita:10/3/2026 Modalidad: PresencialNombre de la Empresa:CARLOS RANGEL GALVIS Ciudad/Municipio:Bogotá",
                    "Dirección de la Empresa:Av. Cl. 20 # 43A – 32 Número de NIT: 900352592-3Correo electrónico:gestionhumana@rangelrehabilitacion.com.coanalistagghh@rangelrehabilitacion.com.coTeléfonos: 3160273992",
                    "Contacto de la empresa:Andres Gerardo Maldonado Triana Camilo Andres Palma Cargo: Coordinador de Gestión Humana",
                    "Empresa afiliada a Caja de Compensación:Compensar Sede Compensar:Suba Asesor: Sergio David Velez España Profesional asignado RECA:Adriana Gonzalez",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900352592-3")
        self.assertEqual(result["nits_empresas"], ["900352592-3"])
        self.assertEqual(result["fecha_servicio"], "2026-03-10")
        self.assertEqual(result["modalidad_servicio"], "Presencial")
        self.assertEqual(result["nombre_empresa"], "CARLOS RANGEL GALVIS")
        self.assertEqual(result["participantes"], [])
        self.assertNotIn("No se detectaron oferentes en el PDF.", result["warnings"])

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_supports_accessibility_layout_with_values_before_labels(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "EVALUACIÓN DE ACCESIBILIDAD",
                    "1.DATOS DE LA EMPRESA",
                    "Número de NIT: 900887188-7",
                    "2/3/2026 Modalidad: Presencial",
                    "CHAZEY PARTNERS COLOMBIA S A S Ciudad/Municipio: Bogotá",
                    "Av. Calle 26 No. 92-32, Edificio G2 – G3",
                    "Fecha de la Visita:",
                    "Nombre de la Empresa:",
                    "Dirección de la Empresa:",
                    "Correo electrónico:",
                    "luisahernandez@chazeypartner.com",
                    "Profesional asignado RECA:Gabriela Rubiano",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900887188-7")
        self.assertEqual(result["fecha_servicio"], "2026-03-02")
        self.assertEqual(result["modalidad_servicio"], "Presencial")
        self.assertEqual(result["nombre_empresa"], "CHAZEY PARTNERS COLOMBIA S A S")
        self.assertEqual(result["participantes"], [])
        self.assertNotIn("No se detecto nombre de empresa en el PDF.", result["warnings"])

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_extracts_vacancy_fields_without_false_oferente_warning(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "REVISIÓN DE LAS CONDICIONES DE LA VACANTE",
                    "Fecha de la Visita: 13/03/2026 Modalidad: Virtual",
                    "Nombre de la Empresa:PAREX RESOURCES (COLOMBIA) AG SUCURSALCiudad/Municipio:Bogotá",
                    "Dirección de la Empresa:Calle 113 #7 -21 of 611 torre A Número de NIT: 900268747-9",
                    "Asesor: Luisa María Angarita Profesional asignado RECA:Alejandra Pérez",
                    "Nombre de la vacante: Analista HSNúmero de vacantes: 2Nivel del cargo: Administrativo.",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "900268747-9")
        self.assertEqual(result["nombre_empresa"], "PAREX RESOURCES (COLOMBIA) AG SUCURSAL")
        self.assertEqual(result["cargo_objetivo"], "Analista HS")
        self.assertEqual(result["total_vacantes"], 2)
        self.assertEqual(result["participantes"], [])
        self.assertNotIn("No se detectaron oferentes en el PDF.", result["warnings"])

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_extracts_selection_cargo_from_oferente_section(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "PROCESO DE SELECCIÓN INCLUYENTE INDIVIDUAL",
                    "Fecha de la Visita:3/10/2026 Modalidad:Virtual",
                    "Nombre de la Empresa:ICOL CONSULTORES SASCiudad/Municipio:Bogotá",
                    "Dirección de la Empresa:Cl 100 #19a-30 Número de NIT:901376637-3",
                    "2. DATOS DEL OFERENTE",
                    "1 Michael Smit Vargas Guiza101847397850 Discapacidad visual baja visión3224611064Aprobado",
                    "CARGO CONTACTO DE EMERGENCIA PARENTESCO TELÉFONO FECHA DE NACIMIENTO EDAD",
                    "Auxiliar administrativo Aura Lucia Guisa Madre 3164699064 05/02/1995 31",
                    "3. DESARROLLO DE LA ACTIVIDAD",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["cargo_objetivo"], "Auxiliar administrativo")

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_extracts_follow_up_participant_with_percentage(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "SEGUIMIENTO AL PROCESO DE INCLUSIÓN LABORAL",
                    "Fecha de la Visita: 02/03/2026 Modalidad:Presencial",
                    "Nombre de la Empresa:INCODEPF S.A Ciudad/Municipio:Funza",
                    "Dirección de la Empresa:Cl. 16 #2-64 Número de NIT: 86045023-4",
                    "Asesor: Paola Andrea Uribe Ramirez Sede Compensar:Mosquera",
                    "Andrés Nicolas Gomez Rueda10732549343164601112ruedaandres3144@gmail.comJuan Carlos Gómez Camelo Padre 3112382985",
                    "Auxiliar AdministrativoSi 27.88% Discapacidad visual baja visión",
                    "Seguimiento 1:2/3/2026 Seguimiento 4:",
                ]
            )
        ]

        path = Path("SEGUIMIENTO AL PROCESO DE INCLUSION LABORA - (1) Andres Gomez - 02_Mar_2026.pdf")
        with patch("pathlib.Path.exists", return_value=True):
            result = parse_acta_pdf(str(path))

        self.assertEqual(result["numero_seguimiento"], "1")
        self.assertEqual(result["participantes"][0]["nombre_usuario"], "Andres Gomez")
        self.assertEqual(result["participantes"][0]["cedula_usuario"], "1073254934")
        self.assertEqual(result["participantes"][0]["discapacidad_usuario"], "visual baja visión")

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_supports_interpreter_layout_and_sumatoria_hours(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "1. DATOS DE LA EMPRESA Fecha: 04/03/2026",
                    "Numero de NIT: 860001000-1",
                    "Nombre de la empresa: SOLLA S.A Direccion: Calle 1 # 2 - 3",
                    "Modalidad servicio: Presencial Interprete: Laura Demo",
                    "Profesional RECA: Ana Perez 2. DATOS DE LOS OFERENTES/ VINCULADOS",
                    "1 Juan Camilo Villa 1073520676 Proceso de seleccion individual",
                    "Nombre interprete: Laura Demo",
                    "Total Tiempo: 1 Hora si el servicio fue realizado en sabana se agrega una hora",
                    "SUMATORIA HORAS INTERPRETES: 2 horas Observaciones: prueba",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "860001000-1")
        self.assertEqual(result["nombre_empresa"], "SOLLA S.A")
        self.assertEqual(result["fecha_servicio"], "2026-03-04")
        self.assertEqual(result["sumatoria_horas_interpretes"], 2.0)
        self.assertEqual(result["total_horas_interprete"], 1.0)
        self.assertEqual(result["participantes"][0]["cedula_usuario"], "1073520676")

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_extracts_follow_up_number_and_vinculado_layout(self, mock_extract_pages) -> None:
        mock_extract_pages.return_value = [
            "\n".join(
                [
                    "SEGUIMIENTO AL PROCESO DE INCLUSIÓN LABORAL",
                    "Fecha de la Visita: 09/03/2026 Modalidad:Presencial",
                    "Nombre de la Empresa:AMBIENTII CONSTRUCTORA INMOBILIARIA S.A.SCiudad/Municipio:Bogotá",
                    "Dirección de la Empresa:Carrera 16 a No. 78 - 11 Piso 6 Número de NIT: 830060858-1",
                    "Persona que atiende lavisita en la empresa:Camila MantillaCarolina Olaya Cargo: Gerente AdmnistrativaCoordinadora Talento Humano",
                    "Asesor: Dennis Katherin Lozano HoyosSede Compensar:Chapinero",
                    "Lisandro Rivas Segura10039450133202563056rivaslisandro160@gmail.comDarlyn RivasHermana 3112663968",
                    "Ayudante de Obra InlcusiónLaboral Si No aplica. Discapacidad física",
                    "Seguimiento 1:9/2/2026 Seguimiento 4:",
                    "Seguimiento 2:9/3/2026 Seguimiento 5:",
                    "Seguimiento 3: Seguimiento 6:",
                ]
            )
        ]

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            temp_path = Path(tmp.name)

        try:
            result = parse_acta_pdf(str(temp_path))
        finally:
            if temp_path.exists():
                temp_path.unlink()

        self.assertEqual(result["nit_empresa"], "830060858-1")
        self.assertEqual(result["nombre_empresa"], "AMBIENTII CONSTRUCTORA INMOBILIARIA S.A.S")
        self.assertEqual(result["fecha_servicio"], "2026-03-09")
        self.assertEqual(result["modalidad_servicio"], "Presencial")
        self.assertEqual(result["numero_seguimiento"], "2")
        self.assertEqual(result["participantes"][0]["nombre_usuario"], "Lisandro Rivas Segura")
        self.assertEqual(result["participantes"][0]["cedula_usuario"], "1003945013")

    @patch("app.services.excel_acta_import._extract_pdf_text_pages")
    def test_parse_acta_pdf_extracts_follow_up_vinculado_with_percentage_or_no_refiere(self, mock_extract_pages) -> None:
        dayana_pages = [
            "\n".join(
                [
                    "SEGUIMIENTO AL PROCESO DE INCLUSIÓN LABORAL",
                    "Fecha de la Visita: 04/03/2026 Modalidad:Virtual",
                    "Nombre de la Empresa:SIS VIDA SAS Ciudad/Municipio:Bogotá",
                    "Dirección de la Empresa:Cra. 23 #166-36 Número de NIT: 830132432-6",
                    "Asesor: Andrea Carolina Guevara GonzalezSede Compensar:Suba",
                    "12338917973148674569dayana3597@hotmail.comLuz Estella RojasMadre 3138506720",
                    "Analista Operativo -Inclusión LaboralSi 41.4 Discapacidad física",
                    "Seguimiento 1:11/12/2025 Seguimiento 4:",
                    "Seguimiento 2:30/01/2026 Seguimiento 5:",
                    "Seguimiento 3:04/03/2026 Seguimiento 6:",
                ]
            )
        ]
        estefania_pages = [
            "\n".join(
                [
                    "SEGUIMIENTO AL PROCESO DE INCLUSIÓN LABORAL",
                    "Fecha de la Visita: 04/03/2026 Modalidad:Virtual",
                    "Nombre de la Empresa:SIS VIDA SAS Ciudad/Municipio:Bogotá",
                    "Dirección de la Empresa:Cra. 23 #166-36 Número de NIT: 830132432-6",
                    "Asesor: Andrea Carolina Guevara GonzalezSede Compensar:Suba",
                    "Estefania Zabala Cuadros12338970643142039495stezabala03@gmail.comSonia CuadrosMadre 3103066115",
                    "Analista Operativo -Inclusión LaboralSi No refiere Discapacidad física",
                    "Seguimiento 1:11/12/2025 Seguimiento 4:",
                    "Seguimiento 2:30/01/2026 Seguimiento 5:",
                    "Seguimiento 3:04/03/2026 Seguimiento 6:",
                ]
            )
        ]
        mock_extract_pages.side_effect = [dayana_pages, estefania_pages]

        path1 = Path("SEGUIMIENTO AL PROCESO DE INCLUSION LABORAL (3) - Dayana Salazar- 04_Mar_2026.pdf")
        path2 = Path("SEGUIMIENTO AL PROCESO DE INCLUSION LABORAL (3) - Estefania Zabala - 04_Mar_2026.pdf")
        with patch("pathlib.Path.exists", return_value=True):
            result_dayana = parse_acta_pdf(str(path1))
            result_estefania = parse_acta_pdf(str(path2))

        self.assertEqual(result_dayana["numero_seguimiento"], "3")
        self.assertEqual(result_dayana["participantes"][0]["nombre_usuario"], "Dayana Salazar")
        self.assertEqual(result_dayana["participantes"][0]["cedula_usuario"], "1233891797")
        self.assertEqual(result_dayana["participantes"][0]["discapacidad_usuario"], "física")
        self.assertEqual(result_estefania["numero_seguimiento"], "3")
        self.assertEqual(result_estefania["participantes"][0]["nombre_usuario"], "Estefania Zabala")
        self.assertEqual(result_estefania["participantes"][0]["cedula_usuario"], "1233897064")
        self.assertEqual(result_estefania["participantes"][0]["discapacidad_usuario"], "física")


if __name__ == "__main__":
    unittest.main()
