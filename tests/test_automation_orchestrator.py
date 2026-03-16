from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.automation.orchestrator import (
    get_automation_staging_case,
    get_automation_staging_cases,
    get_automation_test_status,
    save_automation_staging_case,
    update_automation_staging_case,
)


class AutomationOrchestratorTests(unittest.TestCase):
    def test_returns_skeleton_status_payload(self) -> None:
        result = get_automation_test_status()

        self.assertIn("data", result)
        data = result["data"]
        self.assertEqual(data["title"], "Aaron TEST")
        self.assertEqual(data["environment"], "test_only")
        self.assertGreaterEqual(len(data["components"]), 4)
        self.assertGreaterEqual(len(data["next_steps"]), 1)

    @patch("app.automation.orchestrator.get_automation_attachment_analysis")
    @patch("app.automation.orchestrator.AutomationStagingRepository")
    def test_save_staging_case_uses_analysis_result(self, mock_repo_cls, mock_get_analysis) -> None:
        mock_get_analysis.return_value = {
            "data": {
                "message": {"message_id": "msg-1"},
                "attachment": {"filename": "acta.pdf"},
                "analysis": {"nombre_empresa": "Empresa Demo"},
                "suggestion": {"codigo_servicio": "37"},
            }
        }
        repo = mock_repo_cls.return_value
        repo.save_case.return_value.to_dict.return_value = {"case_id": "auto-123"}

        result = save_automation_staging_case({"message_id": "msg-1", "attachment_index": 0})

        self.assertEqual(result["data"]["case_id"], "auto-123")
        repo.save_case.assert_called_once()

    @patch("app.automation.orchestrator.AutomationStagingRepository")
    def test_get_staging_cases_returns_serialized_rows(self, mock_repo_cls) -> None:
        repo = mock_repo_cls.return_value
        staged_case = MagicMock()
        staged_case.to_dict.return_value = {"case_id": "auto-123", "status": "pending_review"}
        repo.list_cases.return_value = [staged_case]

        result = get_automation_staging_cases()

        self.assertEqual(result["data"]["count"], 1)
        self.assertEqual(result["data"]["cases"][0]["case_id"], "auto-123")

    @patch("app.automation.orchestrator.AutomationStagingRepository")
    def test_get_staging_case_returns_one_case(self, mock_repo_cls) -> None:
        repo = mock_repo_cls.return_value
        staged_case = MagicMock()
        staged_case.to_dict.return_value = {"case_id": "auto-123", "status": "pending_review"}
        repo.get_case.return_value = staged_case

        result = get_automation_staging_case("auto-123")

        self.assertEqual(result["data"]["case_id"], "auto-123")

    @patch("app.automation.orchestrator.AutomationStagingRepository")
    def test_update_staging_case_passes_changes_to_repository(self, mock_repo_cls) -> None:
        repo = mock_repo_cls.return_value
        staged_case = MagicMock()
        staged_case.to_dict.return_value = {"case_id": "auto-123", "status": "approved_for_publish"}
        repo.update_case.return_value = staged_case

        result = update_automation_staging_case(
            {
                "case_id": "auto-123",
                "status": "approved_for_publish",
                "suggestion": {"codigo_servicio": "47"},
            }
        )

        self.assertEqual(result["data"]["status"], "approved_for_publish")
        repo.update_case.assert_called_once()


if __name__ == "__main__":
    unittest.main()
