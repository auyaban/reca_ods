from __future__ import annotations

import unittest

from app.automation.orchestrator import get_automation_test_status


class AutomationOrchestratorTests(unittest.TestCase):
    def test_returns_skeleton_status_payload(self) -> None:
        result = get_automation_test_status()

        self.assertIn("data", result)
        data = result["data"]
        self.assertEqual(data["title"], "Aaron TEST")
        self.assertEqual(data["environment"], "test_only")
        self.assertGreaterEqual(len(data["components"]), 4)
        self.assertGreaterEqual(len(data["next_steps"]), 1)


if __name__ == "__main__":
    unittest.main()
