from __future__ import annotations

import unittest

from app.automation.gmail_inbox import _collect_pdf_parts


class AutomationGmailInboxTests(unittest.TestCase):
    def test_collect_pdf_parts_ignores_inline_images_and_deduplicates_leaf_pdf(self) -> None:
        payload = {
            "parts": [
                {
                    "mimeType": "image/png",
                    "filename": "image.png",
                    "body": {"attachmentId": "img-1", "size": 100},
                },
                {
                    "mimeType": "application/pdf",
                    "filename": "acta.pdf",
                    "body": {"attachmentId": "pdf-1", "size": 200},
                },
                {
                    "mimeType": "multipart/alternative",
                    "parts": [
                        {
                            "mimeType": "application/pdf",
                            "filename": "nested.pdf",
                            "body": {"attachmentId": "pdf-2", "size": 300},
                        }
                    ],
                },
            ]
        }

        parts = _collect_pdf_parts(payload)

        self.assertEqual([item["filename"] for item in parts], ["acta.pdf", "nested.pdf"])


if __name__ == "__main__":
    unittest.main()
