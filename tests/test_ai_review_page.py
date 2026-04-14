from __future__ import annotations

import unittest

from src.app.viewmodels.ai_review_vm import build_llm_response_lookup


class AIReviewPageTests(unittest.TestCase):
    def test_build_llm_response_lookup_uses_custom_id_and_skips_blank(self) -> None:
        records = [
            {"custom_id": "002583.SZ", "status": "success"},
            {"custom_id": "  ", "status": "ignored"},
            {"custom_id": "000078.SZ", "status": "failed"},
        ]

        lookup = build_llm_response_lookup(records)

        self.assertEqual(set(lookup.keys()), {"002583.SZ", "000078.SZ"})
        self.assertEqual(lookup["002583.SZ"]["status"], "success")
        self.assertEqual(lookup["000078.SZ"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
