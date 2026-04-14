from __future__ import annotations

import unittest

from src.utils.premarket_plan import build_premarket_plan


class PremarketPlanTests(unittest.TestCase):
    def test_build_premarket_plan_prefers_successful_discussion(self) -> None:
        payload = build_premarket_plan(
            discussion_snapshot={
                "rounds": [
                    {
                        "round_label": "最新推理研讨",
                        "response_status": "success",
                        "summary_text": "外部模型认为这更像修复延续，盘前重点看 3.45 一带承接。",
                    }
                ]
            },
            action_brief="先看量价是否继续配合。",
            anchor_price=3.45,
            defensive_price=3.37,
            breakeven_price=3.85,
        )

        self.assertIn("最新推理研讨", payload["premarket_plan"])
        self.assertIn("3.45", payload["premarket_plan"])
        self.assertIn("3.37", payload["premarket_plan"])
        self.assertEqual(payload["premarket_plan_source"], "最新推理研讨")

    def test_build_premarket_plan_falls_back_to_system_brief(self) -> None:
        payload = build_premarket_plan(
            discussion_snapshot={"rounds": []},
            action_brief="先看量价是否继续配合，再决定是否转强。",
            anchor_price=3.45,
            defensive_price=3.37,
            breakeven_price=None,
        )

        self.assertIn("先看量价是否继续配合", payload["premarket_plan"])
        self.assertIn("3.45", payload["premarket_plan"])
        self.assertEqual(payload["premarket_plan_source"], "系统默认")


if __name__ == "__main__":
    unittest.main()
