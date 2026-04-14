from __future__ import annotations

import unittest

from src.app.page_registry import PageDefinition, build_page_registry, page_labels, render_registered_page


class PageRegistryTests(unittest.TestCase):
    def test_page_labels_keep_declared_order(self) -> None:
        registry = build_page_registry(
            PageDefinition("a", "页面A", lambda: None),
            PageDefinition("b", "页面B", lambda: None),
        )

        self.assertEqual(page_labels(registry), ["页面A", "页面B"])

    def test_render_registered_page_runs_matching_renderer(self) -> None:
        called: list[str] = []
        registry = build_page_registry(
            PageDefinition("a", "页面A", lambda: called.append("A")),
            PageDefinition("b", "页面B", lambda: called.append("B")),
        )

        render_registered_page(registry, "页面B")

        self.assertEqual(called, ["B"])

    def test_render_registered_page_raises_for_unknown_label(self) -> None:
        registry = build_page_registry(PageDefinition("a", "页面A", lambda: None))

        with self.assertRaises(KeyError):
            render_registered_page(registry, "页面C")


if __name__ == "__main__":
    unittest.main()
