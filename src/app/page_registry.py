from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class PageDefinition:
    key: str
    label: str
    render: Callable[[], None]


def build_page_registry(*page_definitions: PageDefinition) -> list[PageDefinition]:
    return list(page_definitions)


def page_labels(page_registry: list[PageDefinition]) -> list[str]:
    return [page.label for page in page_registry]


def render_registered_page(page_registry: list[PageDefinition], current_page_label: str) -> None:
    for page in page_registry:
        if page.label == current_page_label:
            page.render()
            return
    raise KeyError(f"Unknown page label: {current_page_label}")

