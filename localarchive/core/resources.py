"""Educational resources registry."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LearningResource:
    resource_id: str
    title: str
    path: str


RESOURCES: tuple[LearningResource, ...] = (
    LearningResource("getting-started", "Getting Started with LocalArchive", "docs/education/getting-started.md"),
    LearningResource("research-workflow", "Research Workflow Guide", "docs/education/research-workflow.md"),
    LearningResource("medical-records", "Medical Records Organization Guide", "docs/education/medical-records.md"),
)


def list_resources() -> list[LearningResource]:
    return list(RESOURCES)


def get_resource(resource_id: str) -> LearningResource | None:
    key = resource_id.strip().lower()
    for item in RESOURCES:
        if item.resource_id == key:
            return item
    return None


def read_resource_text(resource: LearningResource) -> str:
    root = Path(__file__).resolve().parents[2]
    path = root / resource.path
    return path.read_text(encoding="utf-8")
