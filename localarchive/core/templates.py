"""Built-in community template library for document tagging."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class CommunityTemplate:
    template_id: str
    title: str
    description: str
    tags: tuple[str, ...]
    filename_patterns: tuple[str, ...]
    text_patterns: tuple[str, ...]


BUILTIN_TEMPLATES: tuple[CommunityTemplate, ...] = (
    CommunityTemplate(
        template_id="w2_form",
        title="W-2 Tax Form",
        description="US wage and tax statement documents.",
        tags=("tax", "w2", "finance"),
        filename_patterns=(r"\bw-?2\b", r"\bwage\b"),
        text_patterns=(r"wage and tax statement", r"form\s+w-?2"),
    ),
    CommunityTemplate(
        template_id="insurance_claim",
        title="Insurance Claim",
        description="Insurance claim packets and claim summaries.",
        tags=("insurance", "claim"),
        filename_patterns=(r"\bclaim\b", r"\binsurance\b"),
        text_patterns=(r"claim number", r"policy number", r"insured"),
    ),
    CommunityTemplate(
        template_id="research_paper",
        title="Research Paper",
        description="Academic papers and manuscripts.",
        tags=("research", "academic"),
        filename_patterns=(r"\bpaper\b", r"\bmanuscript\b", r"\bjournal\b"),
        text_patterns=(r"\babstract\b", r"\breferences\b", r"\bdoi[:\s]"),
    ),
)


def list_templates() -> list[CommunityTemplate]:
    return list(BUILTIN_TEMPLATES)


def get_template(template_id: str) -> CommunityTemplate | None:
    key = template_id.strip().lower()
    for template in BUILTIN_TEMPLATES:
        if template.template_id == key:
            return template
    return None


def matches_template(template: CommunityTemplate, filename: str, text: str) -> bool:
    filename_l = (filename or "").lower()
    text_l = (text or "").lower()
    for pattern in template.filename_patterns:
        if re.search(pattern, filename_l):
            return True
    for pattern in template.text_patterns:
        if re.search(pattern, text_l):
            return True
    return False
