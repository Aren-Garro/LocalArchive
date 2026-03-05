"""PII redaction helpers for safe sharing."""

from __future__ import annotations

import re


_PATTERNS = (
    ("email", re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")),
    ("phone", re.compile(r"\b(?:\+?\d{1,2}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}\b")),
    ("ssn", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("credit_card", re.compile(r"\b(?:\d[ -]*?){13,19}\b")),
)


def redact_text(text: str) -> tuple[str, dict[str, int]]:
    out = str(text)
    counts: dict[str, int] = {}
    for label, pattern in _PATTERNS:
        replacement = f"[REDACTED_{label.upper()}]"
        out, n = pattern.subn(replacement, out)
        counts[label] = int(n)
    return out, counts
