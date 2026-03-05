"""Citation extraction helpers."""

from __future__ import annotations

import re


DOI_PATTERN = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)
ARXIV_PATTERN = re.compile(r"\barXiv:\s*\d{4}\.\d{4,5}(?:v\d+)?\b", re.IGNORECASE)


def _norm_arxiv(value: str) -> str:
    return re.sub(r"\s+", "", value.strip())


def collect_citations(doc: dict, fields: list[dict]) -> list[dict]:
    filename = str(doc.get("filename", ""))
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []

    for field in fields:
        ftype = str(field.get("field_type", "")).lower()
        raw_val = str(field.get("value", "")).strip()
        if not raw_val:
            continue
        if ftype == "doi":
            val = raw_val.lower()
            key = ("doi", val)
            if key not in seen:
                seen.add(key)
                out.append({"type": "doi", "value": val, "source": filename})
        elif ftype == "arxiv":
            val = _norm_arxiv(raw_val)
            key = ("arxiv", val.lower())
            if key not in seen:
                seen.add(key)
                out.append({"type": "arxiv", "value": val, "source": filename})

    text = str(doc.get("ocr_text", "") or "")
    for match in DOI_PATTERN.finditer(text):
        val = match.group(0).strip().lower()
        key = ("doi", val)
        if key in seen:
            continue
        seen.add(key)
        out.append({"type": "doi", "value": val, "source": filename})
    for match in ARXIV_PATTERN.finditer(text):
        val = _norm_arxiv(match.group(0))
        key = ("arxiv", val.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append({"type": "arxiv", "value": val, "source": filename})
    return out
