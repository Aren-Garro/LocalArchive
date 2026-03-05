"""Confidence scoring for extracted documents."""

from __future__ import annotations


def score_document_confidence(doc: dict, fields: list[dict]) -> tuple[float, str]:
    text = str(doc.get("ocr_text", "") or "")
    text_len = len(text.strip())
    unique_fields = {(str(f.get("field_type", "")), str(f.get("value", ""))) for f in fields}
    field_count = len([f for f in unique_fields if f[0]])

    score = 0.0
    reasons: list[str] = []
    if text_len >= 120:
        score += 0.4
    elif text_len >= 40:
        score += 0.25
        reasons.append("short_ocr_text")
    else:
        score += 0.1
        reasons.append("very_short_ocr_text")

    if field_count >= 4:
        score += 0.4
    elif field_count >= 2:
        score += 0.25
        reasons.append("few_extracted_fields")
    elif field_count >= 1:
        score += 0.15
        reasons.append("minimal_extracted_fields")
    else:
        reasons.append("no_extracted_fields")

    status = str(doc.get("status", "")).lower()
    if status == "processed":
        score += 0.2
    elif status == "error":
        reasons.append("processing_error")

    clamped = min(1.0, max(0.0, score))
    reason = ",".join(reasons) if reasons else "ok"
    return round(clamped, 3), reason
