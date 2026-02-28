"""
Structured field extraction.
Pulls dates, amounts, emails, phones from raw OCR text using regex.
"""

import json
import re
from dataclasses import dataclass

from localarchive.config import ExtractionConfig


@dataclass
class ExtractedField:
    field_type: str  # "date", "amount", "email", "phone", "entity"
    value: str
    raw_match: str
    start: int
    end: int


DATE_PATTERNS = [
    r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b",
    r"\b(\d{4}[/-]\d{1,2}[/-]\d{1,2})\b",
    r"\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4})\b",
    r"\b(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{4})\b",
]

AMOUNT_PATTERNS = [
    r"(\$[\d,]+\.?\d{0,2})",
    r"(\u20ac[\d,]+\.?\d{0,2})",
    r"(\u00a3[\d,]+\.?\d{0,2})",
    r"\b(\d{1,3}(?:,\d{3})*\.\d{2})\b",
]

EMAIL_PATTERN = r"\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b"
PHONE_PATTERN = r"\b(\+?\d?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})\b"
DOI_PATTERN = r"\b(10\.\d{4,9}/[-._;()/:A-Z0-9]+)\b"
ARXIV_PATTERN = r"\b(arXiv:\s*\d{4}\.\d{4,5}(?:v\d+)?)\b"
YEAR_PATTERN = r"\b((?:19|20)\d{2})\b"


def _extract_fields_regex(text: str) -> list[ExtractedField]:
    """Extract structured fields from raw text using regex."""
    fields = []

    for pattern in DATE_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            fields.append(
                ExtractedField("date", match.group(1), match.group(0), match.start(), match.end())
            )

    for pattern in AMOUNT_PATTERNS:
        for match in re.finditer(pattern, text):
            fields.append(
                ExtractedField("amount", match.group(1), match.group(0), match.start(), match.end())
            )

    for match in re.finditer(EMAIL_PATTERN, text):
        fields.append(
            ExtractedField("email", match.group(1), match.group(0), match.start(), match.end())
        )

    for match in re.finditer(PHONE_PATTERN, text):
        fields.append(
            ExtractedField("phone", match.group(1), match.group(0), match.start(), match.end())
        )

    for match in re.finditer(DOI_PATTERN, text, re.IGNORECASE):
        fields.append(
            ExtractedField("doi", match.group(1), match.group(0), match.start(), match.end())
        )

    for match in re.finditer(ARXIV_PATTERN, text, re.IGNORECASE):
        fields.append(
            ExtractedField("arxiv", match.group(1), match.group(0), match.start(), match.end())
        )

    for match in re.finditer(YEAR_PATTERN, text):
        fields.append(
            ExtractedField("year", match.group(1), match.group(0), match.start(), match.end())
        )

    # Deduplicate
    seen = set()
    unique = []
    for f in fields:
        key = (f.field_type, f.value)
        if key not in seen:
            seen.add(key)
            unique.append(f)
    return unique


def extract_fields_with_spacy(text: str) -> list[ExtractedField]:
    """Extract named entities using spaCy (optional)."""
    try:
        import spacy

        nlp = spacy.load("en_core_web_sm")
    except Exception:
        return []

    doc = nlp(text[:100000])
    fields = []
    for ent in doc.ents:
        if ent.label_ in ("PERSON", "ORG", "GPE", "DATE", "MONEY"):
            fields.append(
                ExtractedField(
                    f"entity_{ent.label_.lower()}", ent.text, ent.text, ent.start_char, ent.end_char
                )
            )
    return fields


def extract_fields_with_ollama(text: str, model: str = "mistral") -> list[ExtractedField]:
    """Extract fields using a local Ollama model. Returns [] when unavailable or invalid output."""
    try:
        import ollama
    except ImportError:
        return []

    prompt = (
        "Extract structured fields from the text below. "
        "Return ONLY JSON array items with keys: field_type, value, raw_match, start, end. "
        "Allowed field_type values: date, amount, email, phone, entity_person, entity_org, entity_gpe.\n\n"
        f"TEXT:\n{text[:8000]}"
    )
    try:
        response = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0},
        )
        content = response.get("message", {}).get("content", "[]").strip()
        payload = json.loads(content)
    except Exception:
        return []

    fields: list[ExtractedField] = []
    if not isinstance(payload, list):
        return fields
    for item in payload:
        if not isinstance(item, dict):
            continue
        field_type = str(item.get("field_type", "")).strip().lower()
        value = str(item.get("value", "")).strip()
        if not field_type or not value:
            continue
        raw_match = str(item.get("raw_match", value))
        try:
            start = int(item.get("start", 0))
            end = int(item.get("end", start + len(value)))
        except (TypeError, ValueError):
            start = 0
            end = len(value)
        fields.append(ExtractedField(field_type, value, raw_match, start, end))
    return fields


def _dedupe_fields(fields: list[ExtractedField]) -> list[ExtractedField]:
    seen = set()
    unique = []
    for field in fields:
        key = (field.field_type, field.value)
        if key in seen:
            continue
        seen.add(key)
        unique.append(field)
    return unique


def extract_fields(
    text: str,
    mode: str = "regex",
    config: ExtractionConfig | None = None,
) -> list[ExtractedField]:
    """Extract fields using one of: regex, spacy, ollama, hybrid."""
    mode = (mode or "regex").lower()
    cfg = config or ExtractionConfig()

    regex_fields = _extract_fields_regex(text)
    if mode == "regex":
        return regex_fields
    if mode == "spacy":
        return _dedupe_fields(extract_fields_with_spacy(text))
    if mode == "ollama":
        fields = extract_fields_with_ollama(text, model=cfg.ollama_model)
        return _dedupe_fields(fields or regex_fields)
    if mode == "hybrid":
        fields = regex_fields + extract_fields_with_spacy(text)
        if cfg.use_local_llm:
            fields += extract_fields_with_ollama(text, model=cfg.ollama_model)
        return _dedupe_fields(fields)

    return regex_fields
