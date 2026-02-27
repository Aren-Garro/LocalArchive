"""
Structured field extraction.
Pulls dates, amounts, emails, phones from raw OCR text using regex.
"""

import re
from dataclasses import dataclass


@dataclass
class ExtractedField:
    field_type: str   # "date", "amount", "email", "phone", "entity"
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


def extract_fields(text: str) -> list[ExtractedField]:
    """Extract structured fields from raw text using regex."""
    fields = []

    for pattern in DATE_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            fields.append(ExtractedField("date", match.group(1), match.group(0), match.start(), match.end()))

    for pattern in AMOUNT_PATTERNS:
        for match in re.finditer(pattern, text):
            fields.append(ExtractedField("amount", match.group(1), match.group(0), match.start(), match.end()))

    for match in re.finditer(EMAIL_PATTERN, text):
        fields.append(ExtractedField("email", match.group(1), match.group(0), match.start(), match.end()))

    for match in re.finditer(PHONE_PATTERN, text):
        fields.append(ExtractedField("phone", match.group(1), match.group(0), match.start(), match.end()))

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
    except (ImportError, OSError):
        return []

    doc = nlp(text[:100000])
    fields = []
    for ent in doc.ents:
        if ent.label_ in ("PERSON", "ORG", "GPE", "DATE", "MONEY"):
            fields.append(ExtractedField(
                f"entity_{ent.label_.lower()}", ent.text, ent.text, ent.start_char, ent.end_char
            ))
    return fields
