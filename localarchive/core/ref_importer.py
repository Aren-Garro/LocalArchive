"""Simple BibTeX/RIS reference import parser."""

from __future__ import annotations

import re
from pathlib import Path


def parse_bibtex(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    entries: list[dict] = []
    blocks = [b for b in text.split("@") if b.strip()]
    for block in blocks:
        body = block.split("{", 1)[-1]
        fields: dict[str, str] = {}
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lower()
            value = value.strip().strip("{}").strip('"')
            fields[key] = value
        if fields:
            entries.append(fields)
    return entries


def parse_ris(path: Path) -> list[dict]:
    lines = path.read_text(encoding="utf-8").splitlines()
    entries: list[dict] = []
    current: dict[str, str] = {}
    tag_map = {
        "TI": "title",
        "T1": "title",
        "AU": "author",
        "PY": "year",
        "DO": "doi",
    }
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        match = re.match(r"^([A-Z0-9]{2})\s*-\s*(.*)$", line)
        if not match:
            continue
        tag = match.group(1)
        value = match.group(2).strip()
        if tag == "TY":
            current = {}
            continue
        if tag == "ER":
            if current:
                entries.append(current)
            current = {}
            continue
        key = tag_map.get(tag)
        if not key:
            continue
        if key == "author" and key in current:
            current[key] = f"{current[key]}; {value}"
        else:
            current[key] = value
    if current:
        entries.append(current)
    return entries
