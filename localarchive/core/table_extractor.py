"""Lightweight table extraction helpers for OCR/native text."""

from __future__ import annotations

import re


def _split_row(line: str) -> list[str]:
    if "|" in line:
        return [cell.strip() for cell in line.split("|") if cell.strip()]
    if "\t" in line:
        return [cell.strip() for cell in line.split("\t") if cell.strip()]
    # OCR fallback: 2+ spaces as rough column separator.
    parts = [cell.strip() for cell in re.split(r"\s{2,}", line) if cell.strip()]
    return parts


def extract_tables_from_text(text: str) -> list[dict]:
    """
    Extract simple tabular blocks from plain text.

    Heuristic:
    - Detect consecutive lines that can be split into >=2 columns.
    - Keep blocks where row widths are consistent.
    - First row is treated as header when widths align.
    """
    lines = [ln.strip() for ln in text.splitlines()]
    rows = [_split_row(line) for line in lines]
    tables: list[dict] = []
    block: list[list[str]] = []
    width = 0

    def flush_block() -> None:
        nonlocal block, width
        if len(block) >= 2 and width >= 2:
            headers = block[0]
            data_rows = block[1:]
            tables.append({"headers": headers, "rows": data_rows})
        block = []
        width = 0

    for row in rows:
        if len(row) >= 2:
            if not block:
                block = [row]
                width = len(row)
            elif len(row) == width:
                block.append(row)
            else:
                flush_block()
                block = [row]
                width = len(row)
        else:
            flush_block()
    flush_block()
    return tables

