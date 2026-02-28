"""Tests for lightweight table extraction."""

from localarchive.core.table_extractor import extract_tables_from_text


def test_extract_tables_from_pipe_delimited_text():
    text = "Name | Amount\nAlice | 42.00\nBob | 10.50\n"
    tables = extract_tables_from_text(text)
    assert len(tables) == 1
    assert tables[0]["headers"] == ["Name", "Amount"]
    assert tables[0]["rows"][0] == ["Alice", "42.00"]

