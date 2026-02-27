"""Tests for localarchive.db.database"""
import tempfile
from pathlib import Path
from localarchive.db.database import Database
from localarchive.db.search import SearchEngine


def _get_test_db():
    tmp = tempfile.mktemp(suffix=".db")
    db = Database(Path(tmp))
    db.initialize()
    return db


def test_insert_and_get_document():
    db = _get_test_db()
    doc_id = db.insert_document(
        filename="test.pdf", filepath="/tmp/test.pdf", file_hash="abc123",
        file_type="pdf", file_size=1024, ingested_at="2026-01-01T00:00:00Z", status="pending_ocr",
    )
    assert doc_id > 0
    doc = db.get_document(doc_id)
    assert doc is not None
    assert doc["filename"] == "test.pdf"
    assert doc["file_hash"] == "abc123"
    db.close()


def test_duplicate_hash():
    db = _get_test_db()
    db.insert_document(
        filename="a.pdf", filepath="/tmp/a.pdf", file_hash="dup123",
        file_type="pdf", file_size=100, ingested_at="2026-01-01T00:00:00Z", status="pending_ocr",
    )
    assert db.document_exists_by_hash("dup123") is True
    assert db.document_exists_by_hash("nonexistent") is False
    db.close()


def test_tags():
    db = _get_test_db()
    doc_id = db.insert_document(
        filename="tagged.pdf", filepath="/tmp/tagged.pdf", file_hash="tag123",
        file_type="pdf", file_size=200, ingested_at="2026-01-01T00:00:00Z", status="processed",
    )
    db.add_tag(doc_id, "medical")
    db.add_tag(doc_id, "2024")
    tags = db.get_tags(doc_id)
    assert "medical" in tags
    assert "2024" in tags
    db.close()


def test_search_fts():
    db = _get_test_db()
    db.insert_document(
        filename="invoice.pdf", filepath="/tmp/invoice.pdf", file_hash="fts123",
        file_type="pdf", file_size=500, ingested_at="2026-01-01T00:00:00Z",
        status="processed", ocr_text="Payment received from Acme Corp for consulting services",
    )
    rows = db.conn.execute(
        "SELECT * FROM documents_fts WHERE documents_fts MATCH ?", ("Acme",)
    ).fetchall()
    assert len(rows) > 0
    db.close()


def test_search_by_tag_and_field():
    db = _get_test_db()
    doc_id = db.insert_document(
        filename="receipt.pdf", filepath="/tmp/receipt.pdf", file_hash="search123",
        file_type="pdf", file_size=321, ingested_at="2026-01-01T00:00:00Z",
        status="processed", ocr_text="Receipt from Clinic total $99.99",
    )
    db.add_tag(doc_id, "medical")
    db.insert_fields(
        doc_id,
        [{"field_type": "amount", "value": "$99.99", "raw_match": "$99.99", "start": 10}],
    )

    search = SearchEngine(db)
    by_tag = search.by_tag("medical")
    by_field = search.by_field("amount", "99.99")
    assert any(d["id"] == doc_id for d in by_tag)
    assert any(d["id"] == doc_id for d in by_field)
    db.close()


def test_collections_and_audit():
    db = _get_test_db()
    doc_id = db.insert_document(
        filename="paper.pdf",
        filepath="/tmp/paper.pdf",
        file_hash="paperhash",
        file_type="pdf",
        file_size=111,
        ingested_at="2026-01-01T00:00:00Z",
        status="error",
    )
    db.insert_fields(
        doc_id,
        [{"field_type": "year", "value": "2024", "raw_match": "2024", "start": 0}],
    )
    summary = db.auto_build_default_collections()
    cols = db.list_collections()
    assert summary["collections"] >= 2
    assert any(c["name"] == "Research PDFs" for c in cols)
    assert any(c["name"] == "Needs Review" for c in cols)

    audit = db.audit_verify()
    assert isinstance(audit["issues"], list)
    db.close()
