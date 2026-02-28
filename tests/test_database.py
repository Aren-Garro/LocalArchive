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


def test_iter_documents_not_capped():
    db = _get_test_db()
    for i in range(5):
        db.insert_document(
            filename=f"doc{i}.pdf",
            filepath=f"/tmp/doc{i}.pdf",
            file_hash=f"h{i}",
            file_type="pdf",
            file_size=1,
            ingested_at=f"2026-01-0{i+1}T00:00:00Z",
            status="processed",
        )
    docs = list(db.iter_documents(batch_size=2))
    db.close()
    assert len(docs) == 5


def test_hybrid_search_returns_scores():
    db = _get_test_db()
    db.insert_document(
        filename="alpha.pdf",
        filepath="/tmp/alpha.pdf",
        file_hash="hybrid1",
        file_type="pdf",
        file_size=1,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="Graph neural networks and chemistry",
    )
    db.insert_document(
        filename="beta.pdf",
        filepath="/tmp/beta.pdf",
        file_hash="hybrid2",
        file_type="pdf",
        file_size=1,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="Completely unrelated content",
    )
    search = SearchEngine(db)
    results = search.search_hybrid("graph neural", limit=5, bm25_weight=0.5, vector_weight=0.5)
    db.close()
    assert results
    assert "hybrid_score" in results[0]


def test_record_processing_error_and_retry_terminal():
    db = _get_test_db()
    doc_id = db.insert_document(
        filename="broken.pdf",
        filepath="/tmp/broken.pdf",
        file_hash="retry1",
        file_type="pdf",
        file_size=1,
        ingested_at="2026-01-01T00:00:00Z",
        status="pending_ocr",
    )
    state = db.record_processing_error(doc_id, "ocr failure", max_retries=2)
    assert state["attempts"] == 1
    assert state["terminal"] is False

    state = db.record_processing_error(doc_id, "ocr failure", max_retries=2)
    assert state["attempts"] == 2
    assert state["terminal"] is True

    doc = db.get_document(doc_id)
    assert doc["processing_attempts"] == 2
    assert "max_retries_exceeded" in doc["error_message"]

    updated = db.mark_for_reprocess([doc_id])
    assert updated == 1
    doc = db.get_document(doc_id)
    assert doc["status"] == "pending_ocr"
    assert doc["processing_attempts"] == 0
    db.close()


def test_processing_run_checkpoint_and_backup_metadata():
    db = _get_test_db()
    run_id = db.start_processing_run(engine="paddleocr", extractor="regex")
    db.update_processing_checkpoint(run_id, checkpoint_doc_id=42)
    run = db.get_processing_run(run_id)
    assert run is not None
    assert run["checkpoint_doc_id"] == 42

    latest = db.latest_processing_run()
    assert latest is not None
    assert latest["id"] == run_id

    db.record_backup(path="/tmp/backup.zip", db_hash="abc", archive_file_count=3, verified=True)
    backups = db.list_backups(limit=10)
    assert backups
    assert backups[0]["path"] == "/tmp/backup.zip"
    db.finish_processing_run(run_id, status="aborted", processed=0, errors=1, aborted_reason="max_errors_exceeded:1")
    run = db.get_processing_run(run_id)
    assert run["aborted_reason"] == "max_errors_exceeded:1"
    db.close()
