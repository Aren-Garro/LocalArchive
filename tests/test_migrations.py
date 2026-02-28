"""Tests for DB schema migration behavior."""

from pathlib import Path
import sqlite3
import uuid

from localarchive.db.database import Database


def _tmp_db_path() -> Path:
    root = Path.cwd() / ".test_tmp"
    root.mkdir(exist_ok=True)
    return root / f"migration-{uuid.uuid4().hex[:8]}.db"


def test_initialize_migrates_old_schema():
    db_path = _tmp_db_path()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            file_hash TEXT NOT NULL UNIQUE,
            file_type TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            ingested_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending_ocr',
            ocr_text TEXT DEFAULT '',
            page_count INTEGER DEFAULT 0
        );
        CREATE TABLE extracted_fields (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            field_type TEXT NOT NULL,
            value TEXT NOT NULL,
            raw_match TEXT,
            position INTEGER
        );
        CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
        CREATE TABLE document_tags (document_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, PRIMARY KEY (document_id, tag_id));
        CREATE VIRTUAL TABLE documents_fts USING fts5(filename, ocr_text, content=documents, content_rowid=id);
        """
    )
    conn.commit()
    conn.close()

    db = Database(db_path)
    db.initialize()
    cols = db.conn.execute("PRAGMA table_info(documents)").fetchall()
    col_names = {c["name"] for c in cols}
    db.close()

    assert "updated_at" in col_names
    assert "error_message" in col_names
    assert "last_processed_at" in col_names
    assert "processing_attempts" in col_names
    assert "last_error_at" in col_names
