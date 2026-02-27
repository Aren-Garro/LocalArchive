"""Data models / schema definitions for LocalArchive."""

from dataclasses import dataclass, field

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS documents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filename        TEXT NOT NULL,
    filepath        TEXT NOT NULL,
    file_hash       TEXT NOT NULL UNIQUE,
    file_type       TEXT NOT NULL,
    file_size       INTEGER NOT NULL,
    ingested_at     TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending_ocr',
    ocr_text        TEXT DEFAULT '',
    page_count      INTEGER DEFAULT 0,
    error_message   TEXT DEFAULT '',
    last_processed_at TEXT DEFAULT ''
);

CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    filename, ocr_text,
    content=documents, content_rowid=id,
    tokenize='porter unicode61'
);

CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
    INSERT INTO documents_fts(rowid, filename, ocr_text)
    VALUES (new.id, new.filename, new.ocr_text);
END;

CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, filename, ocr_text)
    VALUES ('delete', old.id, old.filename, old.ocr_text);
    INSERT INTO documents_fts(rowid, filename, ocr_text)
    VALUES (new.id, new.filename, new.ocr_text);
END;

CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, filename, ocr_text)
    VALUES ('delete', old.id, old.filename, old.ocr_text);
END;

CREATE TABLE IF NOT EXISTS tags (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS document_tags (
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    tag_id      INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (document_id, tag_id)
);

CREATE TABLE IF NOT EXISTS extracted_fields (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    field_type  TEXT NOT NULL,
    value       TEXT NOT NULL,
    raw_match   TEXT,
    position    INTEGER
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_documents_hash ON documents(file_hash);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_extracted_fields_doc ON extracted_fields(document_id);
CREATE INDEX IF NOT EXISTS idx_extracted_fields_type ON extracted_fields(field_type);
CREATE UNIQUE INDEX IF NOT EXISTS uq_extracted_fields_dedupe
    ON extracted_fields(document_id, field_type, value, position);
"""


@dataclass
class Document:
    id: int = 0
    filename: str = ""
    filepath: str = ""
    file_hash: str = ""
    file_type: str = ""
    file_size: int = 0
    ingested_at: str = ""
    updated_at: str = ""
    status: str = "pending_ocr"
    ocr_text: str = ""
    page_count: int = 0
    error_message: str = ""
    last_processed_at: str = ""
    tags: list[str] = field(default_factory=list)


@dataclass
class Tag:
    id: int = 0
    name: str = ""


@dataclass
class ExtractedFieldRecord:
    id: int = 0
    document_id: int = 0
    field_type: str = ""
    value: str = ""
    raw_match: str = ""
    position: int = 0
