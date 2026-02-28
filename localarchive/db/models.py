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
    last_processed_at TEXT DEFAULT '',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    last_error_at   TEXT DEFAULT ''
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

CREATE TABLE IF NOT EXISTS extracted_tables (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id   INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    table_index   INTEGER NOT NULL DEFAULT 0,
    schema_json   TEXT NOT NULL DEFAULT '[]',
    rows_json     TEXT NOT NULL DEFAULT '[]',
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS document_similarity (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id_a      INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    doc_id_b      INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    score         REAL NOT NULL,
    model         TEXT NOT NULL DEFAULT 'token-jaccard',
    updated_at    TEXT NOT NULL,
    UNIQUE(doc_id_a, doc_id_b)
);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS collections (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT DEFAULT '',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_rules (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    rule_type     TEXT NOT NULL,
    rule_value    TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS document_collections (
    document_id    INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    collection_id  INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    score          REAL NOT NULL DEFAULT 1.0,
    assigned_at    TEXT NOT NULL,
    PRIMARY KEY (document_id, collection_id)
);

CREATE TABLE IF NOT EXISTS processing_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    TEXT NOT NULL,
    ended_at      TEXT DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'running',
    engine        TEXT DEFAULT '',
    extractor     TEXT DEFAULT '',
    processed     INTEGER NOT NULL DEFAULT 0,
    errors        INTEGER NOT NULL DEFAULT 0,
    checkpoint_doc_id INTEGER NOT NULL DEFAULT 0,
    aborted_reason TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS processing_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        INTEGER NOT NULL REFERENCES processing_runs(id) ON DELETE CASCADE,
    document_id   INTEGER REFERENCES documents(id) ON DELETE SET NULL,
    event_type    TEXT NOT NULL,
    message       TEXT DEFAULT '',
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS document_embeddings (
    document_id    INTEGER PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    model          TEXT NOT NULL,
    vector_blob    BLOB NOT NULL,
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backups (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at          TEXT NOT NULL,
    path                TEXT NOT NULL UNIQUE,
    db_hash             TEXT DEFAULT '',
    archive_file_count  INTEGER NOT NULL DEFAULT 0,
    verified            INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_documents_hash ON documents(file_hash);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_extracted_fields_doc ON extracted_fields(document_id);
CREATE INDEX IF NOT EXISTS idx_extracted_fields_type ON extracted_fields(field_type);
CREATE INDEX IF NOT EXISTS idx_extracted_tables_doc ON extracted_tables(document_id);
CREATE INDEX IF NOT EXISTS idx_document_similarity_a ON document_similarity(doc_id_a);
CREATE INDEX IF NOT EXISTS idx_document_similarity_b ON document_similarity(doc_id_b);
CREATE INDEX IF NOT EXISTS idx_document_collections_collection ON document_collections(collection_id);
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
    processing_attempts: int = 0
    last_error_at: str = ""
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
