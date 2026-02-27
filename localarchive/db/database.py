"""SQLite database manager for LocalArchive."""

import sqlite3
from pathlib import Path
from localarchive.db.models import SCHEMA_SQL
from localarchive.utils import timestamp_now

LATEST_SCHEMA_VERSION = 2


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def initialize(self) -> None:
        self.conn.executescript(SCHEMA_SQL)
        self.conn.execute("INSERT INTO schema_version(version) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM schema_version)")
        self._apply_migrations()
        self.conn.commit()

    def _get_schema_version(self) -> int:
        row = self.conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
        return int(row["version"]) if row else 0

    def _set_schema_version(self, version: int) -> None:
        self.conn.execute("UPDATE schema_version SET version = ?", (version,))

    def _has_column(self, table: str, column: str) -> bool:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r["name"] == column for r in rows)

    def _apply_migrations(self) -> None:
        version = self._get_schema_version()
        if version < 1:
            if not self._has_column("documents", "updated_at"):
                self.conn.execute("ALTER TABLE documents ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
            if not self._has_column("documents", "error_message"):
                self.conn.execute("ALTER TABLE documents ADD COLUMN error_message TEXT DEFAULT ''")
            if not self._has_column("documents", "last_processed_at"):
                self.conn.execute("ALTER TABLE documents ADD COLUMN last_processed_at TEXT DEFAULT ''")
            self.conn.execute("UPDATE documents SET updated_at = ingested_at WHERE updated_at = ''")
            self._set_schema_version(1)

        if version < 2:
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_extracted_fields_dedupe "
                "ON extracted_fields(document_id, field_type, value, position)"
            )
            self._set_schema_version(2)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def insert_document(self, **kwargs) -> int:
        now = timestamp_now()
        kwargs.setdefault("updated_at", now)
        kwargs.setdefault("error_message", "")
        kwargs.setdefault("last_processed_at", "")
        cols = ", ".join(kwargs.keys())
        placeholders = ", ".join(["?"] * len(kwargs))
        cur = self.conn.execute(
            f"INSERT INTO documents ({cols}) VALUES ({placeholders})",
            list(kwargs.values()),
        )
        self.conn.commit()
        return cur.lastrowid

    def update_document(self, doc_id: int, **kwargs) -> None:
        kwargs.setdefault("updated_at", timestamp_now())
        set_clause = ", ".join(f"{k} = ?" for k in kwargs)
        self.conn.execute(
            f"UPDATE documents SET {set_clause} WHERE id = ?",
            [*kwargs.values(), doc_id],
        )
        self.conn.commit()

    def get_document(self, doc_id: int) -> dict | None:
        row = self.conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
        return dict(row) if row else None

    def get_document_detail(self, doc_id: int) -> dict | None:
        doc = self.get_document(doc_id)
        if not doc:
            return None
        doc["tags"] = self.get_tags(doc_id)
        doc["fields"] = self.get_fields(doc_id)
        return doc

    def list_documents(self, status: str | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM documents WHERE status = ? ORDER BY ingested_at DESC LIMIT ? OFFSET ?",
                (status, limit, offset),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM documents ORDER BY ingested_at DESC LIMIT ? OFFSET ?", (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_documents_for_reprocess(self, status: str, since: str | None = None, limit: int = 100) -> list[dict]:
        if since:
            rows = self.conn.execute(
                "SELECT * FROM documents WHERE status = ? AND COALESCE(last_processed_at, ingested_at) >= ? "
                "ORDER BY updated_at DESC LIMIT ?",
                (status, since, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM documents WHERE status = ? ORDER BY updated_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def document_exists_by_hash(self, file_hash: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM documents WHERE file_hash = ?", (file_hash,)).fetchone()
        return row is not None

    def add_tag(self, doc_id: int, tag_name: str) -> None:
        self.conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag_name,))
        tag_row = self.conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
        self.conn.execute(
            "INSERT OR IGNORE INTO document_tags (document_id, tag_id) VALUES (?, ?)",
            (doc_id, tag_row["id"]),
        )
        self.conn.commit()

    def set_tags(self, doc_id: int, tags: list[str]) -> None:
        normalized = sorted({t.strip().lower() for t in tags if t and t.strip()})
        self.conn.execute("DELETE FROM document_tags WHERE document_id = ?", (doc_id,))
        for tag_name in normalized:
            self.conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag_name,))
            tag_row = self.conn.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()
            self.conn.execute(
                "INSERT OR IGNORE INTO document_tags (document_id, tag_id) VALUES (?, ?)",
                (doc_id, tag_row["id"]),
            )
        self.conn.commit()

    def get_tags(self, doc_id: int) -> list[str]:
        rows = self.conn.execute(
            "SELECT t.name FROM tags t JOIN document_tags dt ON dt.tag_id = t.id WHERE dt.document_id = ? ORDER BY t.name",
            (doc_id,),
        ).fetchall()
        return [r["name"] for r in rows]

    def insert_fields(self, doc_id: int, fields: list[dict]) -> None:
        for f in fields:
            self.conn.execute(
                "INSERT INTO extracted_fields (document_id, field_type, value, raw_match, position) VALUES (?, ?, ?, ?, ?)",
                (doc_id, f["field_type"], f["value"], f.get("raw_match", ""), f.get("start", 0)),
            )
        self.conn.commit()

    def replace_fields(self, doc_id: int, fields: list[dict]) -> None:
        self.conn.execute("DELETE FROM extracted_fields WHERE document_id = ?", (doc_id,))
        for f in fields:
            self.conn.execute(
                "INSERT OR IGNORE INTO extracted_fields (document_id, field_type, value, raw_match, position) "
                "VALUES (?, ?, ?, ?, ?)",
                (doc_id, f["field_type"], f["value"], f.get("raw_match", ""), f.get("start", 0)),
            )

    def mark_for_reprocess(self, doc_ids: list[int]) -> int:
        if not doc_ids:
            return 0
        placeholders = ", ".join("?" for _ in doc_ids)
        params = [timestamp_now(), *doc_ids]
        cur = self.conn.execute(
            f"UPDATE documents SET status = 'pending_ocr', error_message = '', updated_at = ? "
            f"WHERE id IN ({placeholders})",
            params,
        )
        self.conn.commit()
        return cur.rowcount

    def update_processed_document(self, doc_id: int, ocr_text: str, fields: list[dict]) -> None:
        now = timestamp_now()
        self.conn.execute("BEGIN")
        try:
            self.conn.execute(
                "UPDATE documents SET ocr_text = ?, status = 'processed', error_message = '', "
                "last_processed_at = ?, updated_at = ? WHERE id = ?",
                (ocr_text, now, now, doc_id),
            )
            self.replace_fields(doc_id, fields)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def get_fields(self, doc_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM extracted_fields WHERE document_id = ?", (doc_id,)
        ).fetchall()
        return [dict(r) for r in rows]
