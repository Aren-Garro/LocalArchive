"""SQLite database manager for LocalArchive."""

import sqlite3
from pathlib import Path
from localarchive.db.models import SCHEMA_SQL


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
        self.conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def insert_document(self, **kwargs) -> int:
        cols = ", ".join(kwargs.keys())
        placeholders = ", ".join(["?"] * len(kwargs))
        cur = self.conn.execute(
            f"INSERT INTO documents ({cols}) VALUES ({placeholders})",
            list(kwargs.values()),
        )
        self.conn.commit()
        return cur.lastrowid

    def update_document(self, doc_id: int, **kwargs) -> None:
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

    def list_documents(self, status: str | None = None, limit: int = 100) -> list[dict]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM documents WHERE status = ? ORDER BY ingested_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM documents ORDER BY ingested_at DESC LIMIT ?", (limit,),
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

    def get_fields(self, doc_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM extracted_fields WHERE document_id = ?", (doc_id,)
        ).fetchall()
        return [dict(r) for r in rows]
