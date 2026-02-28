"""SQLite database manager for LocalArchive."""

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

from localarchive.db.models import SCHEMA_SQL
from localarchive.utils import file_hash, timestamp_now

LATEST_SCHEMA_VERSION = 7


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
        self.conn.execute(
            "INSERT INTO schema_version(version) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM schema_version)"
        )
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
                self.conn.execute(
                    "ALTER TABLE documents ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''"
                )
            if not self._has_column("documents", "error_message"):
                self.conn.execute("ALTER TABLE documents ADD COLUMN error_message TEXT DEFAULT ''")
            if not self._has_column("documents", "last_processed_at"):
                self.conn.execute(
                    "ALTER TABLE documents ADD COLUMN last_processed_at TEXT DEFAULT ''"
                )
            self.conn.execute("UPDATE documents SET updated_at = ingested_at WHERE updated_at = ''")
            self._set_schema_version(1)

        if version < 2:
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_extracted_fields_dedupe "
                "ON extracted_fields(document_id, field_type, value, position)"
            )
            self._set_schema_version(2)

        if version < 3:
            self.conn.executescript(
                """
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
                    errors        INTEGER NOT NULL DEFAULT 0
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
                CREATE INDEX IF NOT EXISTS idx_document_collections_collection
                    ON document_collections(collection_id);
                """
            )
            self._set_schema_version(3)

        if version < 4:
            if not self._has_column("documents", "processing_attempts"):
                self.conn.execute(
                    "ALTER TABLE documents ADD COLUMN processing_attempts INTEGER NOT NULL DEFAULT 0"
                )
            if not self._has_column("documents", "last_error_at"):
                self.conn.execute("ALTER TABLE documents ADD COLUMN last_error_at TEXT DEFAULT ''")
            self.conn.execute(
                "UPDATE documents SET processing_attempts = COALESCE(processing_attempts, 0)"
            )
            self.conn.execute("UPDATE documents SET last_error_at = '' WHERE last_error_at IS NULL")
            self._set_schema_version(4)

        if version < 5:
            if not self._has_column("processing_runs", "checkpoint_doc_id"):
                self.conn.execute(
                    "ALTER TABLE processing_runs ADD COLUMN checkpoint_doc_id INTEGER NOT NULL DEFAULT 0"
                )
            if not self._has_column("processing_runs", "aborted_reason"):
                self.conn.execute(
                    "ALTER TABLE processing_runs ADD COLUMN aborted_reason TEXT DEFAULT ''"
                )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backups (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at          TEXT NOT NULL,
                    path                TEXT NOT NULL UNIQUE,
                    db_hash             TEXT DEFAULT '',
                    archive_file_count  INTEGER NOT NULL DEFAULT 0,
                    verified            INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            self._set_schema_version(5)

        if version < 6:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS extracted_tables (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id   INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                    table_index   INTEGER NOT NULL DEFAULT 0,
                    schema_json   TEXT NOT NULL DEFAULT '[]',
                    rows_json     TEXT NOT NULL DEFAULT '[]',
                    created_at    TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_extracted_tables_doc ON extracted_tables(document_id)"
            )
            self._set_schema_version(6)

        if version < 7:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS document_similarity (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_id_a      INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                    doc_id_b      INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
                    score         REAL NOT NULL,
                    model         TEXT NOT NULL DEFAULT 'token-jaccard',
                    updated_at    TEXT NOT NULL,
                    UNIQUE(doc_id_a, doc_id_b)
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_document_similarity_a ON document_similarity(doc_id_a)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_document_similarity_b ON document_similarity(doc_id_b)"
            )
            self._set_schema_version(7)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def insert_document(self, **kwargs) -> int:
        now = timestamp_now()
        kwargs.setdefault("updated_at", now)
        kwargs.setdefault("error_message", "")
        kwargs.setdefault("last_processed_at", "")
        kwargs.setdefault("processing_attempts", 0)
        kwargs.setdefault("last_error_at", "")
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
        doc["tables"] = self.get_tables(doc_id)
        return doc

    def list_documents(
        self, status: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM documents WHERE status = ? ORDER BY ingested_at DESC LIMIT ? OFFSET ?",
                (status, limit, offset),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM documents ORDER BY ingested_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]

    def iter_documents(self, batch_size: int = 1000, status: str | None = None):
        offset = 0
        while True:
            batch = self.list_documents(status=status, limit=batch_size, offset=offset)
            if not batch:
                break
            yield from batch
            offset += len(batch)

    def list_documents_for_reprocess(
        self, status: str, since: str | None = None, limit: int = 100
    ) -> list[dict]:
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

    def list_documents_for_processing(self, limit: int = 100, after_doc_id: int = 0) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM documents WHERE status = 'pending_ocr' AND id > ? ORDER BY id ASC LIMIT ?",
            (after_doc_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def document_exists_by_hash(self, file_hash: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM documents WHERE file_hash = ?", (file_hash,)
        ).fetchone()
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
            tag_row = self.conn.execute(
                "SELECT id FROM tags WHERE name = ?", (tag_name,)
            ).fetchone()
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
            f"UPDATE documents SET status = 'pending_ocr', error_message = '', processing_attempts = 0, "
            f"last_error_at = '', updated_at = ? "
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
                "last_processed_at = ?, updated_at = ?, processing_attempts = 0, last_error_at = '' WHERE id = ?",
                (ocr_text, now, now, doc_id),
            )
            self.replace_fields(doc_id, fields)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def update_processed_documents_batch(self, items: list[dict]) -> None:
        if not items:
            return
        now = timestamp_now()
        self.conn.execute("BEGIN")
        try:
            for item in items:
                doc_id = int(item["doc_id"])
                ocr_text = str(item.get("full_text", ""))
                fields = item.get("fields", [])
                self.conn.execute(
                    "UPDATE documents SET ocr_text = ?, status = 'processed', error_message = '', "
                    "last_processed_at = ?, updated_at = ?, processing_attempts = 0, last_error_at = '' WHERE id = ?",
                    (ocr_text, now, now, doc_id),
                )
                self.replace_fields(doc_id, fields)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def record_processing_error(self, doc_id: int, error_message: str, max_retries: int) -> dict:
        row = self.conn.execute(
            "SELECT processing_attempts FROM documents WHERE id = ?",
            (doc_id,),
        ).fetchone()
        previous_attempts = int(row["processing_attempts"]) if row else 0
        attempts = previous_attempts + 1
        terminal = max_retries >= 0 and attempts >= max_retries
        now = timestamp_now()
        message = str(error_message)
        if terminal:
            message = f"{message} (max_retries_exceeded)"
        self.conn.execute(
            "UPDATE documents SET status = 'error', error_message = ?, processing_attempts = ?, "
            "last_error_at = ?, updated_at = ? WHERE id = ?",
            (message, attempts, now, now, doc_id),
        )
        self.conn.commit()
        return {"attempts": attempts, "terminal": terminal, "max_retries": max_retries}

    def record_processing_errors_batch(self, items: list[dict], max_retries: int) -> list[dict]:
        if not items:
            return []
        states = []
        now = timestamp_now()
        self.conn.execute("BEGIN")
        try:
            for item in items:
                doc_id = int(item["doc_id"])
                row = self.conn.execute(
                    "SELECT processing_attempts FROM documents WHERE id = ?",
                    (doc_id,),
                ).fetchone()
                previous_attempts = int(row["processing_attempts"]) if row else 0
                attempts = previous_attempts + 1
                terminal = max_retries >= 0 and attempts >= max_retries
                message = str(item.get("error", ""))
                if terminal:
                    message = f"{message} (max_retries_exceeded)"
                self.conn.execute(
                    "UPDATE documents SET status = 'error', error_message = ?, processing_attempts = ?, "
                    "last_error_at = ?, updated_at = ? WHERE id = ?",
                    (message, attempts, now, now, doc_id),
                )
                states.append(
                    {
                        "doc_id": doc_id,
                        "attempts": attempts,
                        "terminal": terminal,
                        "max_retries": max_retries,
                        "message": message,
                    }
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return states

    def get_fields(self, doc_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM extracted_fields WHERE document_id = ?", (doc_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def set_tables(self, doc_id: int, tables: list[dict]) -> None:
        self.conn.execute("DELETE FROM extracted_tables WHERE document_id = ?", (doc_id,))
        for idx, table in enumerate(tables):
            headers = table.get("headers", [])
            rows = table.get("rows", [])
            self.conn.execute(
                "INSERT INTO extracted_tables(document_id, table_index, schema_json, rows_json, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    doc_id,
                    idx,
                    json.dumps(headers, ensure_ascii=False),
                    json.dumps(rows, ensure_ascii=False),
                    timestamp_now(),
                ),
            )
        self.conn.commit()

    def get_tables(self, doc_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT table_index, schema_json, rows_json FROM extracted_tables WHERE document_id = ? "
            "ORDER BY table_index ASC",
            (doc_id,),
        ).fetchall()
        out: list[dict] = []
        for row in rows:
            try:
                headers = json.loads(row["schema_json"] or "[]")
            except Exception:
                headers = []
            try:
                table_rows = json.loads(row["rows_json"] or "[]")
            except Exception:
                table_rows = []
            out.append({"table_index": int(row["table_index"]), "headers": headers, "rows": table_rows})
        return out

    def clear_similarity(self) -> None:
        self.conn.execute("DELETE FROM document_similarity")
        self.conn.commit()

    def upsert_similarity_edges(self, edges: list[dict]) -> None:
        now = timestamp_now()
        for edge in edges:
            a = int(edge["doc_id_a"])
            b = int(edge["doc_id_b"])
            score = float(edge["score"])
            if a == b:
                continue
            left, right = (a, b) if a < b else (b, a)
            self.conn.execute(
                "INSERT INTO document_similarity(doc_id_a, doc_id_b, score, model, updated_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(doc_id_a, doc_id_b) DO UPDATE SET "
                "score = excluded.score, model = excluded.model, updated_at = excluded.updated_at",
                (left, right, score, str(edge.get("model", "token-jaccard")), now),
            )
        self.conn.commit()

    def get_similar_documents(self, doc_id: int, limit: int = 10) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT
              CASE WHEN ds.doc_id_a = ? THEN ds.doc_id_b ELSE ds.doc_id_a END AS related_id,
              ds.score AS score,
              d.filename AS filename,
              d.file_type AS file_type,
              d.status AS status
            FROM document_similarity ds
            JOIN documents d ON d.id = CASE WHEN ds.doc_id_a = ? THEN ds.doc_id_b ELSE ds.doc_id_a END
            WHERE ds.doc_id_a = ? OR ds.doc_id_b = ?
            ORDER BY ds.score DESC, related_id ASC
            LIMIT ?
            """,
            (doc_id, doc_id, doc_id, doc_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def start_processing_run(self, engine: str, extractor: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO processing_runs(started_at, status, engine, extractor, checkpoint_doc_id, aborted_reason) "
            "VALUES (?, 'running', ?, ?, 0, '')",
            (timestamp_now(), engine, extractor),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_processing_run(self, run_id: int) -> dict | None:
        row = self.conn.execute("SELECT * FROM processing_runs WHERE id = ?", (run_id,)).fetchone()
        return dict(row) if row else None

    def latest_processing_run(self) -> dict | None:
        row = self.conn.execute("SELECT * FROM processing_runs ORDER BY id DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def update_processing_checkpoint(self, run_id: int, checkpoint_doc_id: int) -> None:
        self.conn.execute(
            "UPDATE processing_runs SET checkpoint_doc_id = ? WHERE id = ?",
            (checkpoint_doc_id, run_id),
        )
        self.conn.commit()

    def add_processing_event(
        self, run_id: int, event_type: str, message: str = "", document_id: int | None = None
    ) -> None:
        self.conn.execute(
            "INSERT INTO processing_events(run_id, document_id, event_type, message, created_at) VALUES (?, ?, ?, ?, ?)",
            (run_id, document_id, event_type, message, timestamp_now()),
        )
        self.conn.commit()

    def add_processing_events_batch(self, events: list[dict]) -> None:
        if not events:
            return
        now = timestamp_now()
        payload = [
            (
                int(event["run_id"]),
                event.get("document_id"),
                str(event["event_type"]),
                str(event.get("message", "")),
                now,
            )
            for event in events
        ]
        self.conn.executemany(
            "INSERT INTO processing_events(run_id, document_id, event_type, message, created_at) VALUES (?, ?, ?, ?, ?)",
            payload,
        )
        self.conn.commit()

    def finish_processing_run(
        self, run_id: int, status: str, processed: int, errors: int, aborted_reason: str = ""
    ) -> None:
        self.conn.execute(
            "UPDATE processing_runs SET ended_at = ?, status = ?, processed = ?, errors = ?, aborted_reason = ? "
            "WHERE id = ?",
            (timestamp_now(), status, processed, errors, aborted_reason, run_id),
        )
        self.conn.commit()

    def record_backup(
        self, path: str, db_hash: str, archive_file_count: int, verified: bool
    ) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO backups(created_at, path, db_hash, archive_file_count, verified) "
            "VALUES (?, ?, ?, ?, ?)",
            (timestamp_now(), path, db_hash, int(archive_file_count), 1 if verified else 0),
        )
        self.conn.commit()

    def list_backups(self, limit: int = 50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM backups ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_backup_record(self, path: str) -> None:
        self.conn.execute("DELETE FROM backups WHERE path = ?", (path,))
        self.conn.commit()

    def upsert_collection(self, name: str, description: str = "") -> int:
        now = timestamp_now()
        self.conn.execute(
            "INSERT INTO collections(name, description, created_at, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET description = excluded.description, updated_at = excluded.updated_at",
            (name, description, now, now),
        )
        row = self.conn.execute("SELECT id FROM collections WHERE name = ?", (name,)).fetchone()
        self.conn.commit()
        return int(row["id"])

    def set_collection_rule(self, collection_id: int, rule_type: str, rule_value: str) -> None:
        self.conn.execute(
            "DELETE FROM collection_rules WHERE collection_id = ? AND rule_type = ?",
            (collection_id, rule_type),
        )
        self.conn.execute(
            "INSERT INTO collection_rules(collection_id, rule_type, rule_value, created_at) VALUES (?, ?, ?, ?)",
            (collection_id, rule_type, rule_value, timestamp_now()),
        )
        self.conn.commit()

    def clear_collection_assignments(self, collection_id: int | None = None) -> None:
        if collection_id is None:
            self.conn.execute("DELETE FROM document_collections")
        else:
            self.conn.execute(
                "DELETE FROM document_collections WHERE collection_id = ?", (collection_id,)
            )
        self.conn.commit()

    def assign_document_to_collection(
        self, doc_id: int, collection_id: int, score: float = 1.0
    ) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO document_collections(document_id, collection_id, score, assigned_at) VALUES (?, ?, ?, ?)",
            (doc_id, collection_id, score, timestamp_now()),
        )

    def list_collections(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT c.*, COUNT(dc.document_id) as doc_count "
            "FROM collections c LEFT JOIN document_collections dc ON dc.collection_id = c.id "
            "GROUP BY c.id ORDER BY c.name"
        ).fetchall()
        return [dict(r) for r in rows]

    def auto_build_default_collections(self) -> dict:
        docs = list(self.iter_documents(batch_size=1000))
        research_id = self.upsert_collection("Research PDFs", "All ingested PDF research documents")
        review_id = self.upsert_collection("Needs Review", "Documents requiring manual review")
        by_year_prefix = "By Year:"
        self.clear_collection_assignments()
        year_collections: dict[str, int] = {}
        assignments = defaultdict(int)

        for doc in docs:
            if doc.get("file_type") == "pdf":
                self.assign_document_to_collection(doc["id"], research_id, score=0.9)
                assignments["Research PDFs"] += 1
            if doc.get("status") == "error":
                self.assign_document_to_collection(doc["id"], review_id, score=1.0)
                assignments["Needs Review"] += 1
            fields = self.get_fields(doc["id"])
            year = None
            for field in fields:
                value = str(field.get("value", ""))
                match = None
                for token in value.replace("/", "-").split("-"):
                    if len(token) == 4 and token.isdigit() and token.startswith(("19", "20")):
                        match = token
                        break
                if match:
                    year = match
                    break
            if year:
                name = f"{by_year_prefix} {year}"
                if name not in year_collections:
                    year_collections[name] = self.upsert_collection(
                        name, f"Documents associated with year {year}"
                    )
                self.assign_document_to_collection(doc["id"], year_collections[name], score=0.8)
                assignments[name] += 1

        self.conn.commit()
        return {"collections": len(self.list_collections()), "assignments": dict(assignments)}

    def timeline_rows(self, entity: str = "topic", limit: int = 200) -> list[dict]:
        entity_field = {
            "author": "entity_person",
            "topic": "entity_org",
            "journal": "entity_org",
        }.get(entity, "entity_org")
        rows = self.conn.execute(
            """
            SELECT d.id, d.filename, d.ingested_at, d.last_processed_at,
                   ef.value as entity_value
            FROM documents d
            LEFT JOIN extracted_fields ef ON ef.document_id = d.id AND ef.field_type = ?
            ORDER BY COALESCE(d.last_processed_at, d.ingested_at) DESC
            LIMIT ?
            """,
            (entity_field, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def audit_verify(self, repair: bool = False, full_check: bool = True) -> dict:
        issues = []
        checked = 0
        for doc in self.iter_documents(batch_size=1000):
            checked += 1
            fpath = Path(doc["filepath"])
            if not fpath.exists():
                issues.append({"id": doc["id"], "issue": "missing_file", "path": str(fpath)})
                continue
            if full_check:
                try:
                    actual_hash = file_hash(fpath)
                    if actual_hash != doc["file_hash"]:
                        issues.append(
                            {"id": doc["id"], "issue": "hash_mismatch", "path": str(fpath)}
                        )
                except Exception:
                    issues.append({"id": doc["id"], "issue": "hash_error", "path": str(fpath)})

        docs_count = self.conn.execute("SELECT COUNT(*) as c FROM documents").fetchone()["c"]
        fts_count = self.conn.execute("SELECT COUNT(*) as c FROM documents_fts").fetchone()["c"]
        if int(docs_count) != int(fts_count):
            issues.append(
                {"id": None, "issue": "fts_mismatch", "path": f"{docs_count} vs {fts_count}"}
            )
            if repair:
                self.conn.execute("INSERT INTO documents_fts(documents_fts) VALUES ('rebuild')")
                self.conn.commit()

        return {"checked": checked, "issues": issues, "full_check": full_check}
