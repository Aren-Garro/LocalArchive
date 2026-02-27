"""Full-text search engine using SQLite FTS5."""

from localarchive.db.database import Database


class SearchEngine:
    def __init__(self, db: Database):
        self.db = db

    def search(self, query: str, limit: int = 20, offset: int = 0,
               tag: str | None = None, file_type: str | None = None, status: str | None = None) -> list[dict]:
        base_query = """
            SELECT d.*, rank FROM documents_fts fts
            JOIN documents d ON d.id = fts.rowid
            WHERE documents_fts MATCH ?
        """
        params: list = [query]
        if tag:
            base_query += " AND d.id IN (SELECT dt.document_id FROM document_tags dt JOIN tags t ON t.id = dt.tag_id WHERE t.name = ?)"
            params.append(tag)
        if file_type:
            base_query += " AND d.file_type = ?"
            params.append(file_type)
        if status:
            base_query += " AND d.status = ?"
            params.append(status)
        base_query += " ORDER BY rank LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        rows = self.db.conn.execute(base_query, params).fetchall()
        return [dict(r) for r in rows]

    def count(self, query: str, status: str | None = None) -> int:
        if status:
            row = self.db.conn.execute(
                "SELECT COUNT(*) as cnt FROM documents_fts fts JOIN documents d ON d.id = fts.rowid "
                "WHERE documents_fts MATCH ? AND d.status = ?",
                (query, status),
            ).fetchone()
            return row["cnt"] if row else 0
        row = self.db.conn.execute(
            "SELECT COUNT(*) as cnt FROM documents_fts WHERE documents_fts MATCH ?", (query,)
        ).fetchone()
        return row["cnt"] if row else 0

    def recent(self, limit: int = 10, offset: int = 0, status: str | None = None) -> list[dict]:
        return self.db.list_documents(limit=limit, offset=offset, status=status)

    def by_tag(self, tag_name: str, limit: int = 50) -> list[dict]:
        rows = self.db.conn.execute(
            "SELECT d.* FROM documents d JOIN document_tags dt ON dt.document_id = d.id JOIN tags t ON t.id = dt.tag_id WHERE t.name = ? ORDER BY d.ingested_at DESC LIMIT ?",
            (tag_name, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def by_field(self, field_type: str, value: str | None = None, limit: int = 50) -> list[dict]:
        if value:
            rows = self.db.conn.execute(
                "SELECT DISTINCT d.* FROM documents d JOIN extracted_fields ef ON ef.document_id = d.id WHERE ef.field_type = ? AND ef.value LIKE ? ORDER BY d.ingested_at DESC LIMIT ?",
                (field_type, f"%{value}%", limit),
            ).fetchall()
        else:
            rows = self.db.conn.execute(
                "SELECT DISTINCT d.* FROM documents d JOIN extracted_fields ef ON ef.document_id = d.id WHERE ef.field_type = ? ORDER BY d.ingested_at DESC LIMIT ?",
                (field_type, limit),
            ).fetchall()
        return [dict(r) for r in rows]
