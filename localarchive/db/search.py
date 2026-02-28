"""Full-text search engine using SQLite FTS5."""

import re
from difflib import SequenceMatcher

from localarchive.db.database import Database


class SearchEngine:
    def __init__(self, db: Database):
        self.db = db

    def search(
        self,
        query: str,
        limit: int = 20,
        offset: int = 0,
        tag: str | None = None,
        file_type: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
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

    def search_hybrid(
        self,
        query: str,
        limit: int = 20,
        offset: int = 0,
        tag: str | None = None,
        file_type: str | None = None,
        status: str | None = None,
        bm25_weight: float = 0.7,
        vector_weight: float = 0.3,
    ) -> list[dict]:
        """
        Hybrid ranking without external vector dependencies.
        Combines normalized BM25 rank from FTS with semantic-proxy token overlap.
        """
        candidates = self.search(
            query,
            limit=max(limit * 5, 50),
            offset=offset,
            tag=tag,
            file_type=file_type,
            status=status,
        )
        if not candidates:
            return []
        tokens = [t for t in re.findall(r"[a-z0-9]+", query.lower()) if len(t) > 1]
        bm25_vals = [float(doc.get("rank", 0.0)) for doc in candidates]
        min_rank = min(bm25_vals)
        max_rank = max(bm25_vals)

        scored = []
        for doc in candidates:
            rank_val = float(doc.get("rank", 0.0))
            if max_rank == min_rank:
                bm25_norm = 1.0
            else:
                bm25_norm = 1.0 - ((rank_val - min_rank) / (max_rank - min_rank))
            haystack = f"{doc.get('filename', '')} {doc.get('ocr_text', '')}".lower()
            if not tokens:
                semantic_proxy = 0.0
            else:
                hit = sum(1 for t in set(tokens) if t in haystack)
                semantic_proxy = hit / len(set(tokens))
            score = (bm25_weight * bm25_norm) + (vector_weight * semantic_proxy)
            row = dict(doc)
            row["hybrid_score"] = round(score, 6)
            scored.append(row)

        scored.sort(key=lambda d: d["hybrid_score"], reverse=True)
        return scored[:limit]

    def _fuzzy_score(self, query: str, haystack: str) -> float:
        query_norm = " ".join(re.findall(r"[a-z0-9]+", query.lower()))
        hay_norm = " ".join(re.findall(r"[a-z0-9]+", haystack.lower()))
        if not query_norm or not hay_norm:
            return 0.0
        if query_norm in hay_norm:
            return 1.0
        query_tokens = query_norm.split()
        hay_tokens = hay_norm.split()
        if not query_tokens or not hay_tokens:
            return 0.0
        token_scores = []
        for q in query_tokens:
            best = max(SequenceMatcher(None, q, h).ratio() for h in hay_tokens)
            token_scores.append(best)
        return sum(token_scores) / len(token_scores)

    def search_fuzzy(
        self,
        query: str,
        limit: int = 20,
        tag: str | None = None,
        file_type: str | None = None,
        status: str | None = None,
        threshold: float = 0.78,
        max_candidates: int = 300,
    ) -> list[dict]:
        base_query = "SELECT d.* FROM documents d"
        where = []
        params: list = []
        if tag:
            where.append(
                "d.id IN (SELECT dt.document_id FROM document_tags dt JOIN tags t ON t.id = dt.tag_id WHERE t.name = ?)"
            )
            params.append(tag)
        if file_type:
            where.append("d.file_type = ?")
            params.append(file_type)
        if status:
            where.append("d.status = ?")
            params.append(status)
        if where:
            base_query += " WHERE " + " AND ".join(where)
        base_query += " ORDER BY d.ingested_at DESC LIMIT ?"
        params.append(max_candidates)
        rows = self.db.conn.execute(base_query, params).fetchall()

        scored = []
        for row in rows:
            doc = dict(row)
            haystack = f"{doc.get('filename', '')} {doc.get('ocr_text', '')}"
            score = self._fuzzy_score(query, haystack)
            if score >= threshold:
                doc["fuzzy_score"] = round(score, 6)
                scored.append(doc)
        scored.sort(key=lambda d: d["fuzzy_score"], reverse=True)
        return scored[:limit]

    def count(
        self,
        query: str,
        tag: str | None = None,
        file_type: str | None = None,
        status: str | None = None,
    ) -> int:
        base_query = (
            "SELECT COUNT(*) as cnt FROM documents_fts fts "
            "JOIN documents d ON d.id = fts.rowid "
            "WHERE documents_fts MATCH ?"
        )
        params: list = [query]
        if tag:
            base_query += (
                " AND d.id IN (SELECT dt.document_id FROM document_tags dt "
                "JOIN tags t ON t.id = dt.tag_id WHERE t.name = ?)"
            )
            params.append(tag)
        if file_type:
            base_query += " AND d.file_type = ?"
            params.append(file_type)
        if status:
            base_query += " AND d.status = ?"
            params.append(status)
        row = self.db.conn.execute(base_query, params).fetchone()
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
