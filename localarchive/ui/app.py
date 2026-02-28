"""FastAPI web UI for LocalArchive. Lightweight HTMX-based interface."""

from html import escape

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from localarchive.config import Config
from localarchive.db.database import Database
from localarchive.db.search import SearchEngine

app = FastAPI(title="LocalArchive", docs_url=None, redoc_url=None)
config: Config = None
_db: Database = None
search_engine: SearchEngine = None


def create_app(cfg: Config) -> FastAPI:
    global config, _db, search_engine
    config = cfg
    _db = Database(cfg.db_path)
    _db.initialize()
    search_engine = SearchEngine(_db)
    return app


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    q: str = "",
    tag: str = "",
    file_type: str = "",
    status: str = "",
    limit: int | None = None,
    offset: int = 0,
):
    results = []
    page_limit = limit or config.ui.default_limit
    page_limit = max(1, min(page_limit, 200))
    offset = max(0, offset)

    if q:
        results = search_engine.search(
            q,
            limit=page_limit,
            offset=offset,
            tag=tag or None,
            file_type=file_type or None,
            status=status or None,
        )
        total = search_engine.count(q, status=status or None)
    else:
        results = search_engine.recent(limit=page_limit, offset=offset, status=status or None)
        if status:
            row = _db.conn.execute("SELECT COUNT(*) as cnt FROM documents WHERE status = ?", (status,)).fetchone()
        else:
            row = _db.conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()
        total = int(row["cnt"]) if row else len(results)

    has_prev = offset > 0
    has_next = (offset + page_limit) < total
    cards = "".join(_render_card(doc) for doc in results)
    plural = "s" if total != 1 else ""
    context = "found" if q else "in archive"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LocalArchive</title>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               max-width: 900px; margin: 0 auto; padding: 2rem; background: #fafafa; color: #1a1a1a; }}
        h1 {{ font-size: 1.8rem; margin-bottom: 1rem; }}
        .search-box {{ display: flex; gap: 0.5rem; margin-bottom: 1.5rem; flex-wrap: wrap; }}
        .search-box input, .search-box select {{
            flex: 1; min-width: 120px; padding: 0.75rem; font-size: 1rem; border: 2px solid #ddd; border-radius: 8px;
        }}
        .search-box button {{ padding: 0.75rem 1.5rem; font-size: 1rem; background: #2563eb; color: white; border: none; border-radius: 8px; cursor: pointer; }}
        .search-box button:hover {{ background: #1d4ed8; }}
        .stats {{ color: #666; margin-bottom: 1rem; font-size: 0.9rem; }}
        .chip {{ display:inline-flex; align-items:center; padding:0.15rem 0.5rem; border-radius:999px; font-size:0.75rem; margin-left:0.4rem; border:1px solid #ddd; }}
        .chip.pending_ocr {{ background:#fff7ed; border-color:#fed7aa; color:#9a3412; }}
        .chip.processed {{ background:#ecfdf5; border-color:#86efac; color:#166534; }}
        .chip.error {{ background:#fef2f2; border-color:#fca5a5; color:#991b1b; }}
        .doc-card {{ background: white; border: 1px solid #e5e7eb; border-radius: 8px; padding: 1rem; margin-bottom: 0.75rem; }}
        .doc-card h3 {{ font-size: 1rem; margin-bottom: 0.25rem; }}
        .doc-card .meta {{ color: #666; font-size: 0.85rem; }}
        .doc-card .preview {{ margin-top: 0.5rem; color: #444; font-size: 0.9rem; background: #f9fafb; padding: 0.5rem; border-radius: 4px; max-height: 100px; overflow: hidden; }}
        .pager {{ margin-top:1rem; display:flex; gap:0.75rem; align-items:center; }}
        .pager a {{ color:#1d4ed8; }}
        .pager a[aria-disabled="true"] {{ color:#94a3b8; pointer-events:none; text-decoration:none; }}
    </style>
</head>
<body>
    <h1>&#128230; LocalArchive</h1>
    <form class="search-box" action="/" method="get" aria-label="Document Search">
        <input type="text" name="q" value="{escape(q)}" placeholder="Search your documents..." autofocus aria-label="Query">
        <input type="text" name="tag" value="{escape(tag)}" placeholder="tag" aria-label="Tag Filter">
        <input type="text" name="file_type" value="{escape(file_type)}" placeholder="type (pdf/png)" aria-label="Type Filter">
        <select name="status" aria-label="Status Filter">
            <option value="" {"selected" if not status else ""}>all statuses</option>
            <option value="pending_ocr" {"selected" if status == "pending_ocr" else ""}>pending_ocr</option>
            <option value="processed" {"selected" if status == "processed" else ""}>processed</option>
            <option value="error" {"selected" if status == "error" else ""}>error</option>
        </select>
        <input type="number" name="limit" value="{page_limit}" min="1" max="200" aria-label="Results Per Page">
        <button type="submit">Search</button>
    </form>
    <p class="stats">{total} document{plural} {context}</p>
    <main aria-live="polite">
        {cards}
    </main>
    <nav class="pager" aria-label="Pagination">
      <a tabindex="0" aria-disabled="{str(not has_prev).lower()}" href="/?q={escape(q)}&tag={escape(tag)}&file_type={escape(file_type)}&status={escape(status)}&limit={page_limit}&offset={max(0, offset-page_limit)}">Prev</a>
      <span>Showing {offset + 1 if total else 0} - {min(offset + page_limit, total)} of {total}</span>
      <a tabindex="0" aria-disabled="{str(not has_next).lower()}" href="/?q={escape(q)}&tag={escape(tag)}&file_type={escape(file_type)}&status={escape(status)}&limit={page_limit}&offset={offset+page_limit}">Next</a>
    </nav>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.get("/documents/{doc_id}", response_class=HTMLResponse)
async def document_detail(doc_id: int):
    doc = _db.get_document_detail(doc_id)
    if not doc:
        return HTMLResponse(content="<h1>Document not found</h1>", status_code=404)

    tags = ", ".join(escape(t) for t in doc.get("tags", [])) or "None"
    fields_rows = "".join(
        f"<tr><td>{escape(str(f.get('field_type', '')))}</td>"
        f"<td>{escape(str(f.get('value', '')))}</td></tr>"
        for f in doc.get("fields", [])
    ) or "<tr><td colspan='2'>No extracted fields</td></tr>"
    preview = escape((doc.get("ocr_text") or "")[:5000]).replace("\n", "<br>")
    status = escape(str(doc.get("status", "?")))
    return HTMLResponse(
        content=f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(doc.get("filename", "Document"))} - LocalArchive</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 900px; margin: 0 auto; padding: 2rem; background: #fafafa; color: #1a1a1a; }}
        a {{ color: #1d4ed8; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 1rem; }}
        th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; }}
        .preview {{ margin-top: 1rem; background: #fff; border: 1px solid #e5e7eb; padding: 1rem; border-radius: 8px; }}
        .chip {{ display:inline-flex; align-items:center; padding:0.15rem 0.5rem; border-radius:999px; font-size:0.75rem; border:1px solid #ddd; }}
        .chip.pending_ocr {{ background:#fff7ed; border-color:#fed7aa; color:#9a3412; }}
        .chip.processed {{ background:#ecfdf5; border-color:#86efac; color:#166534; }}
        .chip.error {{ background:#fef2f2; border-color:#fca5a5; color:#991b1b; }}
    </style>
</head>
<body>
    <p><a href="/">Back to search</a></p>
    <h1>{escape(doc.get("filename", "Untitled"))}</h1>
    <p><strong>ID:</strong> {doc["id"]}</p>
    <p><strong>Type:</strong> {escape(str(doc.get("file_type", "?")))}</p>
    <p><strong>Status:</strong> <span class="chip {status}">{status}</span></p>
    <p><strong>Tags:</strong> {tags}</p>
    <form action="/documents/{doc_id}/retry" method="post" style="margin-top:0.75rem;">
        <button type="submit">Retry Processing</button>
    </form>
    <form action="/documents/{doc_id}/tags" method="post" style="margin-top:0.75rem;">
        <label><strong>Update Tags:</strong></label><br>
        <input type="text" name="tags" value="{escape(', '.join(doc.get('tags', [])))}" style="width:100%;max-width:480px;">
        <button type="submit">Save Tags</button>
    </form>
    <h2>Extracted Fields</h2>
    <table>
        <thead><tr><th>Type</th><th>Value</th></tr></thead>
        <tbody>{fields_rows}</tbody>
    </table>
    <h2>OCR Preview</h2>
    <div class="preview">{preview}</div>
</body>
</html>"""
    )


@app.post("/documents/{doc_id}/retry")
async def retry_document(doc_id: int):
    doc = _db.get_document(doc_id)
    if not doc:
        return HTMLResponse(content="<h1>Document not found</h1>", status_code=404)
    _db.mark_for_reprocess([doc_id])
    return RedirectResponse(url=f"/documents/{doc_id}", status_code=303)


@app.post("/documents/{doc_id}/tags")
async def update_document_tags(doc_id: int, tags: str = Form(default="")):
    doc = _db.get_document(doc_id)
    if not doc:
        return HTMLResponse(content="<h1>Document not found</h1>", status_code=404)
    parsed = [tag.strip() for tag in tags.split(",")]
    _db.set_tags(doc_id, parsed)
    return RedirectResponse(url=f"/documents/{doc_id}", status_code=303)


def _render_card(doc: dict) -> str:
    preview = escape((doc.get("ocr_text") or "")[: config.ui.show_preview_chars])
    preview_html = f'<div class="preview">{preview}</div>' if preview else ""
    status = escape(str(doc.get("status", "?")))
    return f"""
    <div class="doc-card">
        <h3><a href="/documents/{doc['id']}" tabindex="0">{escape(doc.get("filename", "Untitled"))}</a><span class="chip {status}">{status}</span></h3>
        <p class="meta">ID: {doc["id"]} &middot; {escape(str(doc.get("file_type", "?")))} &middot; {escape(str(doc.get("ingested_at", "")))}</p>
        {preview_html}
    </div>"""
