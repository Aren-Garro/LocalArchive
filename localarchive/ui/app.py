"""FastAPI web UI for LocalArchive. Lightweight HTMX-based interface."""

from pathlib import Path
from html import escape
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from localarchive.config import Config
from localarchive.db.database import Database
from localarchive.db.search import SearchEngine

app = FastAPI(title="LocalArchive", docs_url=None, redoc_url=None)
config: Config = None
db: Database = None
search_engine: SearchEngine = None


def create_app(cfg: Config) -> FastAPI:
    global config, db, search_engine
    config = cfg
    db = Database(cfg.db_path)
    db.initialize()
    search_engine = SearchEngine(db)
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
    total = 0
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
        total = len(results)

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
        .search-box {{ display: flex; gap: 0.5rem; margin-bottom: 1.5rem; }}
        .search-box input {{ flex: 1; padding: 0.75rem; font-size: 1rem; border: 2px solid #ddd; border-radius: 8px; }}
        .search-box button {{ padding: 0.75rem 1.5rem; font-size: 1rem; background: #2563eb; color: white; border: none; border-radius: 8px; cursor: pointer; }}
        .search-box button:hover {{ background: #1d4ed8; }}
        .stats {{ color: #666; margin-bottom: 1rem; font-size: 0.9rem; }}
        .doc-card {{ background: white; border: 1px solid #e5e7eb; border-radius: 8px; padding: 1rem; margin-bottom: 0.75rem; }}
        .doc-card h3 {{ font-size: 1rem; margin-bottom: 0.25rem; }}
        .doc-card .meta {{ color: #666; font-size: 0.85rem; }}
        .doc-card .preview {{ margin-top: 0.5rem; color: #444; font-size: 0.9rem; background: #f9fafb; padding: 0.5rem; border-radius: 4px; max-height: 100px; overflow: hidden; }}
    </style>
</head>
<body>
    <h1>&#128230; LocalArchive</h1>
    <form class="search-box" action="/" method="get">
        <input type="text" name="q" value="{escape(q)}" placeholder="Search your documents..." autofocus>
        <input type="text" name="tag" value="{escape(tag)}" placeholder="tag">
        <input type="text" name="file_type" value="{escape(file_type)}" placeholder="type (pdf/png)">
        <input type="text" name="status" value="{escape(status)}" placeholder="status">
        <input type="number" name="limit" value="{page_limit}" min="1" max="200">
        <button type="submit">Search</button>
    </form>
    <p class="stats">{total} document{plural} {context}</p>
    {cards}
    <div style="margin-top:1rem;display:flex;gap:0.75rem;">
      <a href="/?q={escape(q)}&tag={escape(tag)}&file_type={escape(file_type)}&status={escape(status)}&limit={page_limit}&offset={max(0, offset-page_limit)}">Prev</a>
      <a href="/?q={escape(q)}&tag={escape(tag)}&file_type={escape(file_type)}&status={escape(status)}&limit={page_limit}&offset={offset+page_limit}">Next</a>
    </div>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.get("/documents/{doc_id}", response_class=HTMLResponse)
async def document_detail(doc_id: int):
    doc = db.get_document_detail(doc_id)
    if not doc:
        return HTMLResponse(content="<h1>Document not found</h1>", status_code=404)

    tags = ", ".join(escape(t) for t in doc.get("tags", [])) or "None"
    fields_rows = "".join(
        f"<tr><td>{escape(str(f.get('field_type', '')))}</td>"
        f"<td>{escape(str(f.get('value', '')))}</td></tr>"
        for f in doc.get("fields", [])
    ) or "<tr><td colspan='2'>No extracted fields</td></tr>"
    preview = escape((doc.get("ocr_text") or "")[:5000]).replace("\n", "<br>")
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
    </style>
</head>
<body>
    <p><a href="/">Back to search</a></p>
    <h1>{escape(doc.get("filename", "Untitled"))}</h1>
    <p><strong>ID:</strong> {doc["id"]}</p>
    <p><strong>Type:</strong> {escape(str(doc.get("file_type", "?")))}</p>
    <p><strong>Status:</strong> {escape(str(doc.get("status", "?")))}</p>
    <p><strong>Tags:</strong> {tags}</p>
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


def _render_card(doc: dict) -> str:
    preview = escape((doc.get("ocr_text") or "")[: config.ui.show_preview_chars])
    preview_html = f'<div class="preview">{preview}</div>' if preview else ""
    return f"""
    <div class="doc-card">
        <h3><a href="/documents/{doc['id']}">{escape(doc.get("filename", "Untitled"))}</a></h3>
        <p class="meta">ID: {doc["id"]} &middot; {escape(str(doc.get("file_type", "?")))} &middot; {escape(str(doc.get("ingested_at", "")))}</p>
        {preview_html}
    </div>"""
