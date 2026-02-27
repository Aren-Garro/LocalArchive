"""FastAPI web UI for LocalArchive. Lightweight HTMX-based interface."""

from pathlib import Path
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
async def index(request: Request, q: str = "", tag: str = ""):
    results = []
    total = 0
    if q:
        results = search_engine.search(q, tag=tag or None)
        total = search_engine.count(q)
    else:
        results = search_engine.recent(limit=20)
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
        <input type="text" name="q" value="{q}" placeholder="Search your documents..." autofocus>
        <button type="submit">Search</button>
    </form>
    <p class="stats">{total} document{plural} {context}</p>
    {cards}
</body>
</html>"""
    return HTMLResponse(content=html)


def _render_card(doc: dict) -> str:
    preview = (doc.get("ocr_text") or "")[:300]
    preview_html = f'<div class="preview">{preview}</div>' if preview else ""
    return f"""
    <div class="doc-card">
        <h3>{doc.get("filename", "Untitled")}</h3>
        <p class="meta">ID: {doc["id"]} &middot; {doc.get("file_type", "?")} &middot; {doc.get("ingested_at", "")}</p>
        {preview_html}
    </div>"""
