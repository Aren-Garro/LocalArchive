"""FastAPI web UI for LocalArchive. Lightweight, local-first interface."""

import secrets
import tempfile
from html import escape
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from localarchive.config import Config
from localarchive.core.ingester import Ingester
from localarchive.db.database import Database
from localarchive.db.search import SearchEngine
from localarchive.utils import is_supported, safe_filename

app = FastAPI(title="LocalArchive", docs_url=None, redoc_url=None)
config: Config = None
_db: Database = None
search_engine: SearchEngine = None


def _default_port(scheme: str) -> int:
    return 443 if scheme == "https" else 80


def _origin_matches_request(url: str, request: Request) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return False
    req_scheme = request.url.scheme
    req_host = (request.url.hostname or "").lower()
    req_port = request.url.port or _default_port(req_scheme)
    parsed_port = parsed.port or _default_port(parsed.scheme)
    return (
        parsed.scheme == req_scheme
        and parsed.hostname.lower() == req_host
        and parsed_port == req_port
    )


def _has_trusted_source(request: Request) -> bool:
    # For state-changing requests require either Origin or Referer to match exactly.
    origin = request.headers.get("origin", "")
    if origin and _origin_matches_request(origin, request):
        return True
    referer = request.headers.get("referer", "")
    if referer and _origin_matches_request(referer, request):
        return True
    return False


def _ensure_csrf_token(request: Request) -> str:
    token = request.cookies.get("localarchive_csrf")
    if token:
        return token
    return secrets.token_urlsafe(24)


def _with_csrf_cookie(response: HTMLResponse, csrf_token: str, request: Request) -> HTMLResponse:
    response.set_cookie(
        "localarchive_csrf",
        csrf_token,
        httponly=False,
        samesite="strict",
        secure=request.url.scheme == "https",
        path="/",
    )
    return response


def _validate_csrf(request: Request, csrf_token: str) -> bool:
    cookie_token = request.cookies.get("localarchive_csrf", "")
    return bool(cookie_token) and secrets.compare_digest(cookie_token, csrf_token)


def create_app(cfg: Config) -> FastAPI:
    global config, _db, search_engine
    config = cfg
    if _db is not None:
        _db.close()
    _db = Database(cfg.db_path)
    _db.initialize()
    search_engine = SearchEngine(_db)
    return app


@app.on_event("shutdown")
def _shutdown_db() -> None:
    global _db
    if _db is not None:
        _db.close()
        _db = None


def _shared_styles() -> str:
    return """
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               max-width: 980px; margin: 0 auto; padding: 1rem; background: #fafafa; color: #1a1a1a; }
        .skip-link { position:absolute; left:-10000px; top:auto; width:1px; height:1px; overflow:hidden; }
        .skip-link:focus { position:static; width:auto; height:auto; margin-bottom:0.75rem; display:inline-block; }
        h1 { font-size: 1.6rem; margin-bottom: 1rem; }
        a { color:#1d4ed8; }
        :focus-visible { outline: 3px solid #0f766e; outline-offset: 2px; }
        .topbar { display:flex; justify-content:space-between; align-items:center; gap:0.75rem; margin-bottom:1rem; flex-wrap:wrap; }
        .btn { padding: 0.6rem 0.9rem; border-radius: 8px; border: 1px solid #2563eb; background: #2563eb; color: white; text-decoration:none; }
        .btn.secondary { background: white; color:#1d4ed8; }
        .search-box { display: flex; gap: 0.5rem; margin-bottom: 1rem; flex-wrap: wrap; }
        .search-box input, .search-box select {
            flex: 1; min-width: 120px; padding: 0.75rem; font-size: 1rem; border: 2px solid #ddd; border-radius: 8px;
        }
        .search-box button { padding: 0.75rem 1rem; font-size: 1rem; background: #2563eb; color: white; border: none; border-radius: 8px; cursor: pointer; }
        .stats { color: #666; margin-bottom: 1rem; font-size: 0.9rem; }
        .chip { display:inline-flex; align-items:center; padding:0.15rem 0.5rem; border-radius:999px; font-size:0.75rem; margin-left:0.4rem; border:1px solid #ddd; }
        .chip.pending_ocr { background:#fff7ed; border-color:#fed7aa; color:#9a3412; }
        .chip.processed { background:#ecfdf5; border-color:#86efac; color:#166534; }
        .chip.error { background:#fef2f2; border-color:#fca5a5; color:#991b1b; }
        .doc-card { background: white; border: 1px solid #e5e7eb; border-radius: 8px; padding: 1rem; margin-bottom: 0.75rem; }
        .doc-card h3 { font-size: 1rem; margin-bottom: 0.25rem; }
        .doc-card .meta { color: #666; font-size: 0.85rem; }
        .doc-card .preview { margin-top: 0.5rem; color: #444; font-size: 0.9rem; background: #f9fafb; padding: 0.5rem; border-radius: 4px; max-height: 100px; overflow: hidden; }
        .pager { margin-top:1rem; display:flex; gap:0.75rem; align-items:center; flex-wrap:wrap; }
        .pager a[aria-disabled="true"] { color:#94a3b8; pointer-events:none; text-decoration:none; }
        .panel { background:white; border:1px solid #e5e7eb; border-radius:10px; padding:1rem; }
        .upload-zone { border: 2px dashed #94a3b8; border-radius: 10px; padding: 1.25rem; background:#f8fafc; }
        .upload-zone.drag { border-color:#0f766e; background:#ecfeff; }
        .hint { color:#475569; font-size:0.9rem; margin-top:0.5rem; }
        @media (max-width: 768px) {
            body { padding: 0.75rem; }
            .search-box input, .search-box select, .search-box button { width: 100%; min-width: unset; }
            .topbar { align-items: stretch; }
        }
    </style>
    """


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
    page_limit = max(1, min(limit or config.ui.default_limit, 200))
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
        total = search_engine.count(
            q,
            tag=tag or None,
            file_type=file_type or None,
            status=status or None,
        )
    else:
        results = search_engine.recent(limit=page_limit, offset=offset, status=status or None)
        if status:
            row = _db.conn.execute(
                "SELECT COUNT(*) as cnt FROM documents WHERE status = ?", (status,)
            ).fetchone()
        else:
            row = _db.conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()
        total = int(row["cnt"]) if row else len(results)

    has_prev = offset > 0
    has_next = (offset + page_limit) < total
    cards = "".join(_render_card(doc) for doc in results)
    csrf_token = _ensure_csrf_token(request)
    plural = "s" if total != 1 else ""
    context = "found" if q else "in archive"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LocalArchive</title>
    {_shared_styles()}
</head>
<body>
    <a href="#results" class="skip-link">Skip to results</a>
    <header class="topbar">
      <h1>&#128230; LocalArchive</h1>
      <a class="btn secondary" href="/ingest">Upload Documents</a>
    </header>
    <main>
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
      <section id="results" aria-live="polite">
          {cards}
      </section>
      <nav class="pager" aria-label="Pagination">
        <a tabindex="0" aria-disabled="{str(not has_prev).lower()}" href="/?q={escape(q)}&tag={escape(tag)}&file_type={escape(file_type)}&status={escape(status)}&limit={page_limit}&offset={max(0, offset - page_limit)}">Prev</a>
        <span>Showing {offset + 1 if total else 0} - {min(offset + page_limit, total)} of {total}</span>
        <a tabindex="0" aria-disabled="{str(not has_next).lower()}" href="/?q={escape(q)}&tag={escape(tag)}&file_type={escape(file_type)}&status={escape(status)}&limit={page_limit}&offset={offset + page_limit}">Next</a>
      </nav>
      <input type="hidden" name="csrf_token" value="{csrf_token}">
    </main>
</body>
</html>"""
    return _with_csrf_cookie(HTMLResponse(content=html), csrf_token, request)


@app.get("/ingest", response_class=HTMLResponse)
async def ingest_form(request: Request):
    csrf_token = _ensure_csrf_token(request)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Upload Documents - LocalArchive</title>
    {_shared_styles()}
</head>
<body>
    <a href="/" class="btn secondary">Back</a>
    <h1>Upload Documents</h1>
    <div class="panel">
      <form action="/ingest" method="post" enctype="multipart/form-data" aria-label="Upload Documents">
        <input type="hidden" name="csrf_token" value="{csrf_token}">
        <div class="upload-zone" id="upload-zone">
          <p><strong>Drag and drop files here</strong> or choose files below.</p>
          <input id="file-input" type="file" name="files" multiple aria-label="Select document files">
          <p class="hint">Supported: PDF, PNG, JPG, TIFF, BMP, WEBP, GIF</p>
        </div>
        <p style="margin-top:0.75rem;">
          <button class="btn" type="submit">Ingest Files</button>
        </p>
      </form>
    </div>
    <script>
      const zone = document.getElementById('upload-zone');
      const input = document.getElementById('file-input');
      if (zone && input) {{
        zone.addEventListener('dragover', (e) => {{
          e.preventDefault();
          zone.classList.add('drag');
        }});
        zone.addEventListener('dragleave', () => zone.classList.remove('drag'));
        zone.addEventListener('drop', (e) => {{
          e.preventDefault();
          zone.classList.remove('drag');
          if (e.dataTransfer && e.dataTransfer.files) {{
            input.files = e.dataTransfer.files;
          }}
        }});
      }}
    </script>
</body>
</html>"""
    return _with_csrf_cookie(HTMLResponse(content=html), csrf_token, request)


@app.post("/ingest")
async def ingest_upload(
    request: Request, files: list[UploadFile] = File(default=[]), csrf_token: str = Form(default="")
):
    if not _has_trusted_source(request) or not _validate_csrf(request, csrf_token):
        return HTMLResponse(content="<h1>Forbidden</h1>", status_code=403)
    if not files:
        return RedirectResponse(url="/ingest", status_code=303)
    ingester = Ingester(config, _db)
    for upload in files:
        original_name = safe_filename(Path(upload.filename or "upload.bin").name)
        if not original_name:
            continue
        if not is_supported(Path(original_name)):
            continue
        suffix = Path(original_name).suffix.lower()
        fd, tmp_name = tempfile.mkstemp(prefix="upload-", suffix=suffix, dir=str(config.runtime.tmp_dir))
        tmp_path = Path(tmp_name)
        try:
            with open(fd, "wb", closefd=True) as out:
                while True:
                    chunk = await upload.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            ingester.ingest_path(tmp_path, source_name=original_name)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            await upload.close()
    return RedirectResponse(url="/", status_code=303)


@app.get("/documents/{doc_id}", response_class=HTMLResponse)
async def document_detail(request: Request, doc_id: int):
    doc = _db.get_document_detail(doc_id)
    if not doc:
        return HTMLResponse(content="<h1>Document not found</h1>", status_code=404)

    tags = ", ".join(escape(t) for t in doc.get("tags", [])) or "None"
    fields_rows = (
        "".join(
            f"<tr><td>{escape(str(f.get('field_type', '')))}</td>"
            f"<td>{escape(str(f.get('value', '')))}</td></tr>"
            for f in doc.get("fields", [])
        )
        or "<tr><td colspan='2'>No extracted fields</td></tr>"
    )
    preview = escape((doc.get("ocr_text") or "")[:5000]).replace("\n", "<br>")
    status = escape(str(doc.get("status", "?")))
    tables = doc.get("tables") or []
    tables_html_parts: list[str] = []
    for table in tables:
        headers = table.get("headers") or []
        rows = table.get("rows") or []
        header_cells = "".join(f"<th>{escape(str(h))}</th>" for h in headers)
        body_rows = "".join(
            "<tr>" + "".join(f"<td>{escape(str(cell))}</td>" for cell in row) + "</tr>"
            for row in rows
        )
        tables_html_parts.append(
            "<div class='panel' style='margin-top:0.75rem;'>"
            f"<p><strong>Table {int(table.get('table_index', 0)) + 1}</strong></p>"
            f"<table><thead><tr>{header_cells}</tr></thead><tbody>{body_rows}</tbody></table>"
            "</div>"
        )
    tables_html = "".join(tables_html_parts)
    related = _db.get_similar_documents(doc_id, limit=5)
    related_html = ""
    if related:
        items = "".join(
            f"<li><a href='/documents/{int(r.get('related_id', 0))}'>{escape(str(r.get('filename', 'Untitled')))}</a> "
            f"<span class='hint'>(score={float(r.get('score', 0.0)):.3f})</span></li>"
            for r in related
        )
        related_html = f"<ul>{items}</ul>"
    csrf_token = _ensure_csrf_token(request)
    response = HTMLResponse(
        content=f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(doc.get("filename", "Document"))} - LocalArchive</title>
    {_shared_styles()}
</head>
<body>
    <a href="/" class="btn secondary">Back to search</a>
    <h1>{escape(doc.get("filename", "Untitled"))}</h1>
    <div class="panel">
      <p><strong>ID:</strong> {doc["id"]}</p>
      <p><strong>Type:</strong> {escape(str(doc.get("file_type", "?")))}</p>
      <p><strong>Status:</strong> <span class="chip {status}">{status}</span></p>
      <p><strong>Tags:</strong> {tags}</p>
      <form action="/documents/{doc_id}/retry" method="post" style="margin-top:0.75rem;">
          <input type="hidden" name="csrf_token" value="{csrf_token}">
          <button class="btn" type="submit">Retry Processing</button>
      </form>
      <form action="/documents/{doc_id}/tags" method="post" style="margin-top:0.75rem;">
          <label><strong>Update Tags:</strong></label><br>
          <input type="hidden" name="csrf_token" value="{csrf_token}">
          <input type="text" name="tags" value="{escape(", ".join(doc.get("tags", [])))}" style="width:100%;max-width:480px;">
          <button class="btn" type="submit">Save Tags</button>
      </form>
    </div>
    <h2>Extracted Fields</h2>
    <table>
        <thead><tr><th>Type</th><th>Value</th></tr></thead>
        <tbody>{fields_rows}</tbody>
    </table>
    <h2>OCR Preview</h2>
    <div class="panel">{preview}</div>
    <h2>Extracted Tables</h2>
    {tables_html or "<p class='hint'>No tables extracted.</p>"}
    <h2>Related Documents</h2>
    {related_html or "<p class='hint'>No similarity edges built yet. Run `localarchive similarity build`.</p>"}
</body>
</html>"""
    )
    return _with_csrf_cookie(response, csrf_token, request)


@app.post("/documents/{doc_id}/retry")
async def retry_document(request: Request, doc_id: int, csrf_token: str = Form(default="")):
    if not _has_trusted_source(request) or not _validate_csrf(request, csrf_token):
        return HTMLResponse(content="<h1>Forbidden</h1>", status_code=403)
    doc = _db.get_document(doc_id)
    if not doc:
        return HTMLResponse(content="<h1>Document not found</h1>", status_code=404)
    _db.mark_for_reprocess([doc_id])
    return RedirectResponse(url=f"/documents/{doc_id}", status_code=303)


@app.post("/documents/{doc_id}/tags")
async def update_document_tags(
    request: Request,
    doc_id: int,
    tags: str = Form(default=""),
    csrf_token: str = Form(default=""),
):
    if not _has_trusted_source(request) or not _validate_csrf(request, csrf_token):
        return HTMLResponse(content="<h1>Forbidden</h1>", status_code=403)
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
    <article class="doc-card">
        <h3><a href="/documents/{doc["id"]}" tabindex="0">{escape(doc.get("filename", "Untitled"))}</a><span class="chip {status}">{status}</span></h3>
        <p class="meta">ID: {doc["id"]} &middot; {escape(str(doc.get("file_type", "?")))} &middot; {escape(str(doc.get("ingested_at", "")))}</p>
        {preview_html}
    </article>"""
