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
SUPPORTED_UI_LANGUAGES = ("en", "es")
UI_TEXT = {
    "en": {
        "title": "LocalArchive",
        "skip_to_results": "Skip to results",
        "upload_documents": "Upload Documents",
        "search_aria": "Document Search",
        "query_placeholder": "Search your documents...",
        "query_label": "Query",
        "tag_placeholder": "tag",
        "tag_label": "Tag Filter",
        "type_placeholder": "type (pdf/png)",
        "type_label": "Type Filter",
        "status_label": "Status Filter",
        "status_all": "all statuses",
        "results_per_page": "Results Per Page",
        "search_button": "Search",
        "stats_found": "{total} document{plural} found",
        "stats_archive": "{total} document{plural} in archive",
        "pagination_aria": "Pagination",
        "prev": "Prev",
        "next": "Next",
        "showing": "Showing {start} - {end} of {total}",
        "back": "Back",
        "back_to_search": "Back to search",
        "upload_title": "Upload Documents",
        "drag_drop_hint": "Drag and drop files here",
        "or_choose_hint": "or choose files below.",
        "supported_label": "Supported: PDF, PNG, JPG, TIFF, BMP, WEBP, GIF",
        "ingest_button": "Ingest Files",
        "forbidden": "Forbidden",
        "document_not_found": "Document not found",
        "none": "None",
        "no_extracted_fields": "No extracted fields",
        "table_label": "Table {index}",
        "id_label": "ID",
        "type_short": "Type",
        "status_short": "Status",
        "tags_short": "Tags",
        "retry_processing": "Retry Processing",
        "update_tags": "Update Tags",
        "save_tags": "Save Tags",
        "extracted_fields": "Extracted Fields",
        "value": "Value",
        "ocr_preview": "OCR Preview",
        "extracted_tables": "Extracted Tables",
        "no_tables_extracted": "No tables extracted.",
        "related_documents": "Related Documents",
        "no_similarity_edges": "No similarity edges built yet. Run `localarchive similarity build`.",
    },
    "es": {
        "title": "LocalArchive",
        "skip_to_results": "Saltar a resultados",
        "upload_documents": "Subir documentos",
        "search_aria": "Busqueda de documentos",
        "query_placeholder": "Buscar en tus documentos...",
        "query_label": "Consulta",
        "tag_placeholder": "etiqueta",
        "tag_label": "Filtro de etiqueta",
        "type_placeholder": "tipo (pdf/png)",
        "type_label": "Filtro de tipo",
        "status_label": "Filtro de estado",
        "status_all": "todos los estados",
        "results_per_page": "Resultados por pagina",
        "search_button": "Buscar",
        "stats_found": "{total} documento{plural} encontrado{plural}",
        "stats_archive": "{total} documento{plural} en el archivo",
        "pagination_aria": "Paginacion",
        "prev": "Anterior",
        "next": "Siguiente",
        "showing": "Mostrando {start} - {end} de {total}",
        "back": "Volver",
        "back_to_search": "Volver a busqueda",
        "upload_title": "Subir documentos",
        "drag_drop_hint": "Arrastra y suelta archivos aqui",
        "or_choose_hint": "o elige archivos abajo.",
        "supported_label": "Soporta: PDF, PNG, JPG, TIFF, BMP, WEBP, GIF",
        "ingest_button": "Ingerir archivos",
        "forbidden": "Prohibido",
        "document_not_found": "Documento no encontrado",
        "none": "Ninguno",
        "no_extracted_fields": "Sin campos extraidos",
        "table_label": "Tabla {index}",
        "id_label": "ID",
        "type_short": "Tipo",
        "status_short": "Estado",
        "tags_short": "Etiquetas",
        "retry_processing": "Reintentar procesamiento",
        "update_tags": "Actualizar etiquetas",
        "save_tags": "Guardar etiquetas",
        "extracted_fields": "Campos extraidos",
        "value": "Valor",
        "ocr_preview": "Vista OCR",
        "extracted_tables": "Tablas extraidas",
        "no_tables_extracted": "No se extrajeron tablas.",
        "related_documents": "Documentos relacionados",
        "no_similarity_edges": "Aun no hay similitud. Ejecuta `localarchive similarity build`.",
    },
}


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


def _normalize_language(raw: str | None) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return ""
    value = value.replace("_", "-")
    return value.split("-", maxsplit=1)[0]


def _resolve_language(request: Request, requested: str | None = None) -> str:
    candidates = [
        requested,
        request.cookies.get("localarchive_lang"),
        getattr(config.ui, "language", "en"),
        "en",
    ]
    for candidate in candidates:
        normalized = _normalize_language(candidate)
        if normalized in UI_TEXT:
            return normalized
    return "en"


def _t(language: str, key: str, **kwargs) -> str:
    table = UI_TEXT.get(language, UI_TEXT["en"])
    fallback = UI_TEXT["en"]
    text = table.get(key, fallback.get(key, key))
    return text.format(**kwargs) if kwargs else text


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


def _with_lang_cookie(response: HTMLResponse, language: str, request: Request) -> HTMLResponse:
    response.set_cookie(
        "localarchive_lang",
        language,
        httponly=False,
        samesite="lax",
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
    lang: str | None = None,
    limit: int | None = None,
    offset: int = 0,
):
    language = _resolve_language(request, lang)
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
    cards = "".join(_render_card(doc, language) for doc in results)
    csrf_token = _ensure_csrf_token(request)
    plural = "s" if total != 1 else ""
    stats_text = (
        _t(language, "stats_found", total=total, plural=plural)
        if q
        else _t(language, "stats_archive", total=total, plural=plural)
    )
    html = f"""<!DOCTYPE html>
<html lang="{language}">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{_t(language, "title")}</title>
    {_shared_styles()}
</head>
<body>
    <a href="#results" class="skip-link">{_t(language, "skip_to_results")}</a>
    <header class="topbar">
      <h1>&#128230; {_t(language, "title")}</h1>
      <a class="btn secondary" href="/ingest?lang={language}">{_t(language, "upload_documents")}</a>
    </header>
    <main>
      <form class="search-box" action="/" method="get" aria-label="{_t(language, "search_aria")}">
          <input type="hidden" name="lang" value="{language}">
          <input type="text" name="q" value="{escape(q)}" placeholder="{_t(language, "query_placeholder")}" autofocus aria-label="{_t(language, "query_label")}">
          <input type="text" name="tag" value="{escape(tag)}" placeholder="{_t(language, "tag_placeholder")}" aria-label="{_t(language, "tag_label")}">
          <input type="text" name="file_type" value="{escape(file_type)}" placeholder="{_t(language, "type_placeholder")}" aria-label="{_t(language, "type_label")}">
          <select name="status" aria-label="{_t(language, "status_label")}">
              <option value="" {"selected" if not status else ""}>{_t(language, "status_all")}</option>
              <option value="pending_ocr" {"selected" if status == "pending_ocr" else ""}>pending_ocr</option>
              <option value="processed" {"selected" if status == "processed" else ""}>processed</option>
              <option value="error" {"selected" if status == "error" else ""}>error</option>
          </select>
          <input type="number" name="limit" value="{page_limit}" min="1" max="200" aria-label="{_t(language, "results_per_page")}">
          <button type="submit">{_t(language, "search_button")}</button>
      </form>
      <p class="stats">{stats_text}</p>
      <section id="results" aria-live="polite">
          {cards}
      </section>
      <nav class="pager" aria-label="{_t(language, "pagination_aria")}">
        <a tabindex="0" aria-disabled="{str(not has_prev).lower()}" href="/?q={escape(q)}&tag={escape(tag)}&file_type={escape(file_type)}&status={escape(status)}&lang={language}&limit={page_limit}&offset={max(0, offset - page_limit)}">{_t(language, "prev")}</a>
        <span>{_t(language, "showing", start=offset + 1 if total else 0, end=min(offset + page_limit, total), total=total)}</span>
        <a tabindex="0" aria-disabled="{str(not has_next).lower()}" href="/?q={escape(q)}&tag={escape(tag)}&file_type={escape(file_type)}&status={escape(status)}&lang={language}&limit={page_limit}&offset={offset + page_limit}">{_t(language, "next")}</a>
      </nav>
      <input type="hidden" name="csrf_token" value="{csrf_token}">
    </main>
</body>
</html>"""
    return _with_lang_cookie(_with_csrf_cookie(HTMLResponse(content=html), csrf_token, request), language, request)


@app.get("/ingest", response_class=HTMLResponse)
async def ingest_form(request: Request, lang: str | None = None):
    language = _resolve_language(request, lang)
    csrf_token = _ensure_csrf_token(request)
    html = f"""<!DOCTYPE html>
<html lang="{language}">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{_t(language, "upload_title")} - {_t(language, "title")}</title>
    {_shared_styles()}
</head>
<body>
    <a href="/?lang={language}" class="btn secondary">{_t(language, "back")}</a>
    <h1>{_t(language, "upload_title")}</h1>
    <div class="panel">
      <form action="/ingest" method="post" enctype="multipart/form-data" aria-label="{_t(language, "upload_title")}">
        <input type="hidden" name="csrf_token" value="{csrf_token}">
        <input type="hidden" name="lang" value="{language}">
        <div class="upload-zone" id="upload-zone">
          <p><strong>{_t(language, "drag_drop_hint")}</strong> {_t(language, "or_choose_hint")}</p>
          <input id="file-input" type="file" name="files" multiple aria-label="{_t(language, "upload_documents")}">
          <p class="hint">{_t(language, "supported_label")}</p>
        </div>
        <p style="margin-top:0.75rem;">
          <button class="btn" type="submit">{_t(language, "ingest_button")}</button>
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
    return _with_lang_cookie(_with_csrf_cookie(HTMLResponse(content=html), csrf_token, request), language, request)


@app.post("/ingest")
async def ingest_upload(
    request: Request,
    files: list[UploadFile] = File(default=[]),
    csrf_token: str = Form(default=""),
    lang: str = Form(default=""),
):
    language = _resolve_language(request, lang)
    if not _has_trusted_source(request) or not _validate_csrf(request, csrf_token):
        return _with_lang_cookie(
            HTMLResponse(content=f"<h1>{_t(language, 'forbidden')}</h1>", status_code=403),
            language,
            request,
        )
    if not files:
        return _with_lang_cookie(RedirectResponse(url=f"/ingest?lang={language}", status_code=303), language, request)
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
    return _with_lang_cookie(RedirectResponse(url=f"/?lang={language}", status_code=303), language, request)


@app.get("/documents/{doc_id}", response_class=HTMLResponse)
async def document_detail(request: Request, doc_id: int, lang: str | None = None):
    language = _resolve_language(request, lang)
    doc = _db.get_document_detail(doc_id)
    if not doc:
        return _with_lang_cookie(
            HTMLResponse(content=f"<h1>{_t(language, 'document_not_found')}</h1>", status_code=404),
            language,
            request,
        )

    tags = ", ".join(escape(t) for t in doc.get("tags", [])) or _t(language, "none")
    fields_rows = (
        "".join(
            f"<tr><td>{escape(str(f.get('field_type', '')))}</td>"
            f"<td>{escape(str(f.get('value', '')))}</td></tr>"
            for f in doc.get("fields", [])
        )
        or f"<tr><td colspan='2'>{_t(language, 'no_extracted_fields')}</td></tr>"
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
            f"<p><strong>{_t(language, 'table_label', index=int(table.get('table_index', 0)) + 1)}</strong></p>"
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
<html lang="{language}">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(doc.get("filename", "Document"))} - LocalArchive</title>
    {_shared_styles()}
</head>
<body>
    <a href="/?lang={language}" class="btn secondary">{_t(language, "back_to_search")}</a>
    <h1>{escape(doc.get("filename", "Untitled"))}</h1>
    <div class="panel">
      <p><strong>{_t(language, "id_label")}:</strong> {doc["id"]}</p>
      <p><strong>{_t(language, "type_short")}:</strong> {escape(str(doc.get("file_type", "?")))}</p>
      <p><strong>{_t(language, "status_short")}:</strong> <span class="chip {status}">{status}</span></p>
      <p><strong>{_t(language, "tags_short")}:</strong> {tags}</p>
      <form action="/documents/{doc_id}/retry" method="post" style="margin-top:0.75rem;">
          <input type="hidden" name="csrf_token" value="{csrf_token}">
          <input type="hidden" name="lang" value="{language}">
          <button class="btn" type="submit">{_t(language, "retry_processing")}</button>
      </form>
      <form action="/documents/{doc_id}/tags" method="post" style="margin-top:0.75rem;">
          <label><strong>{_t(language, "update_tags")}:</strong></label><br>
          <input type="hidden" name="csrf_token" value="{csrf_token}">
          <input type="hidden" name="lang" value="{language}">
          <input type="text" name="tags" value="{escape(", ".join(doc.get("tags", [])))}" style="width:100%;max-width:480px;">
          <button class="btn" type="submit">{_t(language, "save_tags")}</button>
      </form>
    </div>
    <h2>{_t(language, "extracted_fields")}</h2>
    <table>
        <thead><tr><th>{_t(language, "type_short")}</th><th>{_t(language, "value")}</th></tr></thead>
        <tbody>{fields_rows}</tbody>
    </table>
    <h2>{_t(language, "ocr_preview")}</h2>
    <div class="panel">{preview}</div>
    <h2>{_t(language, "extracted_tables")}</h2>
    {tables_html or f"<p class='hint'>{_t(language, 'no_tables_extracted')}</p>"}
    <h2>{_t(language, "related_documents")}</h2>
    {related_html or f"<p class='hint'>{_t(language, 'no_similarity_edges')}</p>"}
</body>
</html>"""
    )
    return _with_lang_cookie(_with_csrf_cookie(response, csrf_token, request), language, request)


@app.post("/documents/{doc_id}/retry")
async def retry_document(
    request: Request, doc_id: int, csrf_token: str = Form(default=""), lang: str = Form(default="")
):
    language = _resolve_language(request, lang)
    if not _has_trusted_source(request) or not _validate_csrf(request, csrf_token):
        return _with_lang_cookie(
            HTMLResponse(content=f"<h1>{_t(language, 'forbidden')}</h1>", status_code=403),
            language,
            request,
        )
    doc = _db.get_document(doc_id)
    if not doc:
        return _with_lang_cookie(
            HTMLResponse(content=f"<h1>{_t(language, 'document_not_found')}</h1>", status_code=404),
            language,
            request,
        )
    _db.mark_for_reprocess([doc_id])
    return _with_lang_cookie(
        RedirectResponse(url=f"/documents/{doc_id}?lang={language}", status_code=303),
        language,
        request,
    )


@app.post("/documents/{doc_id}/tags")
async def update_document_tags(
    request: Request,
    doc_id: int,
    tags: str = Form(default=""),
    csrf_token: str = Form(default=""),
    lang: str = Form(default=""),
):
    language = _resolve_language(request, lang)
    if not _has_trusted_source(request) or not _validate_csrf(request, csrf_token):
        return _with_lang_cookie(
            HTMLResponse(content=f"<h1>{_t(language, 'forbidden')}</h1>", status_code=403),
            language,
            request,
        )
    doc = _db.get_document(doc_id)
    if not doc:
        return _with_lang_cookie(
            HTMLResponse(content=f"<h1>{_t(language, 'document_not_found')}</h1>", status_code=404),
            language,
            request,
        )
    parsed = [tag.strip() for tag in tags.split(",")]
    _db.set_tags(doc_id, parsed)
    return _with_lang_cookie(
        RedirectResponse(url=f"/documents/{doc_id}?lang={language}", status_code=303),
        language,
        request,
    )


def _render_card(doc: dict, language: str) -> str:
    preview = escape((doc.get("ocr_text") or "")[: config.ui.show_preview_chars])
    preview_html = f'<div class="preview">{preview}</div>' if preview else ""
    status = escape(str(doc.get("status", "?")))
    return f"""
    <article class="doc-card">
        <h3><a href="/documents/{doc["id"]}?lang={language}" tabindex="0">{escape(doc.get("filename", "Untitled"))}</a><span class="chip {status}">{status}</span></h3>
        <p class="meta">{_t(language, "id_label")}: {doc["id"]} &middot; {escape(str(doc.get("file_type", "?")))} &middot; {escape(str(doc.get("ingested_at", "")))}</p>
        {preview_html}
    </article>"""
