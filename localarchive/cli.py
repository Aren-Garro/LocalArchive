"""
LocalArchive CLI - main entry point.
Commands: init, ingest, search, export, tag, process, reprocess, watch, doctor, serve
"""

import click
import importlib.util
from pathlib import Path
from rich.console import Console
from rich.table import Table
from localarchive.config import Config, DEFAULT_CONFIG_PATH
from localarchive.db.database import Database
from localarchive.db.search import SearchEngine
from localarchive.core.ingester import Ingester, watch_directory

console = Console()


class CLIError(click.ClickException):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message)
        self.exit_code = exit_code


def _set_console(quiet: bool, no_color: bool) -> None:
    global console
    console = Console(quiet=quiet, no_color=no_color)


def _runtime_ctx() -> dict:
    ctx = click.get_current_context(silent=True)
    if not ctx or not ctx.obj:
        return {}
    return ctx.obj


def get_config() -> Config:
    opts = _runtime_ctx()
    config_path = opts.get("config_path")
    try:
        cfg = Config.load(config_path or DEFAULT_CONFIG_PATH)
    except ValueError as exc:
        raise CLIError(f"Invalid configuration: {exc}", exit_code=2) from exc
    return cfg


def get_db(config: Config) -> Database:
    db = Database(config.db_path)
    db.initialize()
    return db


@click.group()
@click.option("--config", "config_path", type=click.Path(dir_okay=False, path_type=Path), default=None)
@click.option("--verbose", is_flag=True, help="Verbose diagnostics.")
@click.option("--quiet", is_flag=True, help="Suppress non-error output.")
@click.option("--no-color", is_flag=True, help="Disable colored output.")
@click.version_option(version="0.1.0")
@click.pass_context
def main(ctx: click.Context, config_path: Path | None, verbose: bool, quiet: bool, no_color: bool):
    """LocalArchive - Your private, offline document library."""
    ctx.obj = {
        "config_path": config_path,
        "verbose": verbose,
        "quiet": quiet,
        "no_color": no_color,
    }
    _set_console(quiet=quiet, no_color=no_color)


def _validate_limit(limit: int) -> None:
    if limit < 1:
        raise CLIError("Limit must be >= 1.", exit_code=2)


@main.command()
@click.option("--rewrite-config", is_flag=True, help="Overwrite config file with defaults.")
def init(rewrite_config: bool):
    """Initialize LocalArchive database and directories."""
    opts = _runtime_ctx()
    config_path = opts.get("config_path") or DEFAULT_CONFIG_PATH
    config = get_config()
    config.ensure_dirs()
    if not config_path.exists() or rewrite_config:
        try:
            config.save(config_path=config_path)
        except PermissionError:
            console.print(f"[yellow]Could not write config file:[/yellow] {config_path}")
    else:
        config = get_config()
    db = get_db(config)
    db.close()
    console.print("[green]LocalArchive initialized.[/green]")
    console.print(f"  Archive dir: {config.archive_dir}")
    console.print(f"  Database: {config.db_path}")


@main.command()
@click.argument("path")
def ingest(path: str):
    """Ingest a file or folder into the archive."""
    config = get_config()
    config.ensure_dirs()
    db = get_db(config)
    ingester = Ingester(config, db)
    doc_ids = ingester.ingest_path(Path(path))
    if doc_ids:
        console.print("\nRun [cyan]localarchive process[/cyan] to OCR these documents.")
    db.close()


@main.command()
@click.argument("query")
@click.option("--tag", default=None, help="Filter by tag")
@click.option("--file-type", default=None, help="Filter by file type")
@click.option("--type", "legacy_file_type", default=None, hidden=True)
@click.option("--limit", default=None, type=int, help="Max results")
def search(query: str, tag: str, file_type: str | None, legacy_file_type: str | None, limit: int | None):
    """Search documents in the archive."""
    config = get_config()
    max_results = limit if limit is not None else config.ui.default_limit
    _validate_limit(max_results)
    if legacy_file_type and not file_type:
        file_type = legacy_file_type
        console.print("[yellow]`--type` is deprecated. Use `--file-type`.[/yellow]")
    db = get_db(config)
    engine = SearchEngine(db)
    results = engine.search(query, limit=max_results, tag=tag, file_type=file_type)
    if not results:
        console.print("[yellow]No results found.[/yellow]")
        db.close()
        return
    table = Table(title=f"Search: {query}")
    table.add_column("ID", style="cyan", width=6)
    table.add_column("Filename", style="bold")
    table.add_column("Type", width=6)
    table.add_column("Ingested", width=22)
    table.add_column("Preview", max_width=50)
    for doc in results:
        preview = (doc.get("ocr_text") or "")[:80]
        table.add_row(str(doc["id"]), doc["filename"], doc.get("file_type", "?"), doc.get("ingested_at", ""), preview)
    console.print(table)
    console.print(f"\n[dim]{len(results)} result(s)[/dim]")
    db.close()


@main.command()
@click.option("--query", default=None, help="Search query to filter export")
@click.option("--format", "fmt", type=click.Choice(["csv", "json", "markdown"]), default="csv")
@click.option("--output", "-o", required=True, help="Output file path")
def export(query: str, fmt: str, output: str):
    """Export documents to CSV, JSON, or Markdown."""
    from localarchive.core.exporter import export_csv, export_json, export_markdown
    config = get_config()
    db = get_db(config)
    engine = SearchEngine(db)
    if query:
        documents = engine.search(query, limit=10000)
    else:
        documents = db.list_documents(limit=10000)
    output_path = Path(output)
    if fmt == "csv":
        export_csv(documents, output_path)
    elif fmt == "json":
        export_json(documents, output_path)
    elif fmt == "markdown":
        export_markdown(documents, output_path)
    db.close()


@main.command()
@click.argument("doc_id", type=int)
@click.argument("tags", nargs=-1, required=True)
def tag(doc_id: int, tags: tuple[str]):
    """Add tags to a document."""
    config = get_config()
    db = get_db(config)
    doc = db.get_document(doc_id)
    if not doc:
        console.print(f"[red]Document {doc_id} not found.[/red]")
        db.close()
        return
    for t in tags:
        db.add_tag(doc_id, t.lower().strip())
        console.print(f"[green]Tagged {doc['filename']} with:[/green] {t}")
    db.close()


@main.command()
@click.option("--limit", default=None, type=int, help="Max documents to process")
@click.option(
    "--extractor",
    "extractor_mode",
    type=click.Choice(["regex", "spacy", "ollama", "hybrid"]),
    default=None,
    help="Extraction strategy (defaults to config.extraction.strategy).",
)
def process(limit: int | None, extractor_mode: str | None):
    """Run OCR and field extraction on pending documents."""
    from localarchive.core.ocr_engine import get_ocr_engine, pdf_to_images, extract_text_from_pdf_native
    from localarchive.core.extractor import extract_fields
    config = get_config()
    max_docs = limit if limit is not None else config.processing.default_limit
    _validate_limit(max_docs)
    db = get_db(config)
    ocr = get_ocr_engine(config.ocr)
    pending = db.list_documents(status="pending_ocr", limit=max_docs)
    if not pending:
        console.print("[dim]No documents pending OCR.[/dim]")
        db.close()
        return
    console.print(f"Processing [bold]{len(pending)}[/bold] documents...\n")
    for doc in pending:
        filepath = Path(doc["filepath"])
        console.print(f"  -> {doc['filename']}...", end=" ")
        try:
            full_text = ""
            if doc["file_type"] == "pdf":
                native_text = extract_text_from_pdf_native(filepath)
                if len(native_text.strip()) > config.processing.pdf_native_text_min_chars:
                    full_text = native_text
                else:
                    images = pdf_to_images(filepath)
                    for img_path in images:
                        entries = ocr.extract_text(img_path)
                        full_text += " ".join(e["text"] for e in entries) + "\n"
                        if config.runtime.cleanup_temp_files:
                            img_path.unlink(missing_ok=True)
            else:
                entries = ocr.extract_text(filepath)
                full_text = " ".join(e["text"] for e in entries)
            mode = extractor_mode or config.extraction.strategy
            fields = extract_fields(full_text, mode=mode, config=config.extraction)
            field_dicts = [
                {"field_type": f.field_type, "value": f.value, "raw_match": f.raw_match, "start": f.start}
                for f in fields
            ]
            db.update_processed_document(doc["id"], ocr_text=full_text, fields=field_dicts)
            console.print(f"[green]done[/green] ({len(fields)} fields)")
        except Exception as e:
            db.update_document(doc["id"], status="error", error_message=str(e))
            console.print(f"[red]error: {e}[/red]")
            if config.runtime.fail_fast:
                break
    db.close()
    console.print("\n[green]Processing complete.[/green]")


@main.command()
@click.option("--status", type=click.Choice(["error", "processed"]), default="error")
@click.option("--since", default=None, help="Only include docs since ISO timestamp.")
@click.option("--limit", default=50, help="Max documents to include.")
@click.option("--dry-run", is_flag=True, help="Preview IDs only, no status changes.")
def reprocess(status: str, since: str | None, limit: int, dry_run: bool):
    """Move selected documents back to pending OCR."""
    _validate_limit(limit)
    config = get_config()
    db = get_db(config)
    docs = db.list_documents_for_reprocess(status=status, since=since, limit=limit)
    if not docs:
        console.print("[dim]No matching documents to reprocess.[/dim]")
        db.close()
        return
    doc_ids = [d["id"] for d in docs]
    if dry_run:
        console.print(f"[yellow]Dry run:[/yellow] would reprocess {len(doc_ids)} document(s): {doc_ids}")
        db.close()
        return
    updated = db.mark_for_reprocess(doc_ids)
    db.close()
    console.print(f"[green]Marked {updated} document(s) for reprocessing.[/green]")


@main.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--interval", type=int, default=None, help="Polling interval in seconds.")
@click.option("--once", is_flag=True, help="Run a single scan cycle and exit.")
def watch(path: Path, interval: int | None, once: bool):
    """Watch a folder and ingest newly discovered files."""
    config = get_config()
    config.ensure_dirs()
    db = get_db(config)
    ingester = Ingester(config, db)
    poll_interval = interval if interval is not None else config.watch.interval_seconds
    try:
        total = watch_directory(ingester, path=path, interval_seconds=poll_interval, run_once=once)
        console.print(f"[green]Watcher finished. New documents ingested: {total}[/green]")
    finally:
        db.close()


@main.command()
def doctor():
    """Check local dependencies and writable paths."""
    config = get_config()
    checks = []

    def _check(name: str, ok: bool, detail: str):
        checks.append((name, "PASS" if ok else "FAIL", detail))

    _check("config_path", True, str((_runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH)))
    _check("archive_dir_writable", config.archive_dir.exists() or config.archive_dir.parent.exists(), str(config.archive_dir))
    _check("db_parent_writable", config.db_path.parent.exists(), str(config.db_path.parent))
    _check("tmp_dir_writable", config.runtime.tmp_dir.exists() or config.runtime.tmp_dir.parent.exists(), str(config.runtime.tmp_dir))
    _check("fastapi_installed", importlib.util.find_spec("fastapi") is not None, "optional for `serve`")
    _check("uvicorn_installed", importlib.util.find_spec("uvicorn") is not None, "optional for `serve`")
    _check("pymupdf_installed", importlib.util.find_spec("fitz") is not None, "needed for PDF processing")
    if config.ocr.engine == "easyocr":
        _check("easyocr_installed", importlib.util.find_spec("easyocr") is not None, "required by OCR config")
    else:
        _check("paddleocr_installed", importlib.util.find_spec("paddleocr") is not None, "required by OCR config")

    table = Table(title="LocalArchive Doctor")
    table.add_column("Check", style="bold")
    table.add_column("Status")
    table.add_column("Detail")
    has_fail = False
    for name, status, detail in checks:
        if status == "FAIL":
            has_fail = True
        style = "green" if status == "PASS" else "red"
        table.add_row(name, f"[{style}]{status}[/{style}]", detail)
    console.print(table)
    if has_fail:
        raise CLIError("Doctor found failing checks.", exit_code=3)


@main.command()
@click.option("--host", default=None, help="Host (default: 127.0.0.1)")
@click.option("--port", default=None, type=int, help="Port (default: 8877)")
def serve(host: str, port: int):
    """Launch the web UI."""
    try:
        import uvicorn
    except ImportError as exc:
        raise CLIError("Missing dependency `uvicorn`. Install UI dependencies.", exit_code=3) from exc
    from localarchive.ui.app import create_app
    config = get_config()
    config.ensure_dirs()
    h = host or config.ui.host
    p = port or config.ui.port
    console.print(f"[bold]LocalArchive UI[/bold] -> http://{h}:{p}")
    create_app(config)
    from localarchive.ui.app import app as fastapi_app
    uvicorn.run(fastapi_app, host=h, port=p, log_level="warning")


if __name__ == "__main__":
    try:
        main()
    except CLIError as e:
        console.print(f"[red]{e.message}[/red]")
        raise click.exceptions.Exit(e.exit_code)
