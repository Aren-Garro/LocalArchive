"""
LocalArchive CLI - main entry point.
Commands: init, ingest, search, export, tag, process, reprocess, watch, doctor, collections, timeline, audit, backup, serve
"""

import click
import importlib.util
import zipfile
import shutil
import sqlite3
import tempfile
from uuid import uuid4
from pathlib import Path
from pathlib import PurePosixPath
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


def _validate_hybrid_weights(bm25_weight: float, vector_weight: float) -> tuple[float, float]:
    if bm25_weight < 0 or vector_weight < 0:
        raise CLIError("Hybrid weights must be non-negative.", exit_code=2)
    total = bm25_weight + vector_weight
    if total <= 0:
        raise CLIError("At least one hybrid weight must be > 0.", exit_code=2)
    # Normalize to avoid user surprise for arbitrary scales.
    return bm25_weight / total, vector_weight / total


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
@click.option("--profile", type=click.Choice(["default", "research"]), default="default")
def ingest(path: str, profile: str):
    """Ingest a file or folder into the archive."""
    config = get_config()
    config.ensure_dirs()
    db = get_db(config)
    ingester = Ingester(config, db)
    doc_ids = ingester.ingest_path(Path(path))
    if profile == "research" and config.autopilot.auto_tag:
        for doc_id in doc_ids:
            db.add_tag(doc_id, "research")
    if doc_ids:
        console.print("\nRun [cyan]localarchive process[/cyan] to OCR these documents.")
    db.close()


@main.command()
@click.argument("query")
@click.option("--tag", default=None, help="Filter by tag")
@click.option("--file-type", default=None, help="Filter by file type")
@click.option("--type", "legacy_file_type", default=None, hidden=True)
@click.option("--limit", default=None, type=int, help="Max results")
@click.option("--semantic", is_flag=True, help="Enable semantic scoring when configured.")
@click.option("--bm25-weight", default=0.7, type=float, help="Hybrid BM25 weight.")
@click.option("--vector-weight", default=0.3, type=float, help="Hybrid vector weight.")
def search(
    query: str,
    tag: str,
    file_type: str | None,
    legacy_file_type: str | None,
    limit: int | None,
    semantic: bool,
    bm25_weight: float,
    vector_weight: float,
):
    """Search documents in the archive."""
    config = get_config()
    max_results = limit if limit is not None else config.ui.default_limit
    _validate_limit(max_results)
    if legacy_file_type and not file_type:
        file_type = legacy_file_type
        console.print("[yellow]`--type` is deprecated. Use `--file-type`.[/yellow]")
    db = get_db(config)
    engine = SearchEngine(db)
    if semantic:
        bm25_weight, vector_weight = _validate_hybrid_weights(bm25_weight, vector_weight)
    if semantic and config.search.enable_semantic:
        results = engine.search_hybrid(
            query,
            limit=max_results,
            tag=tag,
            file_type=file_type,
            bm25_weight=bm25_weight,
            vector_weight=vector_weight,
        )
    else:
        results = engine.search(query, limit=max_results, tag=tag, file_type=file_type)
    if semantic and not config.search.enable_semantic:
        console.print("[yellow]Semantic search is disabled in config.search.enable_semantic; using BM25 only.[/yellow]")
    if semantic:
        console.print(
            f"[dim]Hybrid search request: bm25_weight={bm25_weight:.2f}, vector_weight={vector_weight:.2f}[/dim]"
        )
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
    mode = extractor_mode or config.extraction.strategy
    run_id = db.start_processing_run(engine=config.ocr.engine, extractor=mode)
    processed_count = 0
    error_count = 0
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
            fields = extract_fields(full_text, mode=mode, config=config.extraction)
            field_dicts = [
                {"field_type": f.field_type, "value": f.value, "raw_match": f.raw_match, "start": f.start}
                for f in fields
            ]
            db.update_processed_document(doc["id"], ocr_text=full_text, fields=field_dicts)
            db.add_processing_event(run_id, "processed", message=f"{len(fields)} fields", document_id=doc["id"])
            processed_count += 1
            console.print(f"[green]done[/green] ({len(fields)} fields)")
        except Exception as e:
            db.update_document(doc["id"], status="error", error_message=str(e))
            db.add_processing_event(run_id, "error", message=str(e), document_id=doc["id"])
            error_count += 1
            console.print(f"[red]error: {e}[/red]")
            if config.runtime.fail_fast:
                break
    final_status = "completed" if error_count == 0 else "completed_with_errors"
    db.finish_processing_run(run_id, status=final_status, processed=processed_count, errors=error_count)
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


@main.group()
def collections():
    """Manage smart collections."""
    pass


@collections.command("auto-build")
@click.option("--rules", type=click.Choice(["default", "custom"]), default="default")
def collections_auto_build(rules: str):
    """Build collection assignments using configured rules."""
    if rules != "default":
        raise CLIError("Only default rules are currently implemented.", exit_code=2)
    config = get_config()
    db = get_db(config)
    summary = db.auto_build_default_collections()
    db.close()
    console.print(
        f"[green]Built collections:[/green] {summary['collections']} with {sum(summary['assignments'].values())} assignments"
    )


@collections.command("list")
def collections_list():
    """List collections and document counts."""
    config = get_config()
    db = get_db(config)
    rows = db.list_collections()
    table = Table(title="Collections")
    table.add_column("ID", style="cyan", width=6)
    table.add_column("Name", style="bold")
    table.add_column("Documents", width=10)
    for row in rows:
        table.add_row(str(row["id"]), row["name"], str(row["doc_count"]))
    console.print(table)
    db.close()


@main.command()
@click.option("--entity", type=click.Choice(["author", "topic", "journal"]), default="topic")
@click.option("--limit", default=100, type=int)
def timeline(entity: str, limit: int):
    """Show a chronological timeline by extracted entity."""
    _validate_limit(limit)
    config = get_config()
    db = get_db(config)
    rows = db.timeline_rows(entity=entity, limit=limit)
    table = Table(title=f"Timeline ({entity})")
    table.add_column("When", width=28)
    table.add_column("ID", style="cyan", width=6)
    table.add_column("Entity", width=24)
    table.add_column("Filename", style="bold")
    for row in rows:
        when = row.get("last_processed_at") or row.get("ingested_at") or ""
        table.add_row(when, str(row["id"]), str(row.get("entity_value") or "-"), row["filename"])
    console.print(table)
    db.close()


@main.command()
@click.option("--repair", is_flag=True, help="Attempt automatic repairs where possible.")
def audit(repair: bool):
    """Verify archive integrity and index consistency."""
    config = get_config()
    db = get_db(config)
    report = db.audit_verify(repair=repair)
    db.close()
    console.print(f"Checked {report['checked']} documents.")
    if not report["issues"]:
        console.print("[green]Audit passed. No issues found.[/green]")
        return
    table = Table(title="Audit Issues")
    table.add_column("Doc ID", width=8)
    table.add_column("Issue")
    table.add_column("Path/Detail")
    for issue in report["issues"]:
        table.add_row(str(issue.get("id") or "-"), issue["issue"], issue["path"])
    console.print(table)
    raise CLIError("Audit found issues.", exit_code=4)


@main.group()
def backup():
    """Create or restore local backups."""
    pass


@backup.command("create")
@click.option("--path", "backup_path", type=click.Path(dir_okay=False, path_type=Path), required=True)
def backup_create(backup_path: Path):
    """Create a backup archive including DB and config."""
    config = get_config()
    config.ensure_dirs()
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path = _runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH
    snapshot_path = config.runtime.tmp_dir / f"archive-snapshot-{uuid4().hex}.db"
    if config.db_path.exists():
        with sqlite3.connect(str(config.db_path)) as src, sqlite3.connect(str(snapshot_path)) as dst:
            src.backup(dst)
    with zipfile.ZipFile(backup_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if snapshot_path.exists():
            zf.write(snapshot_path, arcname="archive.db")
        if cfg_path.exists():
            zf.write(cfg_path, arcname="config.toml")
        if config.archive_dir.exists():
            for p in config.archive_dir.rglob("*"):
                if p.is_file():
                    zf.write(p, arcname=str(Path("archive_data") / p.relative_to(config.archive_dir)))
    try:
        snapshot_path.unlink(missing_ok=True)
    except PermissionError:
        # On Windows, temporary file handles can linger briefly after zip write.
        pass
    console.print(f"[green]Backup created:[/green] {backup_path}")


@backup.command("restore")
@click.option("--path", "backup_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
def backup_restore(backup_path: Path):
    """Restore DB and archive data from a backup archive."""
    config = get_config()
    config.ensure_dirs()
    cfg_path = _runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH
    staging_dir = Path(tempfile.mkdtemp(prefix="restore-", dir=str(config.runtime.tmp_dir)))
    rollback_dir = Path(tempfile.mkdtemp(prefix="rollback-", dir=str(config.runtime.tmp_dir)))
    moved_pairs: list[tuple[Path, Path]] = []
    created_paths: list[Path] = []
    try:
        with zipfile.ZipFile(backup_path, "r") as zf:
            members = set(zf.namelist())
            for name in members:
                posix = PurePosixPath(name)
                if posix.is_absolute() or ".." in posix.parts:
                    raise CLIError(f"Unsafe backup entry path: {name}", exit_code=2)
            if "archive.db" in members:
                with zf.open("archive.db", "r") as src, open(staging_dir / "archive.db", "wb") as out:
                    out.write(src.read())
            if "config.toml" in members:
                with zf.open("config.toml", "r") as src, open(staging_dir / "config.toml", "wb") as out:
                    out.write(src.read())
            for name in members:
                if not name.startswith("archive_data/") or name.endswith("/"):
                    continue
                rel = Path(*PurePosixPath(name).parts[1:])
                staged = staging_dir / "archive_data" / rel
                staged.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(name, "r") as src, open(staged, "wb") as out:
                    out.write(src.read())

        staged_db = staging_dir / "archive.db"
        if staged_db.exists():
            config.db_path.parent.mkdir(parents=True, exist_ok=True)
            if config.db_path.exists():
                backup_existing = rollback_dir / "archive.db.old"
                shutil.move(str(config.db_path), str(backup_existing))
                moved_pairs.append((backup_existing, config.db_path))
            shutil.move(str(staged_db), str(config.db_path))

        staged_cfg = staging_dir / "config.toml"
        if staged_cfg.exists():
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            if cfg_path.exists():
                backup_existing = rollback_dir / "config.toml.old"
                shutil.move(str(cfg_path), str(backup_existing))
                moved_pairs.append((backup_existing, cfg_path))
            shutil.move(str(staged_cfg), str(cfg_path))

        staged_archive_root = staging_dir / "archive_data"
        if staged_archive_root.exists():
            for staged in staged_archive_root.rglob("*"):
                if not staged.is_file():
                    continue
                rel = staged.relative_to(staged_archive_root)
                dest = config.archive_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    backup_existing = rollback_dir / "archive_data" / rel
                    backup_existing.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(dest), str(backup_existing))
                    moved_pairs.append((backup_existing, dest))
                else:
                    created_paths.append(dest)
                shutil.move(str(staged), str(dest))

        console.print(f"[green]Backup restored from:[/green] {backup_path}")
    except Exception as exc:
        for created in created_paths:
            try:
                if created.exists():
                    created.unlink()
            except Exception:
                pass
        for src, dest in reversed(moved_pairs):
            try:
                if src.exists():
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if dest.exists():
                        if dest.is_file():
                            dest.unlink()
                    shutil.move(str(src), str(dest))
            except Exception:
                pass
        if isinstance(exc, CLIError):
            raise
        raise CLIError(f"Backup restore failed: {exc}", exit_code=4) from exc
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)
        shutil.rmtree(rollback_dir, ignore_errors=True)


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
