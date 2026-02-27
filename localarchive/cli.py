"""
LocalArchive CLI - main entry point.
Commands: init, ingest, search, export, tag, process, watch, serve
"""

import click
from pathlib import Path
from rich.console import Console
from rich.table import Table
from localarchive.config import Config
from localarchive.db.database import Database
from localarchive.db.search import SearchEngine
from localarchive.core.ingester import Ingester, watch_directory

console = Console()


def get_config() -> Config:
    return Config.load()


def get_db(config: Config) -> Database:
    db = Database(config.db_path)
    db.initialize()
    return db


@click.group()
@click.version_option(version="0.1.0")
def main():
    """LocalArchive - Your private, offline document library."""
    pass


@main.command()
def init():
    """Initialize LocalArchive database and directories."""
    config = get_config()
    config.ensure_dirs()
    config.save()
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
@click.option("--type", "file_type", default=None, help="Filter by file type")
@click.option("--limit", default=20, help="Max results")
def search(query: str, tag: str, file_type: str, limit: int):
    """Search documents in the archive."""
    config = get_config()
    db = get_db(config)
    engine = SearchEngine(db)
    results = engine.search(query, limit=limit, tag=tag, file_type=file_type)
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
@click.option("--limit", default=50, help="Max documents to process")
@click.option(
    "--extractor",
    "extractor_mode",
    type=click.Choice(["regex", "spacy", "ollama", "hybrid"]),
    default=None,
    help="Extraction strategy (defaults to config.extraction.strategy).",
)
def process(limit: int, extractor_mode: str | None):
    """Run OCR and field extraction on pending documents."""
    from localarchive.core.ocr_engine import get_ocr_engine, pdf_to_images, extract_text_from_pdf_native
    from localarchive.core.extractor import extract_fields
    config = get_config()
    db = get_db(config)
    ocr = get_ocr_engine(config.ocr)
    pending = db.list_documents(status="pending_ocr", limit=limit)
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
                if len(native_text.strip()) > 50:
                    full_text = native_text
                else:
                    images = pdf_to_images(filepath)
                    for img_path in images:
                        entries = ocr.extract_text(img_path)
                        full_text += " ".join(e["text"] for e in entries) + "\n"
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
            db.update_document(doc["id"], ocr_text=full_text, status="processed")
            if field_dicts:
                db.insert_fields(doc["id"], field_dicts)
            console.print(f"[green]done[/green] ({len(fields)} fields)")
        except Exception as e:
            db.update_document(doc["id"], status="error")
            console.print(f"[red]error: {e}[/red]")
    db.close()
    console.print("\n[green]Processing complete.[/green]")


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
@click.option("--host", default=None, help="Host (default: 127.0.0.1)")
@click.option("--port", default=None, type=int, help="Port (default: 8877)")
def serve(host: str, port: int):
    """Launch the web UI."""
    import uvicorn
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
    main()
