"""
File ingestion pipeline.
Handles importing documents from files or folders into the archive.
Deduplicates by file hash, copies originals to archive storage.
"""

import shutil
import time
from pathlib import Path
from rich.console import Console
from localarchive.config import Config
from localarchive.utils import file_hash, is_supported, timestamp_now
from localarchive.db.database import Database

console = Console()


class Ingester:
    """Imports documents into the LocalArchive."""

    def __init__(self, config: Config, db: Database):
        self.config = config
        self.db = db

    def ingest_path(self, path: Path) -> list[int]:
        path = Path(path).resolve()
        if path.is_file():
            return self._ingest_file(path)
        elif path.is_dir():
            return self._ingest_directory(path)
        else:
            console.print(f"[red]Path not found:[/red] {path}")
            return []

    def _ingest_file(self, filepath: Path) -> list[int]:
        if not is_supported(filepath):
            console.print(f"[yellow]Skipping unsupported file:[/yellow] {filepath.name}")
            return []

        fhash = file_hash(filepath)
        if self.db.document_exists_by_hash(fhash):
            console.print(f"[dim]Already ingested:[/dim] {filepath.name}")
            return []

        dest = self.config.archive_dir / fhash[:2] / f"{fhash}{filepath.suffix.lower()}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(filepath, dest)

        doc_id = self.db.insert_document(
            filename=filepath.name,
            filepath=str(dest),
            file_hash=fhash,
            file_type=filepath.suffix.lower().lstrip("."),
            file_size=filepath.stat().st_size,
            ingested_at=timestamp_now(),
            status="pending_ocr",
        )
        console.print(f"[green]Ingested:[/green] {filepath.name} -> ID {doc_id}")
        return [doc_id]

    def _ingest_directory(self, dirpath: Path) -> list[int]:
        doc_ids = []
        supported = sorted(f for f in dirpath.rglob("*") if f.is_file() and is_supported(f))
        console.print(f"Found [bold]{len(supported)}[/bold] supported files in {dirpath}")
        for filepath in supported:
            doc_ids.extend(self._ingest_file(filepath))
        console.print(f"[green]Ingested {len(doc_ids)} new documents.[/green]")
        return doc_ids


def watch_directory(
    ingester: Ingester,
    path: Path,
    interval_seconds: int = 5,
    run_once: bool = False,
) -> int:
    """
    Poll directory for supported files and ingest any new files.
    Returns number of newly ingested documents across all cycles.
    """
    watch_path = Path(path).resolve()
    if not watch_path.exists() or not watch_path.is_dir():
        console.print(f"[red]Watch path not found or not a directory:[/red] {watch_path}")
        return 0

    total_ingested = 0
    console.print(f"[bold]Watching[/bold] {watch_path} every {interval_seconds}s (Ctrl+C to stop)")
    try:
        while True:
            ingested = ingester.ingest_path(watch_path)
            total_ingested += len(ingested)
            if run_once:
                break
            time.sleep(max(1, interval_seconds))
    except KeyboardInterrupt:
        console.print("\n[dim]Watcher stopped.[/dim]")
    return total_ingested
