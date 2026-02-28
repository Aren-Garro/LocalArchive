"""
File ingestion pipeline.
Handles importing documents from files or folders into the archive.
Deduplicates by file hash, copies originals to archive storage.
"""

import shutil
import time
import json
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
        doc_ids.extend(self.ingest_files(supported))
        console.print(f"[green]Ingested {len(doc_ids)} new documents.[/green]")
        return doc_ids

    def ingest_files(self, files: list[Path], scan_cache: dict[str, dict] | None = None) -> list[int]:
        doc_ids = []
        now = int(time.time())
        for filepath in files:
            if scan_cache is not None:
                try:
                    stat = filepath.stat()
                except OSError:
                    continue
                key = str(filepath.resolve())
                signature = (int(stat.st_size), int(stat.st_mtime_ns))
                cached = scan_cache.get(key) or {}
                if not isinstance(cached, dict):
                    cached = {}
                cached_sig = (int(cached.get("size", -1)), int(cached.get("mtime_ns", -1)))
                if cached_sig == signature:
                    cached["seen_at"] = now
                    scan_cache[key] = cached
                    continue
                scan_cache[key] = {"size": signature[0], "mtime_ns": signature[1], "seen_at": now}
            doc_ids.extend(self._ingest_file(filepath))
        return doc_ids


def _load_scan_manifest(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict] = {}
    for key, value in payload.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            continue
        out[key] = {
            "size": int(value.get("size", -1)),
            "mtime_ns": int(value.get("mtime_ns", -1)),
            "seen_at": int(value.get("seen_at", 0)),
        }
    return out


def _gc_scan_manifest(scan_cache: dict[str, dict], gc_days: int, now_ts: int) -> dict[str, dict]:
    cutoff = now_ts - (max(1, gc_days) * 86400)
    return {k: v for k, v in scan_cache.items() if int(v.get("seen_at", 0)) >= cutoff}


def _save_scan_manifest(path: Path, scan_cache: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(scan_cache, f, ensure_ascii=True)
    tmp.replace(path)


def watch_directory(
    ingester: Ingester,
    path: Path,
    interval_seconds: int = 5,
    run_once: bool = False,
    fast_scan: bool = True,
    manifest_path: Path | None = None,
    manifest_gc_days: int = 30,
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
    manifest = manifest_path or ingester.config.watch.manifest_path
    scan_cache: dict[str, dict] | None = _load_scan_manifest(manifest) if fast_scan else None
    console.print(f"[bold]Watching[/bold] {watch_path} every {interval_seconds}s (Ctrl+C to stop)")
    try:
        while True:
            supported = sorted(f for f in watch_path.rglob("*") if f.is_file() and is_supported(f))
            ingested = ingester.ingest_files(supported, scan_cache=scan_cache)
            total_ingested += len(ingested)
            if scan_cache is not None:
                now_ts = int(time.time())
                scan_cache = _gc_scan_manifest(scan_cache, manifest_gc_days, now_ts)
                _save_scan_manifest(manifest, scan_cache)
            if run_once:
                break
            time.sleep(max(1, interval_seconds))
    except KeyboardInterrupt:
        console.print("\n[dim]Watcher stopped.[/dim]")
    return total_ingested
