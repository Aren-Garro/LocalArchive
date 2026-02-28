"""
LocalArchive CLI - main entry point.
Commands: init, ingest, search, export, tag, process, classify, reprocess, watch,
doctor, collections, timeline, audit, verify, backup, serve
"""

import concurrent.futures
import importlib.util
import json
import shutil
import sqlite3
import tempfile
import threading
import time
import zipfile
from pathlib import Path, PurePosixPath
from uuid import uuid4

import click
from rich.console import Console
from rich.table import Table

from localarchive.config import DEFAULT_CONFIG_PATH, Config
from localarchive.core.ingester import Ingester, watch_directory
from localarchive.db.database import Database
from localarchive.db.search import SearchEngine
from localarchive.utils import file_hash

console = Console()
BACKUP_RESTORE_MAX_MEMBER_BYTES = 256 * 1024 * 1024
BACKUP_RESTORE_MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
BACKUP_RESTORE_MAX_ARCHIVE_FILES = 50000


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
@click.option(
    "--config", "config_path", type=click.Path(dir_okay=False, path_type=Path), default=None
)
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


def _run_integrity_check_if_enabled(config: Config, db: Database, context: str) -> None:
    if not config.reliability.integrity_check_on_startup:
        return
    report = db.audit_verify(repair=False, full_check=False)
    if not report["issues"]:
        console.print(f"[dim]{context}: integrity check passed ({report['checked']} docs).[/dim]")
        return
    sample = ", ".join(issue["issue"] for issue in report["issues"][:3])
    console.print(
        f"[yellow]{context}: integrity check found {len(report['issues'])} issue(s) "
        f"across {report['checked']} docs ({sample}).[/yellow]"
    )


def _validate_hybrid_weights(bm25_weight: float, vector_weight: float) -> tuple[float, float]:
    if bm25_weight < 0 or vector_weight < 0:
        raise CLIError("Hybrid weights must be non-negative.", exit_code=2)
    total = bm25_weight + vector_weight
    if total <= 0:
        raise CLIError("At least one hybrid weight must be > 0.", exit_code=2)
    # Normalize to avoid user surprise for arbitrary scales.
    return bm25_weight / total, vector_weight / total


def _validate_threshold(name: str, value: float) -> None:
    if not 0 <= value <= 1:
        raise CLIError(f"{name} must be between 0 and 1.", exit_code=2)


def _emit_json(payload: dict | list) -> None:
    click.echo(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def _copy_zip_member_limited(
    zf: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    dst: Path,
    limits: dict[str, int],
) -> int:
    file_size = int(member.file_size)
    if file_size < 0 or file_size > BACKUP_RESTORE_MAX_MEMBER_BYTES:
        raise CLIError(
            f"Backup restore failed: entry too large ({member.filename}, {file_size} bytes).",
            exit_code=4,
        )
    next_total = int(limits["total"]) + file_size
    if next_total > BACKUP_RESTORE_MAX_TOTAL_BYTES:
        raise CLIError(
            "Backup restore failed: extracted payload exceeds safety limit.", exit_code=4
        )
    dst.parent.mkdir(parents=True, exist_ok=True)
    copied = 0
    with zf.open(member, "r") as src, open(dst, "wb") as out:
        while True:
            chunk = src.read(1024 * 1024)
            if not chunk:
                break
            copied += len(chunk)
            if copied > BACKUP_RESTORE_MAX_MEMBER_BYTES:
                raise CLIError(
                    f"Backup restore failed: entry too large while reading ({member.filename}).",
                    exit_code=4,
                )
            out.write(chunk)
    limits["total"] = int(limits["total"]) + copied
    return copied


def _issue_breakdown(issues: list[dict]) -> dict:
    breakdown: dict[str, int] = {}
    for issue in issues:
        key = str(issue.get("issue", "unknown"))
        breakdown[key] = breakdown.get(key, 0) + 1
    return breakdown


def _issue_recommendations(breakdown: dict[str, int]) -> list[str]:
    recs = []
    if breakdown.get("missing_file"):
        recs.append("Run `localarchive ingest <path>` to re-ingest missing documents.")
    if breakdown.get("hash_mismatch") or breakdown.get("hash_error"):
        recs.append("Run `localarchive audit --repair` and inspect source files for manual edits.")
    if breakdown.get("fts_mismatch"):
        recs.append("Run `localarchive audit --repair` to rebuild the FTS index.")
    if not recs:
        recs.append("No action required.")
    return recs


def _classify_document(doc: dict, fields: list[dict]) -> tuple[str, float, list[str]]:
    text = f"{doc.get('filename', '')} {doc.get('ocr_text', '')}".lower()
    field_types = {str(f.get("field_type", "")).lower() for f in fields}
    scores = {"invoice": 0.0, "receipt": 0.0, "medical": 0.0, "research": 0.0}
    reasons: list[str] = []
    keyword_weights = {
        "invoice": {"invoice": 2.0, "due": 1.0, "balance": 1.0, "bill to": 1.0},
        "receipt": {"receipt": 2.0, "subtotal": 1.0, "tax": 1.0, "cashier": 1.0},
        "medical": {"patient": 2.0, "clinic": 1.0, "hospital": 1.0, "diagnosis": 2.0, "rx": 1.0},
        "research": {"abstract": 2.0, "references": 1.5, "journal": 1.0, "method": 0.5},
    }
    for label, mapping in keyword_weights.items():
        for token, weight in mapping.items():
            if token in text:
                scores[label] += weight
                reasons.append(f"{label}:{token}")

    if "doi" in field_types or "arxiv" in field_types:
        scores["research"] += 2.0
        reasons.append("research:doi/arxiv")
    if "amount" in field_types:
        scores["invoice"] += 0.8
        scores["receipt"] += 0.8
        reasons.append("invoice/receipt:amount")
    if "date" in field_types and "amount" in field_types:
        scores["invoice"] += 0.5
        scores["receipt"] += 0.5
    if "entity_person" in field_types and "medical" in text:
        scores["medical"] += 0.5

    best_label = max(scores, key=scores.get)
    best_score = scores[best_label]
    if best_score < 1.5:
        return "other", 0.4, reasons[:4]
    confidence = min(0.95, 0.5 + (best_score / 8.0))
    return best_label, round(confidence, 2), reasons[:4]


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
    _run_integrity_check_if_enabled(config, db, "init")
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
@click.option("--fuzzy", is_flag=True, help="Enable fuzzy OCR-tolerant fallback search.")
@click.option("--fuzzy-threshold", default=None, type=float, help="Fuzzy match threshold (0-1).")
@click.option(
    "--fuzzy-max-candidates", default=None, type=int, help="Max fuzzy candidates to score."
)
@click.option("--explain-ranking", is_flag=True, help="Show ranking diagnostics for each result.")
@click.option("--json", "as_json", is_flag=True, help="Emit results as JSON.")
def search(
    query: str,
    tag: str,
    file_type: str | None,
    legacy_file_type: str | None,
    limit: int | None,
    semantic: bool,
    bm25_weight: float,
    vector_weight: float,
    fuzzy: bool,
    fuzzy_threshold: float | None,
    fuzzy_max_candidates: int | None,
    explain_ranking: bool,
    as_json: bool,
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
    fuzzy_enabled = fuzzy or config.search.enable_fuzzy
    if fuzzy_enabled:
        threshold = (
            fuzzy_threshold if fuzzy_threshold is not None else config.search.fuzzy_threshold
        )
        max_candidates = (
            fuzzy_max_candidates
            if fuzzy_max_candidates is not None
            else config.search.fuzzy_max_candidates
        )
        _validate_threshold("fuzzy-threshold", threshold)
        _validate_limit(max_candidates)
        fuzzy_results = engine.search_fuzzy(
            query,
            limit=max_results,
            tag=tag,
            file_type=file_type,
            threshold=threshold,
            max_candidates=max_candidates,
        )
        if results:
            seen = {int(doc["id"]) for doc in results}
            for doc in fuzzy_results:
                if int(doc["id"]) in seen:
                    continue
                results.append(doc)
                seen.add(int(doc["id"]))
                if len(results) >= max_results:
                    break
        else:
            results = fuzzy_results
    if semantic and not config.search.enable_semantic:
        console.print(
            "[yellow]Semantic search is disabled in config.search.enable_semantic; using BM25 only.[/yellow]"
        )
    if semantic:
        console.print(
            f"[dim]Hybrid search request: bm25_weight={bm25_weight:.2f}, vector_weight={vector_weight:.2f}[/dim]"
        )
    if fuzzy_enabled:
        console.print(
            f"[dim]Fuzzy search enabled: threshold={threshold:.2f} candidates={max_candidates}[/dim]"
        )
    if not results:
        if as_json:
            _emit_json(
                {
                    "query": query,
                    "count": 0,
                    "semantic": bool(semantic and config.search.enable_semantic),
                    "fuzzy": bool(fuzzy_enabled),
                    "results": [],
                }
            )
        else:
            console.print("[yellow]No results found.[/yellow]")
            console.print(
                '[dim]Hint: try `localarchive search "<term>" --fuzzy` or broaden filters.[/dim]'
            )
        db.close()
        return
    if as_json:
        payload = {
            "query": query,
            "count": len(results),
            "semantic": bool(semantic and config.search.enable_semantic),
            "fuzzy": bool(fuzzy_enabled),
            "results": [],
        }
        for doc in results:
            item = {
                "id": int(doc["id"]),
                "filename": doc["filename"],
                "file_type": doc.get("file_type"),
                "ingested_at": doc.get("ingested_at"),
                "preview": (doc.get("ocr_text") or "")[:120],
            }
            if "rank" in doc:
                item["rank"] = doc["rank"]
            if "hybrid_score" in doc:
                item["hybrid_score"] = doc["hybrid_score"]
            if "fuzzy_score" in doc:
                item["fuzzy_score"] = doc["fuzzy_score"]
            payload["results"].append(item)
        _emit_json(payload)
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
        table.add_row(
            str(doc["id"]),
            doc["filename"],
            doc.get("file_type", "?"),
            doc.get("ingested_at", ""),
            preview,
        )
    console.print(table)
    if explain_ranking:
        rank_table = Table(title="Ranking Explanation")
        rank_table.add_column("ID", style="cyan", width=6)
        rank_table.add_column("rank", width=12)
        rank_table.add_column("hybrid", width=12)
        rank_table.add_column("fuzzy", width=12)
        for doc in results:
            rank_table.add_row(
                str(doc["id"]),
                str(round(float(doc.get("rank", 0.0)), 6)) if "rank" in doc else "-",
                str(doc.get("hybrid_score", "-")),
                str(doc.get("fuzzy_score", "-")),
            )
        console.print(rank_table)
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
    "--workers", type=int, default=None, help="Worker count (defaults to runtime.max_workers)."
)
@click.option(
    "--commit-batch-size",
    type=int,
    default=None,
    help="DB commit batch size (defaults to processing.commit_batch_size).",
)
@click.option(
    "--checkpoint-every",
    type=int,
    default=None,
    help="Emit progress every N documents (defaults to reliability.checkpoint_batch_size).",
)
@click.option(
    "--extractor",
    "extractor_mode",
    type=click.Choice(["regex", "spacy", "ollama", "hybrid"]),
    default=None,
    help="Extraction strategy (defaults to config.extraction.strategy).",
)
@click.option("--dry-run", is_flag=True, help="Preview pending IDs and exit without processing.")
@click.option("--max-errors", type=int, default=None, help="Abort run after this many errors.")
@click.option("--resume", is_flag=True, help="Resume from latest processing checkpoint.")
@click.option(
    "--from-run", type=int, default=None, help="Resume from a specific processing run ID."
)
@click.option("--json", "as_json", is_flag=True, help="Emit process run summary as JSON.")
def process(
    limit: int | None,
    workers: int | None,
    commit_batch_size: int | None,
    checkpoint_every: int | None,
    extractor_mode: str | None,
    dry_run: bool,
    max_errors: int | None,
    resume: bool,
    from_run: int | None,
    as_json: bool,
):
    """Run OCR and field extraction on pending documents."""
    from localarchive.core.extractor import extract_fields
    from localarchive.core.ocr_engine import (
        extract_text_from_pdf_native,
        get_ocr_engine,
        pdf_to_images,
    )

    config = get_config()
    max_docs = limit if limit is not None else config.processing.default_limit
    _validate_limit(max_docs)
    worker_count = workers if workers is not None else config.runtime.max_workers
    _validate_limit(worker_count)
    commit_size = (
        commit_batch_size if commit_batch_size is not None else config.processing.commit_batch_size
    )
    _validate_limit(commit_size)
    checkpoint_size = (
        checkpoint_every
        if checkpoint_every is not None
        else config.reliability.checkpoint_batch_size
    )
    _validate_limit(checkpoint_size)
    max_error_budget = (
        max_errors if max_errors is not None else config.processing.max_errors_per_run
    )
    _validate_limit(max_error_budget)
    db = get_db(config)
    _run_integrity_check_if_enabled(config, db, "process")
    resume_run = None
    after_doc_id = 0
    if from_run is not None:
        resume_run = db.get_processing_run(from_run)
        if not resume_run:
            db.close()
            raise CLIError(f"Processing run {from_run} not found.", exit_code=2)
    elif resume:
        resume_run = db.latest_processing_run()
    if resume_run:
        after_doc_id = int(resume_run.get("checkpoint_doc_id") or 0)
        console.print(
            f"[dim]Resuming from run {resume_run.get('id')} with checkpoint_doc_id={after_doc_id}[/dim]"
        )
    elif resume or from_run is not None:
        console.print(
            "[yellow]No checkpointed run found; starting from earliest pending document.[/yellow]"
        )
    pending = db.list_documents_for_processing(limit=max_docs, after_doc_id=after_doc_id)
    if not pending:
        if as_json:
            _emit_json(
                {
                    "run_id": None,
                    "status": "noop",
                    "processed": 0,
                    "errors": 0,
                    "aborted_reason": "",
                    "checkpoint_doc_id": after_doc_id,
                    "total_candidates": 0,
                }
            )
        else:
            console.print("[dim]No documents pending OCR for the selected scope.[/dim]")
            console.print("[dim]Hint: run `localarchive ingest <file_or_folder>` first.[/dim]")
        db.close()
        return
    if dry_run:
        doc_ids = [int(doc["id"]) for doc in pending]
        if as_json:
            _emit_json(
                {
                    "dry_run": True,
                    "count": len(doc_ids),
                    "doc_ids": doc_ids,
                    "resumed_from_run": int(resume_run.get("id")) if resume_run else None,
                    "start_after_doc_id": after_doc_id,
                }
            )
        else:
            console.print(
                f"[yellow]Dry run:[/yellow] would process {len(doc_ids)} document(s): {doc_ids}"
            )
        db.close()
        return
    mode = extractor_mode or config.extraction.strategy
    run_id = db.start_processing_run(engine=config.ocr.engine, extractor=mode)
    processed_count = 0
    error_count = 0
    completed_count = 0
    max_completed_doc_id = 0
    success_buffer: list[dict] = []
    error_buffer: list[dict] = []
    event_buffer: list[dict] = []
    attempts_by_doc = {int(doc["id"]): int(doc.get("processing_attempts", 0)) for doc in pending}
    flush_ms = max(10, config.processing.writer_flush_ms)
    checkpoint_interval = max(1, config.processing.resume_checkpoint_interval)
    last_flush_at = time.monotonic()
    aborted_reason = ""
    console.print(f"Processing [bold]{len(pending)}[/bold] documents...\n")
    if config.runtime.fail_fast and worker_count > 1:
        console.print(
            "[yellow]Fail-fast enabled; forcing single-worker processing for deterministic stop behavior.[/yellow]"
        )
        worker_count = 1
    thread_local = threading.local()

    def _get_worker_ocr():
        engine = getattr(thread_local, "ocr_engine", None)
        if engine is None:
            engine = get_ocr_engine(config.ocr)
            thread_local.ocr_engine = engine
        return engine

    def _process_document(doc: dict) -> dict:
        filepath = Path(doc["filepath"])
        temp_images: list[Path] = []
        try:
            full_text = ""
            if doc["file_type"] == "pdf":
                native_text = extract_text_from_pdf_native(filepath)
                if len(native_text.strip()) > config.processing.pdf_native_text_min_chars:
                    full_text = native_text
                else:
                    temp_images = pdf_to_images(filepath, tmp_dir=config.runtime.tmp_dir)
                    ocr_engine = _get_worker_ocr()
                    for img_path in temp_images:
                        entries = ocr_engine.extract_text(img_path)
                        full_text += " ".join(e["text"] for e in entries) + "\n"
            else:
                ocr_engine = _get_worker_ocr()
                entries = ocr_engine.extract_text(filepath)
                full_text = " ".join(e["text"] for e in entries)
            fields = extract_fields(full_text, mode=mode, config=config.extraction)
            field_dicts = [
                {
                    "field_type": f.field_type,
                    "value": f.value,
                    "raw_match": f.raw_match,
                    "start": f.start,
                }
                for f in fields
            ]
            return {
                "doc_id": doc["id"],
                "filename": doc["filename"],
                "full_text": full_text,
                "fields": field_dicts,
            }
        finally:
            if config.runtime.cleanup_temp_files:
                for img_path in temp_images:
                    img_path.unlink(missing_ok=True)

    def _flush_buffers(force: bool = False):
        nonlocal last_flush_at
        buffer_size = len(success_buffer) + len(error_buffer)
        elapsed_ms = (time.monotonic() - last_flush_at) * 1000
        if not force and buffer_size < commit_size and elapsed_ms < flush_ms:
            return
        if success_buffer:
            db.update_processed_documents_batch(success_buffer)
            success_buffer.clear()
        if error_buffer:
            db.record_processing_errors_batch(
                error_buffer, max_retries=config.reliability.max_retries
            )
            error_buffer.clear()
        if event_buffer:
            db.add_processing_events_batch(event_buffer)
            event_buffer.clear()
        last_flush_at = time.monotonic()

    def _handle_result(result: dict):
        nonlocal processed_count, error_count, completed_count, max_completed_doc_id
        doc_id = result["doc_id"]
        max_completed_doc_id = max(max_completed_doc_id, int(doc_id))
        filename = result["filename"]
        error = result.get("error")
        if error:
            attempts_by_doc[doc_id] = attempts_by_doc.get(doc_id, 0) + 1
            terminal = attempts_by_doc[doc_id] >= config.reliability.max_retries
            error_buffer.append({"doc_id": doc_id, "error": str(error)})
            event_buffer.append(
                {
                    "run_id": run_id,
                    "document_id": doc_id,
                    "event_type": "error",
                    "message": f"{error} (attempt {attempts_by_doc[doc_id]}/{config.reliability.max_retries})",
                }
            )
            error_count += 1
            if terminal:
                console.print(
                    f"  -> {filename}... [red]error[/red]: {error} "
                    f"(attempt {attempts_by_doc[doc_id]}/{config.reliability.max_retries}, max retries exceeded)"
                )
            else:
                console.print(
                    f"  -> {filename}... [red]error[/red]: {error} "
                    f"(attempt {attempts_by_doc[doc_id]}/{config.reliability.max_retries})"
                )
        else:
            success_buffer.append(result)
            event_buffer.append(
                {
                    "run_id": run_id,
                    "document_id": doc_id,
                    "event_type": "processed",
                    "message": f"{len(result['fields'])} fields",
                }
            )
            processed_count += 1
            console.print(
                f"  -> {filename}... [green]done[/green] ({len(result['fields'])} fields)"
            )
        completed_count += 1
        if completed_count % checkpoint_size == 0:
            console.print(
                f"[dim]Progress checkpoint: {completed_count}/{len(pending)} "
                f"(processed={processed_count}, errors={error_count})[/dim]"
            )
        _flush_buffers(force=False)
        if completed_count % checkpoint_interval == 0:
            db.update_processing_checkpoint(run_id, checkpoint_doc_id=max_completed_doc_id)
        if error_count >= max_error_budget:
            raise RuntimeError(f"max_errors_exceeded:{max_error_budget}")

    if worker_count == 1:
        for doc in pending:
            try:
                result = _process_document(doc)
            except Exception as e:
                result = {"doc_id": doc["id"], "filename": doc["filename"], "error": e}
            try:
                _handle_result(result)
            except RuntimeError as e:
                aborted_reason = str(e)
                break
            if config.runtime.fail_fast and result.get("error"):
                aborted_reason = "fail_fast_triggered"
                break
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {executor.submit(_process_document, doc): doc for doc in pending}
            for future in concurrent.futures.as_completed(futures):
                doc = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {"doc_id": doc["id"], "filename": doc["filename"], "error": e}
                try:
                    _handle_result(result)
                except RuntimeError as e:
                    aborted_reason = str(e)
                    for pending_future in futures:
                        if not pending_future.done():
                            pending_future.cancel()
                    break
    _flush_buffers(force=True)
    if completed_count > 0:
        db.update_processing_checkpoint(run_id, checkpoint_doc_id=max_completed_doc_id)
    if aborted_reason:
        final_status = "aborted"
    else:
        final_status = "completed" if error_count == 0 else "completed_with_errors"
    db.finish_processing_run(
        run_id,
        status=final_status,
        processed=processed_count,
        errors=error_count,
        aborted_reason=aborted_reason,
    )
    run_meta = db.get_processing_run(run_id) or {}
    db.close()
    if as_json:
        _emit_json(
            {
                "run_id": run_id,
                "status": final_status,
                "processed": processed_count,
                "errors": error_count,
                "aborted_reason": aborted_reason,
                "checkpoint_doc_id": int(run_meta.get("checkpoint_doc_id", 0) or 0),
                "total_candidates": len(pending),
                "resumed_from_run": int(resume_run.get("id")) if resume_run else None,
                "start_after_doc_id": after_doc_id,
            }
        )
    elif aborted_reason:
        console.print(f"\n[yellow]Processing aborted:[/yellow] {aborted_reason}")
    else:
        console.print("\n[green]Processing complete.[/green]")


@main.command()
@click.option("--limit", default=200, type=int, help="Max processed documents to classify.")
@click.option(
    "--retag", is_flag=True, help="Replace existing category tags with new classification tag."
)
@click.option("--explain", is_flag=True, help="Show rule hits that drove classification.")
def classify(limit: int, retag: bool, explain: bool):
    """Auto-classify processed documents and apply category tags."""
    _validate_limit(limit)
    config = get_config()
    db = get_db(config)
    docs = db.list_documents(status="processed", limit=limit)
    if not docs:
        console.print("[dim]No processed documents available for classification.[/dim]")
        console.print("[dim]Hint: run `localarchive process` before classification.[/dim]")
        db.close()
        return
    category_tags = {"invoice", "receipt", "medical", "research", "other"}
    threshold = config.autopilot.confidence_threshold
    updated = 0
    skipped = 0
    table = Table(title="Classification Results")
    table.add_column("ID", style="cyan", width=6)
    table.add_column("Filename", style="bold")
    table.add_column("Label", width=10)
    table.add_column("Confidence", width=10)
    table.add_column("Action", width=12)
    if explain:
        table.add_column("Why", max_width=40)
    for doc in docs:
        fields = db.get_fields(int(doc["id"]))
        label, confidence, reasons = _classify_document(doc, fields)
        action = "skipped"
        if label != "other" and confidence >= threshold and config.autopilot.auto_tag:
            current_tags = set(db.get_tags(int(doc["id"])))
            if retag:
                next_tags = sorted((current_tags - category_tags) | {label})
                db.set_tags(int(doc["id"]), next_tags)
                action = "retagged"
                updated += 1
            else:
                if label not in current_tags:
                    db.add_tag(int(doc["id"]), label)
                    action = "tagged"
                    updated += 1
                else:
                    action = "already"
                    skipped += 1
        else:
            skipped += 1
        row = [str(doc["id"]), str(doc.get("filename", "")), label, f"{confidence:.2f}", action]
        if explain:
            row.append(", ".join(reasons) if reasons else "-")
        table.add_row(*row)
    console.print(table)
    console.print(f"[green]Updated:[/green] {updated}  [dim]Skipped:[/dim] {skipped}")
    db.close()


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
        console.print("[dim]Hint: run `localarchive audit` to inspect archive health.[/dim]")
        db.close()
        return
    doc_ids = [d["id"] for d in docs]
    if dry_run:
        console.print(
            f"[yellow]Dry run:[/yellow] would reprocess {len(doc_ids)} document(s): {doc_ids}"
        )
        db.close()
        return
    updated = db.mark_for_reprocess(doc_ids)
    db.close()
    console.print(f"[green]Marked {updated} document(s) for reprocessing.[/green]")


@main.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--interval", type=int, default=None, help="Polling interval in seconds.")
@click.option("--once", is_flag=True, help="Run a single scan cycle and exit.")
@click.option(
    "--fast-scan/--no-fast-scan", default=True, help="Skip unchanged files between scan cycles."
)
def watch(path: Path, interval: int | None, once: bool, fast_scan: bool):
    """Watch a folder and ingest newly discovered files."""
    config = get_config()
    config.ensure_dirs()
    db = get_db(config)
    ingester = Ingester(config, db)
    poll_interval = interval if interval is not None else config.watch.interval_seconds
    try:
        total = watch_directory(
            ingester,
            path=path,
            interval_seconds=poll_interval,
            run_once=once,
            fast_scan=fast_scan,
            manifest_path=config.watch.manifest_path,
            manifest_gc_days=config.watch.manifest_gc_days,
        )
        console.print(f"[green]Watcher finished. New documents ingested: {total}[/green]")
    finally:
        db.close()


@main.command()
@click.option("--json", "as_json", is_flag=True, help="Emit doctor report as JSON.")
def doctor(as_json: bool):
    """Check local dependencies and writable paths."""
    config = get_config()
    checks = []

    def _check(name: str, ok: bool, detail: str):
        checks.append((name, "PASS" if ok else "FAIL", detail))

    _check("config_path", True, str(_runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH))
    _check(
        "archive_dir_writable",
        config.archive_dir.exists() or config.archive_dir.parent.exists(),
        str(config.archive_dir),
    )
    _check("db_parent_writable", config.db_path.parent.exists(), str(config.db_path.parent))
    _check(
        "tmp_dir_writable",
        config.runtime.tmp_dir.exists() or config.runtime.tmp_dir.parent.exists(),
        str(config.runtime.tmp_dir),
    )
    _check(
        "fastapi_installed", importlib.util.find_spec("fastapi") is not None, "optional for `serve`"
    )
    _check(
        "uvicorn_installed", importlib.util.find_spec("uvicorn") is not None, "optional for `serve`"
    )
    _check(
        "pymupdf_installed",
        importlib.util.find_spec("fitz") is not None,
        "needed for PDF processing",
    )
    if config.ocr.engine == "easyocr":
        _check(
            "easyocr_installed",
            importlib.util.find_spec("easyocr") is not None,
            "required by OCR config",
        )
    else:
        _check(
            "paddleocr_installed",
            importlib.util.find_spec("paddleocr") is not None,
            "required by OCR config",
        )

    if as_json:
        payload = {
            "checks": [{"name": n, "status": s, "detail": d} for n, s, d in checks],
            "has_fail": any(s == "FAIL" for _, s, _ in checks),
        }
        _emit_json(payload)
        if payload["has_fail"]:
            raise CLIError("Doctor found failing checks.", exit_code=3)
        return

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
    assignment_total = sum(summary["assignments"].values())
    console.print(
        f"[green]Built collections:[/green] {summary['collections']} with {assignment_total} assignments"
    )


@collections.command("list")
@click.option("--json", "as_json", is_flag=True, help="Emit collections as JSON.")
def collections_list(as_json: bool):
    """List collections and document counts."""
    config = get_config()
    db = get_db(config)
    rows = db.list_collections()
    if as_json:
        _emit_json({"count": len(rows), "collections": rows})
        db.close()
        return
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
@click.option("--json", "as_json", is_flag=True, help="Emit timeline as JSON.")
def timeline(entity: str, limit: int, as_json: bool):
    """Show a chronological timeline by extracted entity."""
    _validate_limit(limit)
    config = get_config()
    db = get_db(config)
    rows = db.timeline_rows(entity=entity, limit=limit)
    if as_json:
        _emit_json({"entity": entity, "count": len(rows), "rows": rows})
        db.close()
        return
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
    report = db.audit_verify(repair=repair, full_check=True)
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


@main.command()
@click.option("--full", "full_verify", is_flag=True, help="Run full integrity verification.")
@click.option("--json", "as_json", is_flag=True, help="Emit verification report as JSON.")
def verify(full_verify: bool, as_json: bool):
    """Run archive verification with actionable output."""
    config = get_config()
    db = get_db(config)
    report = db.audit_verify(repair=False, full_check=full_verify)
    db.close()
    level = "full" if full_verify else "quick"
    breakdown = _issue_breakdown(report["issues"])
    recommendations = _issue_recommendations(breakdown)
    if as_json:
        _emit_json(
            {
                "mode": level,
                **report,
                "issue_breakdown": breakdown,
                "recommendations": recommendations,
            }
        )
        if report["issues"]:
            raise CLIError("Verify found issues.", exit_code=4)
        return
    console.print(f"Verification mode: {level}")
    console.print(f"Checked {report['checked']} documents.")
    if not report["issues"]:
        console.print("[green]Verify passed. No issues found.[/green]")
        return
    table = Table(title="Verification Issues")
    table.add_column("Doc ID", width=8)
    table.add_column("Issue")
    table.add_column("Path/Detail")
    for issue in report["issues"]:
        table.add_row(str(issue.get("id") or "-"), issue["issue"], issue["path"])
    console.print(table)
    if breakdown:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(breakdown.items()))
        console.print(f"[dim]Issue breakdown:[/dim] {summary}")
    for rec in recommendations:
        console.print(f"[yellow]- {rec}[/yellow]")
    raise CLIError("Verify found issues.", exit_code=4)


@main.group()
def backup():
    """Create or restore local backups."""
    pass


@backup.command("list")
@click.option("--limit", default=20, type=int, help="Max backups to show.")
@click.option("--json", "as_json", is_flag=True, help="Emit backups as JSON.")
@click.option(
    "--prune-missing", is_flag=True, help="Remove records whose backup files no longer exist."
)
@click.option("--missing-only", is_flag=True, help="Only show backups missing on disk.")
def backup_list(limit: int, as_json: bool, prune_missing: bool, missing_only: bool):
    """List tracked backups."""
    _validate_limit(limit)
    config = get_config()
    db = get_db(config)
    rows = db.list_backups(limit=limit)
    if prune_missing:
        for row in rows:
            p = Path(str(row.get("path", "")))
            if not p.exists():
                db.delete_backup_record(str(row.get("path", "")))
        rows = db.list_backups(limit=limit)
    enriched = []
    for row in rows:
        r = dict(row)
        r["exists"] = Path(str(r.get("path", ""))).exists()
        enriched.append(r)
    if missing_only:
        enriched = [row for row in enriched if not bool(row.get("exists"))]
    db.close()
    if as_json:
        _emit_json({"count": len(enriched), "backups": enriched})
        return
    table = Table(title="Backups")
    table.add_column("Created", width=24)
    table.add_column("Path", style="bold")
    table.add_column("Files", width=8)
    table.add_column("Verified", width=8)
    table.add_column("Exists", width=8)
    for row in enriched:
        table.add_row(
            str(row.get("created_at", "")),
            str(row.get("path", "")),
            str(row.get("archive_file_count", 0)),
            "yes" if int(row.get("verified", 0)) else "no",
            "yes" if row.get("exists") else "no",
        )
    console.print(table)


@backup.command("create")
@click.option(
    "--path", "backup_path", type=click.Path(dir_okay=False, path_type=Path), required=True
)
@click.option("--json", "as_json", is_flag=True, help="Emit backup summary as JSON.")
@click.option(
    "--dry-run", is_flag=True, help="Show backup summary without writing files or DB records."
)
def backup_create(backup_path: Path, as_json: bool, dry_run: bool):
    """Create a backup archive including DB and config."""
    config = get_config()
    config.ensure_dirs()
    cfg_path = _runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH
    archive_file_count = 0
    if config.archive_dir.exists():
        for p in config.archive_dir.rglob("*"):
            if p.is_file():
                archive_file_count += 1
    db = get_db(config)
    existing_backups = db.list_backups(limit=1000000)
    keep = max(1, config.reliability.backup_retention_count)
    would_prune_count = max(0, (len(existing_backups) + 1) - keep)
    db.close()
    if dry_run:
        payload = {
            "dry_run": True,
            "path": str(backup_path),
            "archive_file_count": int(archive_file_count),
            "includes_database": bool(config.db_path.exists()),
            "includes_config": bool(cfg_path.exists()),
            "would_prune_count": int(would_prune_count),
        }
        if as_json:
            _emit_json(payload)
            return
        console.print("[bold]Backup Dry Run[/bold]")
        console.print(f"Path: {payload['path']}")
        console.print(f"Includes DB: {'yes' if payload['includes_database'] else 'no'}")
        console.print(f"Includes config: {'yes' if payload['includes_config'] else 'no'}")
        console.print(f"Archive files: {payload['archive_file_count']}")
        console.print(f"Would prune old backups: {payload['would_prune_count']}")
        return
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path = config.runtime.tmp_dir / f"archive-snapshot-{uuid4().hex}.db"
    if config.db_path.exists():
        with (
            sqlite3.connect(str(config.db_path)) as src,
            sqlite3.connect(str(snapshot_path)) as dst,
        ):
            src.backup(dst)
    with zipfile.ZipFile(backup_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if snapshot_path.exists():
            zf.write(snapshot_path, arcname="archive.db")
        if cfg_path.exists():
            zf.write(cfg_path, arcname="config.toml")
        if config.archive_dir.exists():
            for p in config.archive_dir.rglob("*"):
                if p.is_file():
                    zf.write(
                        p, arcname=str(Path("archive_data") / p.relative_to(config.archive_dir))
                    )
                    archive_file_count += 1
    db_hash = ""
    verified = False
    if config.reliability.backup_verify_on_create and backup_path.exists():
        try:
            db_hash = file_hash(backup_path)
            verified = True
        except Exception:
            verified = False
    db = get_db(config)
    db.record_backup(
        str(backup_path), db_hash=db_hash, archive_file_count=archive_file_count, verified=verified
    )
    backups = db.list_backups(limit=1000)
    pruned_count = 0
    for old in backups[keep:]:
        old_path = Path(old["path"])
        try:
            if old_path.exists():
                old_path.unlink()
        except Exception:
            pass
        db.delete_backup_record(str(old.get("path", "")))
        pruned_count += 1
    db.close()
    try:
        snapshot_path.unlink(missing_ok=True)
    except PermissionError:
        # On Windows, temporary file handles can linger briefly after zip write.
        pass
    payload = {
        "created": True,
        "path": str(backup_path),
        "archive_file_count": int(archive_file_count),
        "verified": bool(verified),
        "db_hash": db_hash,
        "pruned_count": int(pruned_count),
    }
    if as_json:
        _emit_json(payload)
        return
    console.print(f"[green]Backup created:[/green] {backup_path}")


@backup.command("restore")
@click.option(
    "--path", "backup_path", type=click.Path(dir_okay=False, path_type=Path), required=False
)
@click.option(
    "--latest", "use_latest", is_flag=True, help="Restore from the newest tracked backup record."
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be restored without modifying local files."
)
@click.option("--json", "as_json", is_flag=True, help="Emit restore summary as JSON.")
def backup_restore(backup_path: Path | None, use_latest: bool, dry_run: bool, as_json: bool):
    """Restore DB and archive data from a backup archive."""
    config = get_config()
    config.ensure_dirs()
    cfg_path = _runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH
    if bool(backup_path) == bool(use_latest):
        raise CLIError("Specify exactly one of --path or --latest.", exit_code=2)
    if use_latest:
        db = get_db(config)
        rows = db.list_backups(limit=1)
        db.close()
        if not rows:
            raise CLIError(
                "No tracked backups found. Create one with `backup create` first.", exit_code=2
            )
        selected = Path(str(rows[0].get("path", "")))
        if not selected.exists():
            raise CLIError(
                f"Newest tracked backup is missing on disk: {selected}. Run `backup list --prune-missing` and retry.",
                exit_code=2,
            )
        backup_path = selected
    if backup_path is None or not backup_path.exists():
        raise CLIError(f"Backup path does not exist: {backup_path}", exit_code=2)
    try:
        with zipfile.ZipFile(backup_path, "r") as zf:
            infos = {info.filename: info for info in zf.infolist()}
            members = set(infos)
            for name in members:
                posix = PurePosixPath(name)
                if posix.is_absolute() or ".." in posix.parts:
                    raise CLIError(f"Unsafe backup entry path: {name}", exit_code=2)
            has_db = "archive.db" in members
            has_config = "config.toml" in members
            create_count = 0
            overwrite_count = 0
            archive_entries: list[str] = []
            for name in members:
                if not name.startswith("archive_data/") or name.endswith("/"):
                    continue
                rel = Path(*PurePosixPath(name).parts[1:])
                archive_entries.append(name)
                dest = config.archive_dir / rel
                if dest.exists():
                    overwrite_count += 1
                else:
                    create_count += 1
            if len(archive_entries) > BACKUP_RESTORE_MAX_ARCHIVE_FILES:
                raise CLIError(
                    "Backup restore failed: archive contains too many files to restore safely.",
                    exit_code=4,
                )
            summary = {
                "backup": str(backup_path),
                "has_database": has_db,
                "has_config": has_config,
                "archive_files": len(archive_entries),
                "would_create": create_count,
                "would_overwrite": overwrite_count,
            }
    except CLIError:
        raise
    except Exception as exc:
        raise CLIError(f"Backup restore failed: {exc}", exit_code=4) from exc

    if dry_run:
        payload = {"dry_run": True, **summary}
        if as_json:
            _emit_json(payload)
            return
        console.print("[bold]Restore Dry Run[/bold]")
        console.print(f"Backup: {summary['backup']}")
        console.print(f"Contains DB: {'yes' if summary['has_database'] else 'no'}")
        console.print(f"Contains config: {'yes' if summary['has_config'] else 'no'}")
        console.print(f"Archive files: {summary['archive_files']}")
        console.print(f"Would create: {summary['would_create']}")
        console.print(f"Would overwrite: {summary['would_overwrite']}")
        return

    staging_dir = Path(tempfile.mkdtemp(prefix="restore-", dir=str(config.runtime.tmp_dir)))
    rollback_dir = Path(tempfile.mkdtemp(prefix="rollback-", dir=str(config.runtime.tmp_dir)))
    moved_pairs: list[tuple[Path, Path]] = []
    created_paths: list[Path] = []
    verify_issue_count = 0
    limits = {"total": 0}
    try:
        with zipfile.ZipFile(backup_path, "r") as zf:
            infos = {info.filename: info for info in zf.infolist()}
            if "archive.db" in members:
                _copy_zip_member_limited(
                    zf,
                    infos["archive.db"],
                    staging_dir / "archive.db",
                    limits,
                )
            if "config.toml" in members:
                _copy_zip_member_limited(
                    zf,
                    infos["config.toml"],
                    staging_dir / "config.toml",
                    limits,
                )
            for name in members:
                if not name.startswith("archive_data/") or name.endswith("/"):
                    continue
                rel = Path(*PurePosixPath(name).parts[1:])
                staged = staging_dir / "archive_data" / rel
                _copy_zip_member_limited(
                    zf,
                    infos[name],
                    staged,
                    limits,
                )

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

        if config.reliability.auto_verify_after_restore:
            verify_db = get_db(config)
            verify_report = verify_db.audit_verify(repair=False, full_check=False)
            verify_db.close()
            verify_issue_count = len(verify_report.get("issues") or [])
            if verify_report["issues"]:
                raise CLIError(
                    f"Restore completed but verify found {len(verify_report['issues'])} issue(s).",
                    exit_code=4,
                )
        payload = {"restored": True, **summary, "verify_issues": verify_issue_count}
        if as_json:
            _emit_json(payload)
        else:
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
        raise CLIError(
            "Missing dependency `uvicorn`. Install UI dependencies.", exit_code=3
        ) from exc
    from localarchive.ui.app import create_app

    config = get_config()
    config.ensure_dirs()
    startup_db = get_db(config)
    _run_integrity_check_if_enabled(config, startup_db, "serve")
    startup_db.close()
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
