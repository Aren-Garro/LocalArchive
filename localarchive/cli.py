"""
LocalArchive CLI - main entry point.
Commands: init, ingest, search, export, tag, process, classify, reprocess, watch,
doctor, collections, timeline, audit, verify, backup, duplicates, review, graph, citations, redaction, versions, serve, gui
"""

import imaplib  # noqa: F401 - compatibility for tests monkeypatching localarchive.cli.imaplib
import importlib.util
import json
import re
import sqlite3
import zipfile
from email.header import decode_header
from pathlib import Path
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
SUPPORTED_OCR_LANGUAGES = {
    "en",
    "es",
    "fr",
    "de",
    "zh",
    "ar",
}
OCR_LANGUAGE_ALIASES = {
    "zh-cn": "zh",
    "zh-hans": "zh",
    "zh-hant": "zh",
}


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


def _parse_ocr_languages(raw: str | None, fallback: list[str]) -> list[str]:
    if raw is None:
        langs = [str(x).strip().lower() for x in fallback if str(x).strip()]
        return langs or ["en"]
    parts = [p.strip().lower() for p in raw.split(",")]
    langs = [p for p in parts if p]
    if not langs:
        raise CLIError("`--ocr-languages` must include at least one language code.", exit_code=2)
    normalized: list[str] = []
    for code in langs:
        code = OCR_LANGUAGE_ALIASES.get(code, code)
        if not all(ch.isalnum() or ch in {"-", "_"} for ch in code):
            raise CLIError(
                f"Invalid OCR language code: {code}. Use comma-separated tokens like `en,es,de`.",
                exit_code=2,
            )
        if code not in SUPPORTED_OCR_LANGUAGES:
            supported = ", ".join(sorted(SUPPORTED_OCR_LANGUAGES))
            raise CLIError(
                f"Unsupported OCR language code: {code}. Supported codes: {supported}.",
                exit_code=2,
            )
        normalized.append(code)
    return list(dict.fromkeys(normalized))


def _emit_json(payload: dict | list) -> None:
    click.echo(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def _decode_mime_value(raw: str | None) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    out: list[str] = []
    for value, encoding in parts:
        if isinstance(value, bytes):
            out.append(value.decode(encoding or "utf-8", errors="replace"))
        else:
            out.append(str(value))
    return "".join(out)


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


def _norm_ref_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


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


def _classify_document_ml(doc: dict, model: dict) -> tuple[str, float, list[str]]:
    from localarchive.core.classifier import predict

    text = f"{doc.get('filename', '')} {doc.get('ocr_text', '')}"
    pred = predict(model, text)
    label = str(pred.get("label", "other"))
    confidence = float(pred.get("confidence", 0.0))
    return label, round(confidence, 2), [f"ml:{label}"]


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
    from localarchive.cli_commands.search_cmd import run_search

    run_search(
        query=query,
        tag=tag,
        file_type=file_type,
        legacy_file_type=legacy_file_type,
        limit=limit,
        semantic=semantic,
        bm25_weight=bm25_weight,
        vector_weight=vector_weight,
        fuzzy=fuzzy,
        fuzzy_threshold=fuzzy_threshold,
        fuzzy_max_candidates=fuzzy_max_candidates,
        explain_ranking=explain_ranking,
        as_json=as_json,
    )


@main.command()
@click.option("--query", default=None, help="Search query to filter export")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["csv", "json", "markdown", "csljson", "bibtex", "ris"]),
    default="csv",
)
@click.option("--output", "-o", required=True, help="Output file path")
@click.option("--include-tables", is_flag=True, help="Include extracted tables in exported rows.")
def export(query: str, fmt: str, output: str, include_tables: bool):
    """Export documents to CSV, JSON, Markdown, CSL-JSON, BibTeX, or RIS."""
    from localarchive.core.exporter import (
        export_bibtex,
        export_csljson,
        export_csv,
        export_json,
        export_markdown,
        export_ris,
    )

    config = get_config()
    db = get_db(config)
    engine = SearchEngine(db)
    if query:
        documents = engine.search(query, limit=10000)
    else:
        documents = db.list_documents(limit=10000)
    if include_tables:
        enriched = []
        for doc in documents:
            row = dict(doc)
            row["tables"] = db.get_tables(int(doc["id"]))
            row["metadata"] = db.get_document_metadata(int(doc["id"]))
            enriched.append(row)
        documents = enriched
    else:
        enriched = []
        for doc in documents:
            row = dict(doc)
            row["metadata"] = db.get_document_metadata(int(doc["id"]))
            enriched.append(row)
        documents = enriched
    output_path = Path(output)
    if fmt == "csv":
        export_csv(documents, output_path)
    elif fmt == "json":
        export_json(documents, output_path)
    elif fmt == "markdown":
        export_markdown(documents, output_path)
    elif fmt == "csljson":
        export_csljson(documents, output_path)
    elif fmt == "bibtex":
        export_bibtex(documents, output_path)
    elif fmt == "ris":
        export_ris(documents, output_path)
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
@click.option("--extract-tables", is_flag=True, help="Extract simple table structures from document text.")
@click.option("--dry-run", is_flag=True, help="Preview pending IDs and exit without processing.")
@click.option("--max-errors", type=int, default=None, help="Abort run after this many errors.")
@click.option("--resume", is_flag=True, help="Resume from latest processing checkpoint.")
@click.option(
    "--from-run", type=int, default=None, help="Resume from a specific processing run ID."
)
@click.option(
    "--ocr-languages",
    default=None,
    help="Comma-separated OCR language codes for this run (overrides config.ocr.languages).",
)
@click.option(
    "--ocr-engine",
    "ocr_engine_override",
    type=click.Choice(["paddleocr", "easyocr"]),
    default=None,
    help="OCR backend for this run (overrides config.ocr.engine).",
)
@click.option("--json", "as_json", is_flag=True, help="Emit process run summary as JSON.")
def process(
    limit: int | None,
    workers: int | None,
    commit_batch_size: int | None,
    checkpoint_every: int | None,
    extractor_mode: str | None,
    extract_tables: bool,
    dry_run: bool,
    max_errors: int | None,
    resume: bool,
    from_run: int | None,
    ocr_languages: str | None,
    ocr_engine_override: str | None,
    as_json: bool,
):
    """Run OCR and field extraction on pending documents."""
    from localarchive.cli_commands.process_cmd import run_process

    run_process(
        limit=limit,
        workers=workers,
        commit_batch_size=commit_batch_size,
        checkpoint_every=checkpoint_every,
        extractor_mode=extractor_mode,
        extract_tables=extract_tables,
        dry_run=dry_run,
        max_errors=max_errors,
        resume=resume,
        from_run=from_run,
        ocr_languages=ocr_languages,
        ocr_engine_override=ocr_engine_override,
        as_json=as_json,
    )


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
    model_name = config.autopilot.classification_model
    model = None
    if model_name == "ml":
        try:
            from localarchive.core.classifier import load_model

            model = load_model(config.autopilot.model_path)
        except Exception:
            console.print(
                "[yellow]ML model unavailable; falling back to rules model. "
                "Train with `localarchive classify-train`.[/yellow]"
            )
            model_name = "rules"
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
        if model_name == "ml" and model is not None:
            label, confidence, reasons = _classify_document_ml(doc, model)
        else:
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
    console.print(f"[dim]Model:[/dim] {model_name}")
    console.print(f"[green]Updated:[/green] {updated}  [dim]Skipped:[/dim] {skipped}")
    db.close()


@main.command("classify-train")
@click.option(
    "--dataset",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to labeled dataset with columns/keys `text` and `label`.",
)
@click.option(
    "--format",
    "dataset_format",
    type=click.Choice(["csv", "json"]),
    default="csv",
    help="Dataset format.",
)
@click.option(
    "--output-model",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Optional model output path (defaults to config.autopilot.model_path).",
)
@click.option("--json", "as_json", is_flag=True, help="Emit training summary as JSON.")
def classify_train(dataset: Path, dataset_format: str, output_model: Path | None, as_json: bool):
    """Train local ML classifier from labeled examples."""
    from localarchive.core.classifier import load_labeled_examples, save_model, train_model

    config = get_config()
    examples = load_labeled_examples(dataset, fmt=dataset_format)
    if len(examples) < config.autopilot.min_training_samples:
        raise CLIError(
            f"Need at least {config.autopilot.min_training_samples} examples to train ML classifier.",
            exit_code=2,
        )
    model = train_model(examples)
    model_path = output_model or config.autopilot.model_path
    save_model(model, model_path)
    payload = {
        "trained": True,
        "examples": len(examples),
        "labels": model.get("labels", []),
        "vocab_size": int(model.get("vocab_size", 0)),
        "model_path": str(model_path),
    }
    if as_json:
        _emit_json(payload)
        return
    console.print(
        f"[green]Model trained:[/green] {payload['examples']} examples, "
        f"{len(payload['labels'])} labels, vocab={payload['vocab_size']}"
    )
    console.print(f"[dim]Saved:[/dim] {model_path}")


@main.command("classify-evaluate")
@click.option(
    "--dataset",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to labeled evaluation dataset with columns/keys `text` and `label`.",
)
@click.option(
    "--format",
    "dataset_format",
    type=click.Choice(["csv", "json"]),
    default="csv",
    help="Dataset format.",
)
@click.option(
    "--model",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Optional model path (defaults to config.autopilot.model_path).",
)
@click.option("--json", "as_json", is_flag=True, help="Emit evaluation report as JSON.")
def classify_evaluate(dataset: Path, dataset_format: str, model: Path | None, as_json: bool):
    """Evaluate local ML classifier against labeled examples."""
    from localarchive.core.classifier import evaluate, load_labeled_examples, load_model

    config = get_config()
    model_path = model or config.autopilot.model_path
    model_data = load_model(model_path)
    examples = load_labeled_examples(dataset, fmt=dataset_format)
    report = evaluate(model_data, examples)
    payload = {"model_path": str(model_path), **report}
    if as_json:
        _emit_json(payload)
        return
    console.print(
        f"[green]Evaluation:[/green] accuracy={report['accuracy']:.3f} "
        f"({report['correct']}/{report['total']})"
    )


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
        has_paddleocr = importlib.util.find_spec("paddleocr") is not None
        has_paddle = importlib.util.find_spec("paddle") is not None
        _check(
            "paddleocr_installed",
            has_paddleocr,
            "required by OCR config (`pip install -r requirements-ocr-paddle.txt`)",
        )
        _check(
            "paddlepaddle_installed",
            has_paddle,
            "required by PaddleOCR backend (`pip install -r requirements-ocr-paddle.txt`)",
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
def plugins():
    """Inspect and manage local plugins."""
    pass


@plugins.command("list")
@click.option("--json", "as_json", is_flag=True, help="Emit plugins as JSON.")
def plugins_list(as_json: bool):
    """List discovered plugins from configured search paths."""
    from localarchive.core.plugins import discover_plugins

    config = get_config()
    rows = discover_plugins(config)
    if as_json:
        _emit_json({"count": len(rows), "plugins": rows})
        return
    table = Table(title="Plugins")
    table.add_column("Name", style="bold")
    table.add_column("Version", width=10)
    table.add_column("Kind", width=12)
    table.add_column("Enabled", width=8)
    table.add_column("Path")
    for row in rows:
        table.add_row(
            str(row.get("name", "")),
            str(row.get("version", "")),
            str(row.get("kind", "")),
            "yes" if row.get("enabled") else "no",
            str(row.get("path", "")),
        )
    console.print(table)


@plugins.command("inspect")
@click.argument("name")
@click.option("--json", "as_json", is_flag=True, help="Emit plugin details as JSON.")
def plugins_inspect(name: str, as_json: bool):
    """Inspect a discovered plugin manifest."""
    from localarchive.core.plugins import get_plugin_by_name

    config = get_config()
    row = get_plugin_by_name(config, name)
    if not row:
        raise CLIError(f"Plugin `{name}` not found in configured search paths.", exit_code=2)
    if as_json:
        _emit_json(row)
        return
    table = Table(title=f"Plugin: {row['name']}")
    table.add_column("Field", style="bold", width=16)
    table.add_column("Value")
    for key in ("name", "version", "kind", "description", "entrypoint", "path", "enabled"):
        table.add_row(key, str(row.get(key, "")))
    console.print(table)


@plugins.command("enable")
@click.argument("name")
def plugins_enable(name: str):
    """Enable plugin by name in config.plugins.enabled."""
    from localarchive.core.plugins import get_plugin_by_name

    config = get_config()
    row = get_plugin_by_name(config, name)
    if not row:
        raise CLIError(f"Plugin `{name}` not found in configured search paths.", exit_code=2)
    if row["name"] not in config.plugins.enabled:
        config.plugins.enabled.append(row["name"])
        config.plugins.enabled = sorted(set(config.plugins.enabled))
    config_path = _runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH
    config.save(config_path)
    console.print(f"[green]Enabled plugin:[/green] {row['name']}")


@plugins.command("disable")
@click.argument("name")
def plugins_disable(name: str):
    """Disable plugin by name in config.plugins.enabled."""
    config = get_config()
    target = name.strip().lower()
    kept = [n for n in config.plugins.enabled if str(n).strip().lower() != target]
    if len(kept) == len(config.plugins.enabled):
        raise CLIError(f"Plugin `{name}` is not enabled.", exit_code=2)
    config.plugins.enabled = kept
    config_path = _runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH
    config.save(config_path)
    console.print(f"[green]Disabled plugin:[/green] {name}")


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


@main.group()
def similarity():
    """Build and inspect local document similarity edges."""
    pass


@main.group()
def graph():
    """Build and export relationship graphs."""
    pass


@main.group()
def citations():
    """Extract citation identifiers and bibliography candidates."""
    pass


@main.group()
def metadata():
    """Metadata profile validation and editing."""
    pass


@main.group()
def redaction():
    """Privacy-safe redaction tools."""
    pass


@metadata.command("profiles")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def metadata_profiles(as_json: bool):
    """List metadata profiles and required fields."""
    from localarchive.core.metadata_profiles import list_profiles

    rows = list_profiles()
    if as_json:
        _emit_json({"count": len(rows), "profiles": rows})
        return
    table = Table(title="Metadata Profiles")
    table.add_column("Profile", style="cyan", width=14)
    table.add_column("Required Fields")
    for row in rows:
        table.add_row(str(row["id"]), ", ".join(row["required_fields"]))
    console.print(table)


@metadata.command("validate")
@click.argument("doc_id", type=int)
@click.option(
    "--profile",
    "profile_id",
    type=click.Choice(["core", "research", "archival"]),
    default="research",
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def metadata_validate(doc_id: int, profile_id: str, as_json: bool):
    """Validate a document against a metadata profile."""
    from localarchive.core.metadata_profiles import validate_profile

    config = get_config()
    db = get_db(config)
    doc = db.get_document(doc_id)
    if not doc:
        db.close()
        raise CLIError(f"Document {doc_id} not found.", exit_code=2)
    metadata_map = db.get_document_metadata(doc_id)
    citations_rows = db.list_document_citations(doc_id=doc_id, limit=200)
    result = validate_profile(profile_id, metadata_map, citations_rows)
    payload = {
        "doc_id": int(doc_id),
        "filename": str(doc.get("filename", "")),
        "profile": profile_id,
        **result,
    }
    db.close()
    if as_json:
        _emit_json(payload)
        return
    if payload["ok"]:
        console.print(f"[green]Metadata valid:[/green] profile={profile_id} doc={doc_id}")
        return
    missing = ", ".join(payload["missing_fields"]) if payload["missing_fields"] else "-"
    issues = ", ".join(payload["issues"]) if payload["issues"] else "-"
    console.print(f"[yellow]Metadata issues:[/yellow] profile={profile_id} doc={doc_id}")
    console.print(f"[dim]missing={missing} issues={issues}[/dim]")


@metadata.command("edit")
@click.argument("doc_id", type=int)
@click.option("--field", "field_name", required=True, help="Metadata field key.")
@click.option("--value", "field_value", required=True, help="Metadata value.")
@click.option("--source", default="manual", help="Metadata source (manual/import/ocr).")
@click.option("--confidence", default=1.0, type=float, help="Metadata confidence (0-1).")
@click.option("--updated-by", default="operator", help="Operator identifier.")
@click.option("--note", default="", help="Optional metadata note to append.")
def metadata_edit(
    doc_id: int,
    field_name: str,
    field_value: str,
    source: str,
    confidence: float,
    updated_by: str,
    note: str,
):
    """Set or update one metadata field for a document."""
    if confidence < 0 or confidence > 1:
        raise CLIError("confidence must be between 0 and 1", exit_code=2)
    config = get_config()
    db = get_db(config)
    doc = db.get_document(doc_id)
    if not doc:
        db.close()
        raise CLIError(f"Document {doc_id} not found.", exit_code=2)
    db.set_document_metadata(
        doc_id=doc_id,
        key=field_name.strip().lower(),
        value=field_value.strip(),
        source=source.strip().lower(),
        confidence=float(confidence),
        updated_by=updated_by.strip() or "operator",
    )
    if note.strip():
        db.add_metadata_note(doc_id, note.strip())
    db.close()
    console.print(f"[green]Metadata updated:[/green] doc={doc_id} {field_name}={field_value}")


@main.group()
def versions():
    """Track and inspect document version snapshots."""
    pass


@versions.command("record")
@click.argument("doc_id", type=int)
@click.option("--note", default="", help="Optional note for this snapshot.")
def versions_record(doc_id: int, note: str):
    """Create a version snapshot for a document."""
    config = get_config()
    db = get_db(config)
    changed = db.record_document_version(doc_id, note=note)
    db.close()
    if changed == 0:
        raise CLIError(f"Document {doc_id} not found.", exit_code=2)
    console.print(f"[green]Version snapshot recorded for document {doc_id}.[/green]")


@versions.command("list")
@click.argument("doc_id", type=int)
@click.option("--limit", default=20, type=int, help="Max versions to list.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def versions_list(doc_id: int, limit: int, as_json: bool):
    """List version snapshots for a document."""
    _validate_limit(limit)
    config = get_config()
    db = get_db(config)
    rows = db.list_document_versions(doc_id, limit=limit)
    db.close()
    if as_json:
        _emit_json({"doc_id": int(doc_id), "count": len(rows), "versions": rows})
        return
    table = Table(title=f"Document Versions: {doc_id}")
    table.add_column("Version", width=8)
    table.add_column("Captured At", width=28)
    table.add_column("Status", width=12)
    table.add_column("Note")
    for row in rows:
        table.add_row(
            str(row.get("version_no", "")),
            str(row.get("captured_at", "")),
            str(row.get("status", "")),
            str(row.get("note", "")),
        )
    console.print(table)


@redaction.command("document")
@click.argument("doc_id", type=int)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Output file path for redacted content.",
)
@click.option("--json", "as_json", is_flag=True, help="Emit redaction metadata as JSON.")
def redaction_document(doc_id: int, output: Path, as_json: bool):
    """Create a redacted text export for a document."""
    from localarchive.core.redaction import redact_text

    config = get_config()
    db = get_db(config)
    doc = db.get_document(doc_id)
    db.close()
    if not doc:
        raise CLIError(f"Document {doc_id} not found.", exit_code=2)
    redacted, counts = redact_text(str(doc.get("ocr_text", "") or ""))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(redacted, encoding="utf-8")
    payload = {
        "doc_id": int(doc_id),
        "filename": str(doc.get("filename", "")),
        "output": str(output),
        "counts": counts,
    }
    if as_json:
        _emit_json(payload)
        return
    summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    console.print(f"[green]Redacted export written:[/green] {output}")
    console.print(f"[dim]{summary}[/dim]")


@citations.command("extract")
@click.option("--limit", default=1000, type=int, help="Max documents to scan.")
@click.option("--status", default="processed", help="Optional status filter.")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["json", "markdown"]),
    default="json",
    help="Output format.",
)
def citations_extract(limit: int, status: str, fmt: str):
    """Extract DOI/arXiv citation identifiers across documents."""
    _validate_limit(limit)
    from localarchive.core.citations import collect_citations

    config = get_config()
    db = get_db(config)
    docs = db.list_documents(limit=limit, status=status or None)
    citations_out: list[dict] = []
    for doc in docs:
        fields = db.get_fields(int(doc["id"]))
        found = collect_citations(doc, fields)
        citations_out.extend(found)
        for item in found:
            db.upsert_document_citation(
                int(doc["id"]),
                str(item["type"]).lower(),
                str(item["value"]).strip(),
                status="unresolved",
                resolved_value="",
            )
    db.close()

    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for item in citations_out:
        key = (str(item["type"]), str(item["value"]).lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    if fmt == "json":
        _emit_json({"count": len(deduped), "citations": deduped})
        return
    if not deduped:
        console.print("No citation candidates found.")
        return
    lines = ["# Citation Candidates", ""]
    for row in deduped:
        lines.append(f"- `{row['type']}` {row['value']} (source: {row['source']})")
    click.echo("\n".join(lines))


@citations.command("list")
@click.option("--status", type=click.Choice(["unresolved", "resolved", "all"]), default="unresolved")
@click.option("--limit", default=200, type=int, help="Max rows.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def citations_list(status: str, limit: int, as_json: bool):
    """List extracted citation records."""
    _validate_limit(limit)
    config = get_config()
    db = get_db(config)
    rows = db.list_document_citations(status=None if status == "all" else status, limit=limit)
    db.close()
    if as_json:
        _emit_json({"count": len(rows), "status": status, "citations": rows})
        return
    table = Table(title=f"Citations ({status})")
    table.add_column("Doc ID", style="cyan", width=8)
    table.add_column("Type", width=8)
    table.add_column("Value")
    table.add_column("Status", width=10)
    table.add_column("Resolved")
    table.add_column("Filename", style="bold")
    for row in rows:
        table.add_row(
            str(row.get("document_id", "")),
            str(row.get("citation_type", "")),
            str(row.get("citation_value", "")),
            str(row.get("status", "")),
            str(row.get("resolved_value", "")),
            str(row.get("filename", "")),
        )
    console.print(table)


@citations.command("resolve")
@click.argument("doc_id", type=int)
@click.option("--doi", default="", help="Resolved DOI value.")
@click.option("--arxiv", default="", help="Resolved arXiv value.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def citations_resolve(doc_id: int, doi: str, arxiv: str, as_json: bool):
    """Resolve citation records for a document."""
    if not doi.strip() and not arxiv.strip():
        raise CLIError("Provide --doi and/or --arxiv.", exit_code=2)
    config = get_config()
    db = get_db(config)
    doc = db.get_document(doc_id)
    if not doc:
        db.close()
        raise CLIError(f"Document {doc_id} not found.", exit_code=2)
    changed = 0
    if doi.strip():
        value = doi.strip().lower()
        db.upsert_document_citation(
            doc_id=doc_id,
            citation_type="doi",
            citation_value=value,
            status="resolved",
            resolved_value=value,
        )
        changed += 1
    if arxiv.strip():
        value = arxiv.strip()
        db.upsert_document_citation(
            doc_id=doc_id,
            citation_type="arxiv",
            citation_value=value,
            status="resolved",
            resolved_value=value,
        )
        changed += 1
    db.close()
    payload = {"doc_id": doc_id, "resolved_count": changed}
    if as_json:
        _emit_json(payload)
        return
    console.print(f"[green]Resolved citations:[/green] doc={doc_id} count={changed}")


@graph.command("entities")
@click.option("--limit", default=500, type=int, help="Max documents to include.")
@click.option("--status", default="processed", help="Optional document status filter.")
@click.option("--json", "as_json", is_flag=True, help="Emit graph in JSON.")
def graph_entities(limit: int, status: str, as_json: bool):
    """Build document-to-entity relationship graph."""
    _validate_limit(limit)
    from localarchive.core.entity_graph import build_entity_graph

    config = get_config()
    db = get_db(config)
    docs = db.list_documents(limit=limit, status=status or None)
    fields_by_doc = {int(doc["id"]): db.get_fields(int(doc["id"])) for doc in docs}
    db.close()
    graph_payload = build_entity_graph(docs, fields_by_doc)
    if as_json:
        _emit_json(
            {
                "documents": len(docs),
                "nodes": graph_payload["nodes"],
                "edges": graph_payload["edges"],
            }
        )
        return

    entity_nodes = [n for n in graph_payload["nodes"] if str(n.get("kind")) == "entity"]
    console.print(
        f"[green]Entity graph built:[/green] docs={len(docs)} entities={len(entity_nodes)} edges={len(graph_payload['edges'])}"
    )
    if not entity_nodes:
        console.print("[dim]No entity nodes found. Run `localarchive process --extractor hybrid` first.[/dim]")
        return
    table = Table(title="Top Entity Nodes")
    table.add_column("Type", width=16)
    table.add_column("Entity", style="bold")
    counts: dict[str, int] = {}
    for edge in graph_payload["edges"]:
        tgt = str(edge.get("target", ""))
        counts[tgt] = counts.get(tgt, 0) + 1
    ranked = sorted(entity_nodes, key=lambda n: counts.get(str(n["id"]), 0), reverse=True)[:15]
    for node in ranked:
        table.add_row(str(node.get("entity_type", "")), str(node.get("label", "")))
    console.print(table)


@similarity.command("build")
@click.option("--limit", default=2000, type=int, help="Max processed documents to include.")
@click.option("--top-k", default=5, type=int, help="Max neighbors per document.")
@click.option("--min-score", default=0.15, type=float, help="Minimum similarity score (0-1).")
@click.option("--json", "as_json", is_flag=True, help="Emit build summary as JSON.")
def similarity_build(limit: int, top_k: int, min_score: float, as_json: bool):
    """Build pairwise similarity edges using local token-based scoring."""
    from localarchive.core.similarity import build_similarity_edges

    _validate_limit(limit)
    _validate_limit(top_k)
    _validate_threshold("min-score", min_score)
    config = get_config()
    db = get_db(config)
    docs = db.list_documents(status="processed", limit=limit)
    edges = build_similarity_edges(docs, top_k=top_k, min_score=min_score)
    db.clear_similarity()
    db.upsert_similarity_edges(edges)
    payload = {
        "built": True,
        "documents": len(docs),
        "edges": len(edges),
        "top_k": int(top_k),
        "min_score": float(min_score),
        "model": "token-jaccard",
    }
    db.close()
    if as_json:
        _emit_json(payload)
        return
    console.print(
        f"[green]Similarity built:[/green] {payload['documents']} docs, {payload['edges']} edges "
        f"(top_k={top_k}, min_score={min_score:.2f})"
    )


@similarity.command("for")
@click.argument("doc_id", type=int)
@click.option("--top-k", default=10, type=int, help="Max related documents to return.")
@click.option("--json", "as_json", is_flag=True, help="Emit related documents as JSON.")
def similarity_for(doc_id: int, top_k: int, as_json: bool):
    """Show most similar documents for a given doc ID."""
    _validate_limit(top_k)
    config = get_config()
    db = get_db(config)
    doc = db.get_document(doc_id)
    if not doc:
        db.close()
        raise CLIError(f"Document {doc_id} not found.", exit_code=2)
    rows = db.get_similar_documents(doc_id, limit=top_k)
    db.close()
    if as_json:
        _emit_json({"doc_id": doc_id, "count": len(rows), "related": rows})
        return
    table = Table(title=f"Related Documents for {doc_id}")
    table.add_column("ID", style="cyan", width=8)
    table.add_column("Filename", style="bold")
    table.add_column("Type", width=8)
    table.add_column("Status", width=12)
    table.add_column("Score", width=8)
    for row in rows:
        table.add_row(
            str(row.get("related_id")),
            str(row.get("filename", "")),
            str(row.get("file_type", "")),
            str(row.get("status", "")),
            f"{float(row.get('score', 0.0)):.3f}",
        )
    console.print(table)


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


@main.group()
def duplicates():
    """Detect likely duplicate scans using perceptual hashes."""
    pass


@main.group()
def review():
    """Confidence review queue workflows."""
    pass


@main.group()
def sync():
    """Local-first metadata sync helpers (snapshot + merge)."""
    pass


@review.command("build")
@click.option("--limit", default=500, type=int, help="Max documents to evaluate.")
@click.option("--status", default="processed", help="Optional document status filter.")
@click.option(
    "--profile",
    "profile_id",
    type=click.Choice(["default", "research"]),
    default="default",
    help="Review profile policy.",
)
@click.option(
    "--threshold",
    default=0.55,
    type=float,
    help="Queue documents with confidence score below this threshold.",
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def review_build(limit: int, status: str, profile_id: str, threshold: float, as_json: bool):
    """Compute confidence scores and queue low-confidence documents for manual review."""
    _validate_limit(limit)
    _validate_threshold("threshold", threshold)
    from localarchive.core.metadata_profiles import validate_profile
    from localarchive.core.validation import score_document_confidence

    config = get_config()
    db = get_db(config)
    docs = db.list_documents(limit=limit, status=status or None)
    queued = 0
    scanned = 0
    for doc in docs:
        scanned += 1
        fields = db.get_fields(int(doc["id"]))
        score, reason = score_document_confidence(doc, fields)
        if profile_id == "research":
            metadata_map = db.get_document_metadata(int(doc["id"]))
            citations_rows = db.list_document_citations(doc_id=int(doc["id"]), limit=200)
            profile_result = validate_profile("research", metadata_map, citations_rows)
            if not profile_result["ok"]:
                score = min(score, 0.49)
                extra = ",".join(profile_result["issues"]) or "metadata_profile_invalid"
                reason = f"{reason},{extra}" if reason else extra
        if score < threshold:
            db.upsert_review_item(int(doc["id"]), score, reason)
            queued += 1
    db.close()
    payload = {
        "scanned": scanned,
        "queued": queued,
        "threshold": float(threshold),
        "status_filter": status or "",
        "profile": profile_id,
    }
    if as_json:
        _emit_json(payload)
        return
    console.print(
        f"[green]Review queue updated:[/green] scanned={scanned} queued={queued} threshold={threshold:.2f}"
    )


@review.command("list")
@click.option("--status", default="pending", help="Queue status filter: pending/resolved/all")
@click.option("--limit", default=100, type=int, help="Max rows to return.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def review_list(status: str, limit: int, as_json: bool):
    """List review queue items."""
    _validate_limit(limit)
    status_norm = status.strip().lower()
    if status_norm not in {"pending", "resolved", "all"}:
        raise CLIError("status must be one of: pending, resolved, all", exit_code=2)
    config = get_config()
    db = get_db(config)
    rows = db.list_review_items(status=None if status_norm == "all" else status_norm, limit=limit)
    db.close()
    if as_json:
        _emit_json({"count": len(rows), "status": status_norm, "items": rows})
        return
    table = Table(title=f"Review Queue ({status_norm})")
    table.add_column("Doc ID", style="cyan", width=8)
    table.add_column("Score", width=8)
    table.add_column("Status", width=10)
    table.add_column("Reason", width=30)
    table.add_column("Filename", style="bold")
    for row in rows:
        table.add_row(
            str(row.get("document_id")),
            f"{float(row.get('confidence_score', 0.0)):.3f}",
            str(row.get("status", "")),
            str(row.get("reason", "")),
            str(row.get("filename", "")),
        )
    console.print(table)


@review.command("stats")
@click.option("--top-reasons", default=5, type=int, help="Max pending reason buckets to show.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def review_stats(top_reasons: int, as_json: bool):
    """Summarize review queue backlog and top pending reasons."""
    _validate_limit(top_reasons)
    config = get_config()
    db = get_db(config)
    status_rows = db.conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM review_queue
        GROUP BY status
        """
    ).fetchall()
    reasons = db.conn.execute(
        """
        SELECT reason, COUNT(*) AS count
        FROM review_queue
        WHERE status = 'pending'
        GROUP BY reason
        ORDER BY count DESC, reason ASC
        LIMIT ?
        """,
        (top_reasons,),
    ).fetchall()
    db.close()
    status_counts = {str(row["status"]): int(row["count"]) for row in status_rows}
    pending = int(status_counts.get("pending", 0))
    resolved = int(status_counts.get("resolved", 0))
    payload = {
        "total": pending + resolved,
        "pending": pending,
        "resolved": resolved,
        "top_pending_reasons": [
            {"reason": str(row["reason"] or "unspecified"), "count": int(row["count"])} for row in reasons
        ],
    }
    if as_json:
        _emit_json(payload)
        return
    console.print(
        f"[green]Review queue:[/green] total={payload['total']} pending={pending} resolved={resolved}"
    )
    if not payload["top_pending_reasons"]:
        console.print("[dim]No pending review reasons.[/dim]")
        return
    table = Table(title="Top Pending Review Reasons")
    table.add_column("Reason", style="bold")
    table.add_column("Count", justify="right")
    for row in payload["top_pending_reasons"]:
        table.add_row(str(row["reason"]), str(row["count"]))
    console.print(table)


@review.command("resolve")
@click.argument("doc_id", type=int)
@click.option("--note", default="", help="Resolution note.")
def review_resolve(doc_id: int, note: str):
    """Mark a queued document as reviewed/resolved."""
    config = get_config()
    db = get_db(config)
    changed = db.resolve_review_item(doc_id, note=note)
    db.close()
    if changed == 0:
        raise CLIError(f"Review item for document {doc_id} not found.", exit_code=2)
    console.print(f"[green]Resolved review item for document {doc_id}.[/green]")


@review.command("next")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def review_next(as_json: bool):
    """Return the next pending review item."""
    config = get_config()
    db = get_db(config)
    rows = db.list_review_items(status="pending", limit=1)
    db.close()
    if as_json:
        _emit_json({"count": len(rows), "item": rows[0] if rows else None})
        return
    if not rows:
        console.print("[dim]No pending review items.[/dim]")
        return
    row = rows[0]
    console.print(
        f"[yellow]Next review item:[/yellow] id={row.get('id')} doc={row.get('document_id')} "
        f"score={float(row.get('confidence_score', 0.0)):.3f} reason={row.get('reason', '')}"
    )


@review.command("complete")
@click.argument("item_id", type=int)
@click.option("--note", default="", help="Resolution note.")
def review_complete(item_id: int, note: str):
    """Resolve a review item by queue item ID."""
    config = get_config()
    db = get_db(config)
    changed = db.resolve_review_item_by_id(item_id, note=note)
    db.close()
    if changed == 0:
        raise CLIError(f"Review item {item_id} not found.", exit_code=2)
    console.print(f"[green]Resolved review queue item {item_id}.[/green]")


@duplicates.command("scan")
@click.option("--limit", default=1000, type=int, help="Max documents to inspect.")
@click.option(
    "--max-distance",
    default=6,
    type=int,
    help="Max perceptual hash Hamming distance to treat as duplicate.",
)
@click.option("--status", default=None, help="Optional document status filter.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def duplicates_scan(limit: int, max_distance: int, status: str | None, as_json: bool):
    """Scan archive for near-duplicate documents."""
    if limit < 1:
        raise CLIError("Limit must be >= 1.", exit_code=2)
    if max_distance < 0 or max_distance > 64:
        raise CLIError("max-distance must be between 0 and 64.", exit_code=2)

    from localarchive.core.duplicates import (
        DuplicateCandidate,
        find_duplicate_pairs,
        perceptual_hash_for_file,
    )

    config = get_config()
    db = get_db(config)
    docs = db.list_documents(limit=limit, status=status or None)
    candidates: list[DuplicateCandidate] = []
    skipped = 0
    for doc in docs:
        file_type = str(doc.get("file_type", "")).lower()
        if file_type not in {"pdf", "png", "jpg", "jpeg", "tif", "tiff", "bmp", "webp", "gif"}:
            skipped += 1
            continue
        path = Path(str(doc.get("filepath", "")))
        if not path.exists():
            skipped += 1
            continue
        try:
            phash = perceptual_hash_for_file(path, file_type=file_type)
        except Exception:
            skipped += 1
            continue
        candidates.append(
            DuplicateCandidate(
                doc_id=int(doc["id"]),
                filename=str(doc.get("filename", "")),
                filepath=str(path),
                file_type=file_type,
                phash=phash,
            )
        )
    db.close()
    pairs = find_duplicate_pairs(candidates, max_distance=max_distance)
    if as_json:
        _emit_json(
            {
                "inspected": len(docs),
                "hashed": len(candidates),
                "skipped": skipped,
                "max_distance": max_distance,
                "duplicates": pairs,
            }
        )
        return
    if not pairs:
        console.print(
            f"[green]No duplicates found.[/green] inspected={len(docs)} hashed={len(candidates)} skipped={skipped}"
        )
        return
    table = Table(title=f"Duplicate Candidates (distance <= {max_distance})")
    table.add_column("Doc A", style="cyan", width=8)
    table.add_column("Doc B", style="cyan", width=8)
    table.add_column("Distance", width=10)
    table.add_column("File A", style="bold")
    table.add_column("File B", style="bold")
    for pair in pairs:
        table.add_row(
            str(pair["doc_id_a"]),
            str(pair["doc_id_b"]),
            str(pair["distance"]),
            str(pair["filename_a"]),
            str(pair["filename_b"]),
        )
    console.print(table)
    console.print(
        f"\n[dim]inspected={len(docs)} hashed={len(candidates)} skipped={skipped} duplicate_pairs={len(pairs)}[/dim]"
    )


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
    from localarchive.cli_commands.backup_cmd import run_backup_restore

    run_backup_restore(
        backup_path=backup_path,
        use_latest=use_latest,
        dry_run=dry_run,
        as_json=as_json,
    )


@main.group()
def connectors():
    """Integration connectors (email, sync, automation)."""


@main.group("import")
def import_group():
    """Import external data into LocalArchive."""


@main.group()
def resources():
    """Educational guides and learning resources."""


@resources.command("list")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def resources_list(as_json: bool):
    """List educational resources."""
    from localarchive.core.resources import list_resources

    rows = list_resources()
    payload = {
        "count": len(rows),
        "resources": [
            {"id": row.resource_id, "title": row.title, "path": row.path}
            for row in rows
        ],
    }
    if as_json:
        _emit_json(payload)
        return
    table = Table(title="Educational Resources")
    table.add_column("ID", style="cyan", width=20)
    table.add_column("Title", style="bold")
    table.add_column("Path")
    for row in rows:
        table.add_row(row.resource_id, row.title, row.path)
    console.print(table)


@resources.command("show")
@click.argument("resource_id")
def resources_show(resource_id: str):
    """Print a resource by ID."""
    from localarchive.core.resources import get_resource, read_resource_text

    resource = get_resource(resource_id)
    if resource is None:
        raise CLIError(f"Resource not found: {resource_id}", exit_code=2)
    console.print(f"[bold]{resource.title}[/bold]")
    console.print(f"[dim]{resource.path}[/dim]\n")
    console.print(read_resource_text(resource))


@import_group.command("refs")
@click.option("--format", "fmt", type=click.Choice(["bibtex", "ris"]), required=True)
@click.option("--path", "input_path", type=click.Path(dir_okay=False, exists=True, path_type=Path), required=True)
@click.option("--dry-run", is_flag=True, help="Preview matches/updates without writing metadata.")
@click.option(
    "--unresolved-output",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Write unresolved reference entries to a JSON file.",
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def import_refs(fmt: str, input_path: Path, dry_run: bool, unresolved_output: Path | None, as_json: bool):
    """Import reference metadata and map onto local documents."""
    from localarchive.core.ref_importer import parse_bibtex, parse_ris

    parser = parse_bibtex if fmt == "bibtex" else parse_ris
    entries = parser(input_path)
    config = get_config()
    db = get_db(config)
    matched = 0
    planned_updates = 0
    applied_updates = 0
    unresolved = 0
    unresolved_entries: list[dict[str, str]] = []
    for entry in entries:
        doi = str(entry.get("doi", "")).strip().lower()
        title = str(entry.get("title", "")).strip()
        author = str(entry.get("author", "")).strip()
        year = str(entry.get("year", "")).strip()
        row = None
        if doi:
            row = db.conn.execute(
                """
                SELECT document_id
                FROM document_citations
                WHERE citation_type = 'doi' AND citation_value = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (doi,),
            ).fetchone()
        if row is None and doi:
            row = db.conn.execute(
                """
                SELECT document_id
                FROM document_metadata
                WHERE key = 'doi' AND lower(value) = ?
                LIMIT 1
                """,
                (doi,),
            ).fetchone()
        if row is None and title:
            norm_title = _norm_ref_title(title)
            row = db.conn.execute(
                """
                SELECT dm.document_id
                FROM document_metadata dm
                WHERE dm.key = 'title'
                  AND replace(replace(replace(replace(replace(lower(dm.value), ' ', ''), '-', ''), ':', ''), ',', ''), '.', '') = ?
                LIMIT 1
                """,
                (norm_title,),
            ).fetchone()
        if row is None and title:
            row = db.conn.execute(
                "SELECT id AS document_id FROM documents WHERE lower(filename) = ? LIMIT 1",
                (title.lower(),),
            ).fetchone()
        if row is None:
            unresolved += 1
            unresolved_entries.append(
                {
                    "title": title,
                    "author": author,
                    "year": year,
                    "doi": doi,
                }
            )
            continue
        doc_id = int(row["document_id"])
        matched += 1
        if title:
            planned_updates += 1
            if not dry_run:
                db.set_document_metadata(doc_id, "title", title, source="import", confidence=1.0, updated_by="refs")
                applied_updates += 1
        if author:
            planned_updates += 1
            if not dry_run:
                db.set_document_metadata(
                    doc_id, "author", author, source="import", confidence=1.0, updated_by="refs"
                )
                applied_updates += 1
        if year:
            planned_updates += 1
            if not dry_run:
                db.set_document_metadata(doc_id, "year", year, source="import", confidence=1.0, updated_by="refs")
                applied_updates += 1
        if doi:
            if not dry_run:
                db.upsert_document_citation(
                    doc_id,
                    citation_type="doi",
                    citation_value=doi,
                    status="resolved",
                    resolved_value=doi,
                )
    unresolved_written = 0
    if unresolved_output is not None:
        unresolved_payload = {
            "format": fmt,
            "input": str(input_path),
            "unresolved_entries": unresolved_entries,
        }
        unresolved_output.parent.mkdir(parents=True, exist_ok=True)
        unresolved_output.write_text(json.dumps(unresolved_payload, indent=2), encoding="utf-8")
        unresolved_written = len(unresolved_entries)
    db.close()
    payload = {
        "format": fmt,
        "input": str(input_path),
        "dry_run": bool(dry_run),
        "entries": len(entries),
        "matched": matched,
        "planned_metadata_updates": planned_updates,
        "metadata_updates": applied_updates,
        "unresolved_entries": unresolved,
        "unresolved_output": str(unresolved_output) if unresolved_output is not None else None,
        "unresolved_written": unresolved_written,
    }
    if as_json:
        _emit_json(payload)
        return
    console.print(
        f"[green]References imported:[/green] entries={len(entries)} matched={matched} "
        f"planned_updates={planned_updates} updated={applied_updates} unresolved={unresolved}"
    )


@main.group()
def templates():
    """Community document templates for common document types."""


@templates.command("list")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def templates_list(as_json: bool):
    """List built-in community templates."""
    from localarchive.core.templates import list_templates

    rows = list_templates()
    if as_json:
        _emit_json(
            {
                "count": len(rows),
                "templates": [
                    {
                        "id": t.template_id,
                        "title": t.title,
                        "description": t.description,
                        "tags": list(t.tags),
                    }
                    for t in rows
                ],
            }
        )
        return
    table = Table(title="Community Template Library")
    table.add_column("ID", style="cyan", width=20)
    table.add_column("Title", style="bold")
    table.add_column("Description")
    table.add_column("Tags", width=24)
    for item in rows:
        table.add_row(item.template_id, item.title, item.description, ", ".join(item.tags))
    console.print(table)


@templates.command("apply")
@click.option("--template", "template_id", required=True, help="Template ID to apply.")
@click.option("--doc-id", type=int, default=None, help="Apply to a single document ID.")
@click.option("--all", "apply_all", is_flag=True, help="Apply to matching documents in the archive.")
@click.option("--limit", default=1000, type=int, help="Max documents to scan with --all.")
@click.option("--dry-run", is_flag=True, help="Show matches without writing tags.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def templates_apply(
    template_id: str,
    doc_id: int | None,
    apply_all: bool,
    limit: int,
    dry_run: bool,
    as_json: bool,
):
    """Apply a template's tag set to matching documents."""
    from localarchive.core.templates import get_template, matches_template

    _validate_limit(limit)
    if apply_all and doc_id is not None:
        raise CLIError("Use either --doc-id or --all, not both.", exit_code=2)
    if not apply_all and doc_id is None:
        raise CLIError("Specify --doc-id or --all.", exit_code=2)
    template = get_template(template_id)
    if template is None:
        raise CLIError(f"Template not found: {template_id}", exit_code=2)

    config = get_config()
    db = get_db(config)
    candidates: list[dict] = []
    if doc_id is not None:
        doc = db.get_document(doc_id)
        if not doc:
            db.close()
            raise CLIError(f"Document {doc_id} not found.", exit_code=2)
        candidates = [doc]
    else:
        candidates = db.list_documents(limit=limit)

    matched: list[dict] = []
    for doc in candidates:
        if matches_template(
            template,
            filename=str(doc.get("filename", "")),
            text=str(doc.get("ocr_text", "")),
        ):
            matched.append({"id": int(doc["id"]), "filename": str(doc.get("filename", ""))})
            if not dry_run:
                for tag in template.tags:
                    db.add_tag(int(doc["id"]), tag)
    db.close()
    payload = {
        "template": template.template_id,
        "dry_run": bool(dry_run),
        "scanned": len(candidates),
        "matched": len(matched),
        "tag_count": len(template.tags),
        "documents": matched,
    }
    if as_json:
        _emit_json(payload)
        return
    if dry_run:
        console.print(
            f"[yellow]Template dry run:[/yellow] {template.template_id} matched {len(matched)} of {len(candidates)} document(s)."
        )
    else:
        console.print(
            f"[green]Template applied:[/green] {template.template_id} matched {len(matched)} of {len(candidates)} document(s)."
        )
    if matched:
        sample = ", ".join(f"{m['id']}:{m['filename']}" for m in matched[:5])
        console.print(f"[dim]Matches: {sample}[/dim]")


@connectors.command("imap")
@click.option("--host", required=True, help="IMAP host (for example: imap.gmail.com).")
@click.option("--username", required=True, help="IMAP username or email address.")
@click.option(
    "--password",
    default="",
    help="IMAP password or app-password. Falls back to LOCALARCHIVE_IMAP_PASSWORD.",
)
@click.option("--mailbox", default="INBOX", show_default=True, help="Mailbox/folder name.")
@click.option("--unseen/--all", default=True, show_default=True, help="Only fetch unseen emails.")
@click.option("--limit", default=25, show_default=True, type=int, help="Max messages to inspect.")
@click.option("--dry-run", is_flag=True, help="Inspect and report without ingesting attachments.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def connectors_imap(
    host: str,
    username: str,
    password: str,
    mailbox: str,
    unseen: bool,
    limit: int,
    dry_run: bool,
    as_json: bool,
):
    """Ingest supported attachments from an IMAP mailbox."""
    from localarchive.cli_commands.connectors_cmd import run_connectors_imap

    run_connectors_imap(
        host=host,
        username=username,
        password=password,
        mailbox=mailbox,
        unseen=unseen,
        limit=limit,
        dry_run=dry_run,
        as_json=as_json,
    )


@sync.command("snapshot")
@click.option(
    "--output", "output_path", type=click.Path(dir_okay=False, path_type=Path), required=True
)
@click.option("--limit", default=50000, type=int, help="Max documents to include.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def sync_snapshot(output_path: Path, limit: int, as_json: bool):
    """Export a metadata snapshot for offline device-to-device sync."""
    _validate_limit(limit)
    config = get_config()
    db = get_db(config)
    docs = db.list_documents(limit=limit)
    payload_docs = []
    for doc in docs:
        payload_docs.append(
            {
                "file_hash": str(doc.get("file_hash", "")),
                "filename": str(doc.get("filename", "")),
                "status": str(doc.get("status", "")),
                "tags": db.get_tags(int(doc["id"])),
                "metadata": db.get_document_metadata(int(doc["id"])),
                "notes": db.get_metadata_notes(int(doc["id"])),
            }
        )
    db.close()
    bundle = {
        "format": "localarchive.sync.snapshot.v1",
        "doc_count": len(payload_docs),
        "docs": payload_docs,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8")
    if as_json:
        _emit_json({"written": True, "path": str(output_path), "doc_count": len(payload_docs)})
        return
    console.print(f"[green]Sync snapshot written:[/green] {output_path} ({len(payload_docs)} docs)")


@sync.command("merge")
@click.option(
    "--input", "input_path", type=click.Path(dir_okay=False, exists=True, path_type=Path), required=True
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def sync_merge(input_path: Path, as_json: bool):
    """Merge a sync snapshot into local metadata using CRDT-style tag union."""
    config = get_config()
    db = get_db(config)
    raw = json.loads(input_path.read_text(encoding="utf-8"))
    docs = list(raw.get("docs") or [])
    matched = 0
    updated = 0
    unmatched = 0
    notes_added = 0
    metadata_updated = 0
    for entry in docs:
        file_hash_val = str(entry.get("file_hash", "")).strip()
        if not file_hash_val:
            continue
        row = db.conn.execute(
            "SELECT id FROM documents WHERE file_hash = ? LIMIT 1",
            (file_hash_val,),
        ).fetchone()
        if not row:
            unmatched += 1
            continue
        matched += 1
        doc_id = int(row["id"])
        existing = set(db.get_tags(doc_id))
        incoming = {str(tag).strip() for tag in (entry.get("tags") or []) if str(tag).strip()}
        merged = existing | incoming
        if merged != existing:
            db.set_tags(doc_id, sorted(merged))
            updated += 1
        existing_meta = db.get_document_metadata(doc_id)
        incoming_meta = dict(entry.get("metadata") or {})
        for key, row in incoming_meta.items():
            meta_key = str(key).strip().lower()
            if not meta_key:
                continue
            incoming_updated = str(row.get("updated_at", "")).strip()
            local_updated = str(existing_meta.get(meta_key, {}).get("updated_at", "")).strip()
            if local_updated and incoming_updated and incoming_updated < local_updated:
                continue
            db.set_document_metadata(
                doc_id=doc_id,
                key=meta_key,
                value=str(row.get("value", "")),
                source=str(row.get("source", "sync") or "sync"),
                confidence=float(row.get("confidence", 1.0) or 1.0),
                updated_by=str(row.get("updated_by", "sync") or "sync"),
                updated_at=incoming_updated or None,
            )
            metadata_updated += 1
        existing_notes = {
            (str(n.get("note", "")), str(n.get("updated_at", "")))
            for n in db.get_metadata_notes(doc_id)
        }
        for note in list(entry.get("notes") or []):
            note_text = str(note.get("note", "")).strip()
            note_updated = str(note.get("updated_at", "")).strip()
            key = (note_text, note_updated)
            if not note_text or key in existing_notes:
                continue
            db.add_metadata_note(doc_id, note_text, updated_at=note_updated or None)
            existing_notes.add(key)
            notes_added += 1
    db.close()
    payload = {
        "merged": True,
        "path": str(input_path),
        "incoming_docs": len(docs),
        "matched_docs": matched,
        "updated_docs": updated,
        "unmatched_docs": unmatched,
        "metadata_updates": metadata_updated,
        "notes_added": notes_added,
    }
    if as_json:
        _emit_json(payload)
        return
    console.print(
        f"[green]Sync merge complete:[/green] matched={matched} updated={updated} unmatched={unmatched}"
    )


@sync.command("export-log")
@click.option(
    "--output", "output_path", type=click.Path(dir_okay=False, path_type=Path), required=True
)
@click.option("--since", default="", help="Opaque cursor timestamp (ISO).")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def sync_export_log(output_path: Path, since: str, as_json: bool):
    """Export a log-style sync bundle (cursor-aware snapshot)."""
    config = get_config()
    db = get_db(config)
    docs = db.list_documents(limit=50000)
    filtered = []
    for doc in docs:
        updated_at = str(doc.get("updated_at", ""))
        if since and updated_at and updated_at <= since:
            continue
        filtered.append(doc)
    payload_docs = []
    for doc in filtered:
        payload_docs.append(
            {
                "file_hash": str(doc.get("file_hash", "")),
                "filename": str(doc.get("filename", "")),
                "updated_at": str(doc.get("updated_at", "")),
                "tags": db.get_tags(int(doc["id"])),
                "metadata": db.get_document_metadata(int(doc["id"])),
                "notes": db.get_metadata_notes(int(doc["id"])),
            }
        )
    db.close()
    cursor = max([str(d.get("updated_at", "")) for d in filtered], default=since or "")
    bundle = {
        "format": "localarchive.sync.log.v1",
        "cursor": cursor,
        "since": since,
        "count": len(payload_docs),
        "docs": payload_docs,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8")
    payload = {"written": True, "path": str(output_path), "count": len(payload_docs), "cursor": cursor}
    if as_json:
        _emit_json(payload)
        return
    console.print(f"[green]Sync log written:[/green] {output_path} count={len(payload_docs)}")


@sync.command("import-log")
@click.option(
    "--path", "input_path", type=click.Path(dir_okay=False, exists=True, path_type=Path), required=True
)
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def sync_import_log(input_path: Path, as_json: bool):
    """Import a log-style sync bundle."""
    raw = json.loads(input_path.read_text(encoding="utf-8"))
    docs = list(raw.get("docs") or [])
    config = get_config()
    db = get_db(config)
    matched = 0
    updated = 0
    unmatched = 0
    notes_added = 0
    metadata_updated = 0
    for entry in docs:
        file_hash_val = str(entry.get("file_hash", "")).strip()
        if not file_hash_val:
            continue
        row = db.conn.execute(
            "SELECT id FROM documents WHERE file_hash = ? LIMIT 1",
            (file_hash_val,),
        ).fetchone()
        if not row:
            unmatched += 1
            continue
        matched += 1
        doc_id = int(row["id"])
        existing = set(db.get_tags(doc_id))
        incoming = {str(tag).strip() for tag in (entry.get("tags") or []) if str(tag).strip()}
        merged = existing | incoming
        if merged != existing:
            db.set_tags(doc_id, sorted(merged))
            updated += 1
        existing_meta = db.get_document_metadata(doc_id)
        incoming_meta = dict(entry.get("metadata") or {})
        for key, meta_row in incoming_meta.items():
            meta_key = str(key).strip().lower()
            if not meta_key:
                continue
            incoming_updated = str(meta_row.get("updated_at", "")).strip()
            local_updated = str(existing_meta.get(meta_key, {}).get("updated_at", "")).strip()
            if local_updated and incoming_updated and incoming_updated < local_updated:
                continue
            db.set_document_metadata(
                doc_id=doc_id,
                key=meta_key,
                value=str(meta_row.get("value", "")),
                source=str(meta_row.get("source", "sync") or "sync"),
                confidence=float(meta_row.get("confidence", 1.0) or 1.0),
                updated_by=str(meta_row.get("updated_by", "sync") or "sync"),
                updated_at=incoming_updated or None,
            )
            metadata_updated += 1
        existing_notes = {
            (str(n.get("note", "")), str(n.get("updated_at", "")))
            for n in db.get_metadata_notes(doc_id)
        }
        for note in list(entry.get("notes") or []):
            note_text = str(note.get("note", "")).strip()
            note_updated = str(note.get("updated_at", "")).strip()
            key = (note_text, note_updated)
            if not note_text or key in existing_notes:
                continue
            db.add_metadata_note(doc_id, note_text, updated_at=note_updated or None)
            existing_notes.add(key)
            notes_added += 1
    db.close()
    payload = {
        "imported": True,
        "source": str(input_path),
        "cursor": str(raw.get("cursor", "")),
        "matched_docs": matched,
        "updated_docs": updated,
        "unmatched_docs": unmatched,
        "metadata_updates": metadata_updated,
        "notes_added": notes_added,
    }
    if as_json:
        _emit_json(payload)
        return
    console.print(f"[green]Sync log imported:[/green] {input_path}")


@sync.command("status")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable output.")
def sync_status(as_json: bool):
    """Show local sync-related metadata counts."""
    config = get_config()
    db = get_db(config)
    docs = db.conn.execute("SELECT COUNT(*) AS c FROM documents").fetchone()
    metadata_rows = db.conn.execute("SELECT COUNT(*) AS c FROM document_metadata").fetchone()
    notes_rows = db.conn.execute("SELECT COUNT(*) AS c FROM metadata_notes").fetchone()
    citations_rows = db.conn.execute(
        "SELECT COUNT(*) AS c FROM document_citations WHERE status = 'unresolved'"
    ).fetchone()
    latest = db.conn.execute("SELECT MAX(updated_at) AS ts FROM documents").fetchone()
    db.close()
    payload = {
        "documents": int(docs["c"]) if docs else 0,
        "metadata_rows": int(metadata_rows["c"]) if metadata_rows else 0,
        "notes_rows": int(notes_rows["c"]) if notes_rows else 0,
        "unresolved_citations": int(citations_rows["c"]) if citations_rows else 0,
        "latest_cursor": str(latest["ts"]) if latest and latest["ts"] else "",
    }
    if as_json:
        _emit_json(payload)
        return
    console.print(
        f"[green]Sync status:[/green] docs={payload['documents']} metadata={payload['metadata_rows']} "
        f"notes={payload['notes_rows']} unresolved_citations={payload['unresolved_citations']}"
    )


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


@main.command()
def gui():
    """Launch desktop GUI starter for the local web UI."""
    from localarchive.gui_launcher import launch_gui

    launch_gui()


if __name__ == "__main__":
    try:
        main()
    except CLIError as e:
        console.print(f"[red]{e.message}[/red]")
        raise click.exceptions.Exit(e.exit_code)
