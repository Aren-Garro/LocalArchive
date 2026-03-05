"""Process command implementation."""

import concurrent.futures
import threading
import time
from pathlib import Path

from localarchive import cli as c


def run_process(
    *,
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
) -> None:
    from localarchive.core.extractor import extract_fields
    from localarchive.core.ocr_engine import (
        extract_text_from_pdf_native,
        get_ocr_engine,
        pdf_to_images,
    )
    from localarchive.core.table_extractor import extract_tables_from_text

    config = c.get_config()
    max_docs = limit if limit is not None else config.processing.default_limit
    c._validate_limit(max_docs)
    worker_count = workers if workers is not None else config.runtime.max_workers
    c._validate_limit(worker_count)
    commit_size = (
        commit_batch_size if commit_batch_size is not None else config.processing.commit_batch_size
    )
    c._validate_limit(commit_size)
    checkpoint_size = (
        checkpoint_every if checkpoint_every is not None else config.reliability.checkpoint_batch_size
    )
    c._validate_limit(checkpoint_size)
    max_error_budget = max_errors if max_errors is not None else config.processing.max_errors_per_run
    c._validate_limit(max_error_budget)
    resolved_ocr_languages = c._parse_ocr_languages(ocr_languages, config.ocr.languages)
    selected_ocr_engine = ocr_engine_override or config.ocr.engine
    if selected_ocr_engine not in {"paddleocr", "easyocr"}:
        raise c.CLIError(
            "Unsupported OCR engine. Choose from: paddleocr, easyocr.",
            exit_code=2,
        )
    if selected_ocr_engine == "paddleocr" and any(lang != "en" for lang in resolved_ocr_languages):
        raise c.CLIError(
            "PaddleOCR run override currently supports only `en` in LocalArchive. "
            "Use `--ocr-engine easyocr` for multi-language OCR (en/es/fr/de/zh/ar).",
            exit_code=2,
        )
    config.ocr.engine = selected_ocr_engine
    config.ocr.languages = resolved_ocr_languages
    db = c.get_db(config)
    c._run_integrity_check_if_enabled(config, db, "process")
    resume_run = None
    after_doc_id = 0
    if from_run is not None:
        resume_run = db.get_processing_run(from_run)
        if not resume_run:
            db.close()
            raise c.CLIError(f"Processing run {from_run} not found.", exit_code=2)
    elif resume:
        resume_run = db.latest_processing_run()
    if resume_run:
        after_doc_id = int(resume_run.get("checkpoint_doc_id") or 0)
        c.console.print(
            f"[dim]Resuming from run {resume_run.get('id')} with checkpoint_doc_id={after_doc_id}[/dim]"
        )
    elif resume or from_run is not None:
        c.console.print("[yellow]No checkpointed run found; starting from earliest pending document.[/yellow]")
    pending = db.list_documents_for_processing(limit=max_docs, after_doc_id=after_doc_id)
    if not pending:
        if as_json:
            c._emit_json(
                {
                    "run_id": None,
                    "status": "noop",
                    "processed": 0,
                    "errors": 0,
                    "aborted_reason": "",
                    "ocr_engine": selected_ocr_engine,
                    "ocr_languages": resolved_ocr_languages,
                    "extract_tables": bool(extract_tables),
                    "checkpoint_doc_id": after_doc_id,
                    "total_candidates": 0,
                }
            )
        else:
            c.console.print("[dim]No documents pending OCR for the selected scope.[/dim]")
            c.console.print("[dim]Hint: run `localarchive ingest <file_or_folder>` first.[/dim]")
        db.close()
        return
    if dry_run:
        doc_ids = [int(doc["id"]) for doc in pending]
        if as_json:
            c._emit_json(
                {
                    "dry_run": True,
                    "count": len(doc_ids),
                    "doc_ids": doc_ids,
                    "ocr_engine": selected_ocr_engine,
                    "ocr_languages": resolved_ocr_languages,
                    "extract_tables": bool(extract_tables),
                    "resumed_from_run": int(resume_run.get("id")) if resume_run else None,
                    "start_after_doc_id": after_doc_id,
                }
            )
        else:
            c.console.print(f"[yellow]Dry run:[/yellow] would process {len(doc_ids)} document(s): {doc_ids}")
        db.close()
        return
    mode = extractor_mode or config.extraction.strategy
    run_id = db.start_processing_run(engine=selected_ocr_engine, extractor=mode)
    db.add_processing_event(
        run_id,
        document_id=None,
        event_type="config",
        message=f"ocr_engine={selected_ocr_engine}",
    )
    db.add_processing_event(
        run_id,
        document_id=None,
        event_type="config",
        message=f"ocr_languages={','.join(resolved_ocr_languages)}",
    )
    db.add_processing_event(
        run_id,
        document_id=None,
        event_type="config",
        message=f"extract_tables={str(bool(extract_tables)).lower()}",
    )
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
    c.console.print(f"Processing [bold]{len(pending)}[/bold] documents...\n")
    if config.runtime.fail_fast and worker_count > 1:
        c.console.print(
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
            table_dicts = extract_tables_from_text(full_text) if extract_tables else []
            return {
                "doc_id": doc["id"],
                "filename": doc["filename"],
                "full_text": full_text,
                "fields": field_dicts,
                "tables": table_dicts,
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
            for item in success_buffer:
                db.set_tables(int(item["doc_id"]), item.get("tables") or [])
            success_buffer.clear()
        if error_buffer:
            db.record_processing_errors_batch(error_buffer, max_retries=config.reliability.max_retries)
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
                c.console.print(
                    f"  -> {filename}... [red]error[/red]: {error} "
                    f"(attempt {attempts_by_doc[doc_id]}/{config.reliability.max_retries}, max retries exceeded)"
                )
            else:
                c.console.print(
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
                    "message": f"{len(result['fields'])} fields, {len(result.get('tables') or [])} tables",
                }
            )
            processed_count += 1
            c.console.print(
                f"  -> {filename}... [green]done[/green] "
                f"({len(result['fields'])} fields, {len(result.get('tables') or [])} tables)"
            )
        completed_count += 1
        if completed_count % checkpoint_size == 0:
            c.console.print(
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
        c._emit_json(
            {
                "run_id": run_id,
                "status": final_status,
                "processed": processed_count,
                "errors": error_count,
                "aborted_reason": aborted_reason,
                "ocr_engine": selected_ocr_engine,
                "ocr_languages": resolved_ocr_languages,
                "extract_tables": bool(extract_tables),
                "checkpoint_doc_id": int(run_meta.get("checkpoint_doc_id", 0) or 0),
                "total_candidates": len(pending),
                "resumed_from_run": int(resume_run.get("id")) if resume_run else None,
                "start_after_doc_id": after_doc_id,
            }
        )
    elif aborted_reason:
        c.console.print(f"\n[yellow]Processing aborted:[/yellow] {aborted_reason}")
    else:
        c.console.print("\n[green]Processing complete.[/green]")
