"""Connector command implementations."""

import contextlib
import email
import io
import os
import tempfile
from pathlib import Path

from localarchive import cli as c
from localarchive.core.ingester import Ingester
from localarchive.utils import is_supported, safe_filename


def run_connectors_imap(
    *,
    host: str,
    username: str,
    password: str,
    mailbox: str,
    unseen: bool,
    limit: int,
    dry_run: bool,
    as_json: bool,
) -> None:
    c._validate_limit(limit)
    resolved_password = password or str(os.environ.get("LOCALARCHIVE_IMAP_PASSWORD", ""))
    if not resolved_password:
        raise c.CLIError(
            "Missing IMAP password. Provide `--password` or set LOCALARCHIVE_IMAP_PASSWORD.",
            exit_code=2,
        )

    config = c.get_config()
    config.ensure_dirs()
    db = c.get_db(config)
    ingester = Ingester(config, db)
    inspected = 0
    attachments_seen = 0
    ingested = 0
    skipped = 0
    errors = 0

    imap = c.imaplib.IMAP4_SSL(host)
    try:
        imap.login(username, resolved_password)
        status, _ = imap.select(mailbox)
        if status != "OK":
            raise c.CLIError(f"Failed to open mailbox: {mailbox}", exit_code=3)
        criteria = "(UNSEEN)" if unseen else "ALL"
        status, data = imap.search(None, criteria)
        if status != "OK":
            raise c.CLIError("IMAP search failed.", exit_code=3)
        message_ids = (data[0] or b"").split()
        selected_ids = list(reversed(message_ids))[:limit]
        for msg_id in selected_ids:
            inspected += 1
            status, payload = imap.fetch(msg_id, "(RFC822)")
            if status != "OK" or not payload:
                errors += 1
                continue
            raw_email = payload[0][1] if isinstance(payload[0], tuple) and len(payload[0]) > 1 else b""
            if not raw_email:
                errors += 1
                continue
            msg = email.message_from_bytes(raw_email)
            for part in msg.walk():
                if part.get_content_disposition() != "attachment":
                    continue
                name = safe_filename(
                    Path(c._decode_mime_value(part.get_filename()) or "attachment.bin").name
                )
                if not name:
                    skipped += 1
                    continue
                if not is_supported(Path(name)):
                    skipped += 1
                    continue
                body = part.get_payload(decode=True) or b""
                attachments_seen += 1
                if dry_run:
                    continue
                fd, tmp_name = tempfile.mkstemp(
                    prefix="imap-",
                    suffix=Path(name).suffix.lower(),
                    dir=str(config.runtime.tmp_dir),
                )
                tmp_path = Path(tmp_name)
                try:
                    with open(fd, "wb", closefd=True) as handle:
                        handle.write(body)
                    if as_json:
                        with contextlib.redirect_stdout(io.StringIO()):
                            ingester.ingest_path(tmp_path, source_name=name)
                    else:
                        ingester.ingest_path(tmp_path, source_name=name)
                    ingested += 1
                except Exception:
                    errors += 1
                finally:
                    tmp_path.unlink(missing_ok=True)
    finally:
        try:
            imap.logout()
        except Exception:
            pass
        db.close()

    summary = {
        "mailbox": mailbox,
        "inspected_messages": inspected,
        "attachments_seen": attachments_seen,
        "ingested": ingested,
        "skipped": skipped,
        "errors": errors,
        "dry_run": dry_run,
    }
    if as_json:
        c._emit_json(summary)
        return
    mode = "Dry run complete" if dry_run else "IMAP ingestion complete"
    c.console.print(
        f"[green]{mode}[/green] inspected={inspected} attachments={attachments_seen} "
        f"ingested={ingested} skipped={skipped} errors={errors}"
    )
