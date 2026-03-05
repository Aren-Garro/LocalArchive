"""Integration tests for CLI wiring."""

import json
import sys
import tempfile
import time
import types
import uuid
import zipfile
from email.message import EmailMessage
from pathlib import Path

import pytest
from click.testing import CliRunner

from localarchive.cli import main
from localarchive.config import Config
from localarchive.db.database import Database


class _FakeOCR:
    def extract_text(self, image_path: Path) -> list[dict]:
        return [{"text": "Acme invoice $42.00 contact@example.com", "confidence": 0.99, "bbox": []}]


def _workspace_tmp_dir(prefix: str) -> Path:
    root = Path.cwd() / ".test_tmp"
    root.mkdir(exist_ok=True)
    path = root / f"{prefix}-{uuid.uuid4().hex[:8]}"
    path.mkdir(exist_ok=True)
    return path


def test_cli_lifecycle(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-cli")
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=archive_dir, db_path=db_path)

    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _FakeOCR(),
        pdf_to_images=lambda _path: [],
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    runner = CliRunner()
    source_file = tmp_path / "invoice.png"
    source_file.write_bytes(b"fake-png-content")

    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0

    result = runner.invoke(main, ["ingest", str(source_file)])
    assert result.exit_code == 0
    assert "Ingested:" in result.output

    result = runner.invoke(main, ["process", "--extractor", "regex"])
    assert result.exit_code == 0
    assert "Processing complete." in result.output

    result = runner.invoke(main, ["search", "Acme"])
    assert result.exit_code == 0
    assert "invoice.png" in result.output

    result = runner.invoke(main, ["tag", "1", "finance"])
    assert result.exit_code == 0

    export_csv = tmp_path / "out.csv"
    result = runner.invoke(main, ["export", "--format", "csv", "--output", str(export_csv)])
    assert result.exit_code == 0
    assert export_csv.exists()


def test_watch_once(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-watch")
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=archive_dir, db_path=db_path)
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    runner = CliRunner()
    watch_dir = tmp_path / "incoming"
    watch_dir.mkdir()
    (watch_dir / "doc.png").write_bytes(b"x")

    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0

    result = runner.invoke(main, ["watch", str(watch_dir), "--once"])
    assert result.exit_code == 0
    assert "Watcher finished" in result.output

    db = Database(db_path)
    db.initialize()
    docs = db.list_documents(limit=10)
    db.close()
    assert len(docs) == 1


def test_reprocess_flow(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-reprocess")
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=archive_dir, db_path=db_path)
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(db_path)
    db.initialize()
    doc_id = db.insert_document(
        filename="broken.pdf",
        filepath="/tmp/broken.pdf",
        file_hash="broken-hash",
        file_type="pdf",
        file_size=100,
        ingested_at="2026-01-01T00:00:00Z",
        status="error",
        error_message="ocr failed",
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["reprocess", "--status", "error", "--dry-run"])
    assert result.exit_code == 0
    assert "Dry run" in result.output

    result = runner.invoke(main, ["reprocess", "--status", "error"])
    assert result.exit_code == 0
    assert "Marked 1 document" in result.output

    db = Database(db_path)
    db.initialize()
    doc = db.get_document(doc_id)
    db.close()
    assert doc["status"] == "pending_ocr"
    assert doc["error_message"] == ""


def test_doctor_failure_exit_code(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-doctor")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    monkeypatch.setattr("importlib.util.find_spec", lambda _name: None)

    runner = CliRunner()
    result = runner.invoke(main, ["doctor"])
    assert result.exit_code == 3


def test_ingest_research_profile(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-profile")
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=archive_dir, db_path=db_path)
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    runner = CliRunner()
    source_file = tmp_path / "paper.pdf"
    source_file.write_bytes(b"%PDF-1.4 fake")

    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0
    result = runner.invoke(main, ["ingest", str(source_file), "--profile", "research"])
    assert result.exit_code == 0

    db = Database(db_path)
    db.initialize()
    tags = db.get_tags(1)
    db.close()
    assert "research" in tags


def test_collections_timeline_backup_and_audit(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-ops")
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=archive_dir, db_path=db_path)
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(db_path)
    db.initialize()
    doc_id = db.insert_document(
        filename="timeline.pdf",
        filepath=str(tmp_path / "timeline.pdf"),
        file_hash="timelinehash",
        file_type="pdf",
        file_size=100,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="Entity Org in 2024",
    )
    (tmp_path / "timeline.pdf").write_bytes(b"not-real-pdf")
    db.insert_fields(
        doc_id,
        [{"field_type": "entity_org", "value": "ACME Lab", "raw_match": "ACME Lab", "start": 0}],
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["collections", "auto-build"])
    assert result.exit_code == 0

    result = runner.invoke(main, ["collections", "list"])
    assert result.exit_code == 0
    assert "Research PDFs" in result.output

    result = runner.invoke(main, ["timeline", "--entity", "topic"])
    assert result.exit_code == 0
    assert "ACME Lab" in result.output

    backup_path = tmp_path / "backup.zip"
    result = runner.invoke(main, ["backup", "create", "--path", str(backup_path)])
    assert result.exit_code == 0
    assert backup_path.exists()
    result = runner.invoke(main, ["backup", "list", "--json"])
    assert result.exit_code == 0
    assert '"backups"' in result.output

    result = runner.invoke(main, ["audit"])
    assert result.exit_code in (0, 4)


def test_backup_retention_keeps_newest_only(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-backup-retention")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.reliability.backup_retention_count = 1
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.close()

    runner = CliRunner()
    backup1 = tmp_path / "backup1.zip"
    backup2 = tmp_path / "backup2.zip"
    result = runner.invoke(main, ["backup", "create", "--path", str(backup1)])
    assert result.exit_code == 0
    assert backup1.exists()
    result = runner.invoke(main, ["backup", "create", "--path", str(backup2)])
    assert result.exit_code == 0
    assert backup2.exists()
    assert not backup1.exists()

    result = runner.invoke(main, ["backup", "list", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert int(payload.get("count", 0)) == 1
    assert payload["backups"][0]["path"].endswith("backup2.zip")
    assert payload["backups"][0]["exists"] is True


def test_backup_create_json_summary(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-backup-create-json")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.reliability.backup_retention_count = 1
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    config.archive_dir.mkdir(parents=True, exist_ok=True)
    (config.archive_dir / "doc.txt").write_text("hello")
    db = Database(config.db_path)
    db.initialize()
    db.close()

    runner = CliRunner()
    backup1 = tmp_path / "one.zip"
    backup2 = tmp_path / "two.zip"
    result = runner.invoke(main, ["backup", "create", "--path", str(backup1), "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["created"] is True
    assert payload["path"].endswith("one.zip")
    assert payload["archive_file_count"] >= 1
    assert payload["pruned_count"] == 0

    result = runner.invoke(main, ["backup", "create", "--path", str(backup2), "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["created"] is True
    assert payload["path"].endswith("two.zip")
    assert payload["archive_file_count"] >= 1
    assert payload["pruned_count"] >= 1


def test_connectors_imap_dry_run_json(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-imap-dry")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.ensure_dirs()
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    msg = EmailMessage()
    msg["Subject"] = "Invoice"
    msg["From"] = "sender@example.com"
    msg["To"] = "user@example.com"
    msg.set_content("Please see attachment.")
    msg.add_attachment(
        b"%PDF-1.4 fake attachment",
        maintype="application",
        subtype="pdf",
        filename="invoice.pdf",
    )
    raw = msg.as_bytes()

    class _FakeIMAP:
        def __init__(self, _host: str):
            self.logged_out = False

        def login(self, _username: str, _password: str):
            return ("OK", [b"logged in"])

        def select(self, _mailbox: str):
            return ("OK", [b"1"])

        def search(self, _charset, _criteria):
            return ("OK", [b"1"])

        def fetch(self, _msg_id, _parts):
            return ("OK", [(b"1 (RFC822 {100})", raw)])

        def logout(self):
            self.logged_out = True
            return ("BYE", [b"logout"])

    monkeypatch.setattr("localarchive.cli.imaplib.IMAP4_SSL", _FakeIMAP)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "connectors",
            "imap",
            "--host",
            "imap.example.com",
            "--username",
            "user@example.com",
            "--password",
            "secret",
            "--dry-run",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert int(payload["attachments_seen"]) == 1
    assert int(payload["ingested"]) == 0


def test_connectors_imap_ingests_supported_attachment(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-imap-live")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.ensure_dirs()
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    msg = EmailMessage()
    msg["Subject"] = "Receipt"
    msg["From"] = "sender@example.com"
    msg["To"] = "user@example.com"
    msg.set_content("Attachment included.")
    msg.add_attachment(
        b"%PDF-1.4 fake attachment",
        maintype="application",
        subtype="pdf",
        filename="receipt.pdf",
    )
    raw = msg.as_bytes()

    class _FakeIMAP:
        def __init__(self, _host: str):
            pass

        def login(self, _username: str, _password: str):
            return ("OK", [b"logged in"])

        def select(self, _mailbox: str):
            return ("OK", [b"1"])

        def search(self, _charset, _criteria):
            return ("OK", [b"1"])

        def fetch(self, _msg_id, _parts):
            return ("OK", [(b"1 (RFC822 {100})", raw)])

        def logout(self):
            return ("BYE", [b"logout"])

    monkeypatch.setattr("localarchive.cli.imaplib.IMAP4_SSL", _FakeIMAP)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "connectors",
            "imap",
            "--host",
            "imap.example.com",
            "--username",
            "user@example.com",
            "--password",
            "secret",
            "--all",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["dry_run"] is False
    assert int(payload["ingested"]) == 1

    db = Database(config.db_path)
    db.initialize()
    docs = db.list_documents(limit=10)
    db.close()
    assert len(docs) == 1
    assert docs[0]["filename"] == "receipt.pdf"


def test_connectors_imap_skips_oversized_message(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-imap-message-limit")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.ensure_dirs()
    config.reliability.max_imap_message_bytes = 50
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    raw = b"X" * 200

    class _FakeIMAP:
        def __init__(self, _host: str):
            pass

        def login(self, _username: str, _password: str):
            return ("OK", [b"logged in"])

        def select(self, _mailbox: str):
            return ("OK", [b"1"])

        def search(self, _charset, _criteria):
            return ("OK", [b"1"])

        def fetch(self, _msg_id, _parts):
            if _parts == "(RFC822.SIZE)":
                return ("OK", [(b"1 (RFC822.SIZE 200)", b"")])
            return ("OK", [(b"1 (RFC822 {200})", raw)])

        def logout(self):
            return ("BYE", [b"logout"])

    monkeypatch.setattr("localarchive.cli.imaplib.IMAP4_SSL", _FakeIMAP)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "connectors",
            "imap",
            "--host",
            "imap.example.com",
            "--username",
            "user@example.com",
            "--password",
            "secret",
            "--all",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert int(payload["inspected_messages"]) == 1
    assert int(payload["ingested"]) == 0
    assert int(payload["skipped"]) >= 1


def test_connectors_imap_skips_oversized_attachment(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-imap-attachment-limit")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.ensure_dirs()
    config.reliability.max_imap_attachment_bytes = 8
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    msg = EmailMessage()
    msg["Subject"] = "Receipt"
    msg["From"] = "sender@example.com"
    msg["To"] = "user@example.com"
    msg.set_content("Attachment included.")
    msg.add_attachment(
        b"%PDF-1.4 larger-than-limit",
        maintype="application",
        subtype="pdf",
        filename="receipt.pdf",
    )
    raw = msg.as_bytes()

    class _FakeIMAP:
        def __init__(self, _host: str):
            pass

        def login(self, _username: str, _password: str):
            return ("OK", [b"logged in"])

        def select(self, _mailbox: str):
            return ("OK", [b"1"])

        def search(self, _charset, _criteria):
            return ("OK", [b"1"])

        def fetch(self, _msg_id, _parts):
            if _parts == "(RFC822.SIZE)":
                return ("OK", [(b"1 (RFC822.SIZE 200)", b"")])
            return ("OK", [(b"1 (RFC822 {200})", raw)])

        def logout(self):
            return ("BYE", [b"logout"])

    monkeypatch.setattr("localarchive.cli.imaplib.IMAP4_SSL", _FakeIMAP)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "connectors",
            "imap",
            "--host",
            "imap.example.com",
            "--username",
            "user@example.com",
            "--password",
            "secret",
            "--all",
            "--json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert int(payload["attachments_seen"]) == 1
    assert int(payload["ingested"]) == 0
    assert int(payload["skipped"]) >= 1


def test_backup_create_dry_run_json_no_side_effects(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-backup-create-dry-run")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.reliability.backup_retention_count = 1
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    config.archive_dir.mkdir(parents=True, exist_ok=True)
    (config.archive_dir / "doc.txt").write_text("hello")
    db = Database(config.db_path)
    db.initialize()
    existing_backup = tmp_path / "existing.zip"
    with zipfile.ZipFile(existing_backup, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("archive.db", "x")
    db.record_backup(path=str(existing_backup), db_hash="", archive_file_count=0, verified=False)
    db.close()

    target = tmp_path / "dry-run.zip"
    runner = CliRunner()
    result = runner.invoke(main, ["backup", "create", "--path", str(target), "--dry-run", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert payload["path"].endswith("dry-run.zip")
    assert payload["archive_file_count"] >= 1
    assert payload["would_prune_count"] >= 1
    assert target.exists() is False

    verify_db = Database(config.db_path)
    rows = verify_db.list_backups(limit=10)
    verify_db.close()
    assert len(rows) == 1
    assert rows[0]["path"].endswith("existing.zip")


def test_backup_list_prune_missing(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-backup-prune")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.record_backup(
        path=str(tmp_path / "ghost.zip"), db_hash="", archive_file_count=0, verified=False
    )
    db.close()

    runner = CliRunner()
    before = runner.invoke(main, ["backup", "list", "--json"])
    assert before.exit_code == 0
    assert '"count": 1' in before.output
    assert '"exists": false' in before.output

    after = runner.invoke(main, ["backup", "list", "--json", "--prune-missing"])
    assert after.exit_code == 0
    assert '"count": 0' in after.output


def test_backup_list_missing_only(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-backup-missing-only")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    present = tmp_path / "present.zip"
    present.write_bytes(b"x")

    db = Database(config.db_path)
    db.initialize()
    db.record_backup(path=str(present), db_hash="", archive_file_count=0, verified=False)
    db.record_backup(
        path=str(tmp_path / "ghost.zip"), db_hash="", archive_file_count=0, verified=False
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["backup", "list", "--json", "--missing-only"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert int(payload.get("count", 0)) == 1
    assert payload["backups"][0]["path"].endswith("ghost.zip")
    assert payload["backups"][0]["exists"] is False


def test_backup_restore_rejects_unsafe_paths(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-unsafe-backup")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    bad_zip = tmp_path / "bad.zip"
    with zipfile.ZipFile(bad_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("archive_data/../evil.txt", "bad")

    runner = CliRunner()
    result = runner.invoke(main, ["backup", "restore", "--path", str(bad_zip)])
    assert result.exit_code == 2


def test_backup_restore_rejects_large_entries(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-large-backup")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    monkeypatch.setattr("localarchive.cli.BACKUP_RESTORE_MAX_MEMBER_BYTES", 32)

    large_zip = tmp_path / "large.zip"
    with zipfile.ZipFile(large_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("archive_data/large.txt", "x" * 64)

    runner = CliRunner()
    result = runner.invoke(main, ["backup", "restore", "--path", str(large_zip)])
    assert result.exit_code == 4
    assert "entry too large" in result.output


def test_backup_restore_dry_run_summary(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-restore-dry-run")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    config.archive_dir.mkdir(parents=True, exist_ok=True)
    existing = config.archive_dir / "keep.txt"
    existing.write_text("old")
    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    config.db_path.write_text("old-db")

    backup_path = tmp_path / "restore.zip"
    with zipfile.ZipFile(backup_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("archive.db", "new-db")
        zf.writestr("archive_data/keep.txt", "new")
        zf.writestr("archive_data/new.txt", "new")

    runner = CliRunner()
    result = runner.invoke(
        main, ["backup", "restore", "--path", str(backup_path), "--dry-run", "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert payload["has_database"] is True
    assert payload["archive_files"] == 2
    assert payload["would_create"] == 1
    assert payload["would_overwrite"] == 1

    assert existing.read_text() == "old"
    assert config.db_path.read_text() == "old-db"


def test_backup_restore_latest_dry_run(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-restore-latest")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    old_backup = tmp_path / "old.zip"
    new_backup = tmp_path / "new.zip"
    with zipfile.ZipFile(old_backup, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("archive.db", "old-db")
    with zipfile.ZipFile(new_backup, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("archive.db", "new-db")
    db.record_backup(path=str(old_backup), db_hash="", archive_file_count=0, verified=False)
    db.record_backup(path=str(new_backup), db_hash="", archive_file_count=0, verified=False)
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["backup", "restore", "--latest", "--dry-run", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    assert str(payload["backup"]).endswith("new.zip")


def test_backup_restore_latest_requires_backups(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-restore-latest-empty")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["backup", "restore", "--latest"])
    assert result.exit_code == 2
    assert "No tracked backups found" in result.output


def test_search_semantic_respects_config_gate(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-semantic-gate")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.search.enable_semantic = False
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    # If hybrid path is called despite config gate, this test should fail.
    monkeypatch.setattr(
        "localarchive.cli.SearchEngine.search_hybrid",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("should not run hybrid")),
    )

    db = Database(config.db_path)
    db.initialize()
    db.insert_document(
        filename="x.pdf",
        filepath=str(tmp_path / "x.pdf"),
        file_hash="xhash",
        file_type="pdf",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="graph neural networks",
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["search", "graph", "--semantic"])
    assert result.exit_code == 0
    assert "Semantic search is disabled" in result.output


def test_search_semantic_weight_validation(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-semantic-weights")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.search.enable_semantic = True
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    runner = CliRunner()
    result = runner.invoke(main, ["search", "graph", "--semantic", "--bm25-weight", "-1"])
    assert result.exit_code == 2


def test_search_no_results_shows_hint(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-search-hint")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["search", "nothinghere"])
    assert result.exit_code == 0
    assert "No results found." in result.output
    assert "Hint: try `localarchive search" in result.output

    as_json = runner.invoke(main, ["search", "nothinghere", "--json"])
    assert as_json.exit_code == 0
    assert '"count": 0' in as_json.output
    assert '"results": []' in as_json.output


def test_process_parallel_workers_and_checkpoint(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-process-parallel")
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=archive_dir, db_path=db_path)
    config.runtime.max_workers = 4
    config.reliability.checkpoint_batch_size = 1
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _FakeOCR(),
        pdf_to_images=lambda _path, tmp_dir=None: [],
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    db = Database(db_path)
    db.initialize()
    for i in range(3):
        source = tmp_path / f"doc-{i}.png"
        source.write_bytes(b"fake-png-content")
        db.insert_document(
            filename=source.name,
            filepath=str(source),
            file_hash=f"hash-{i}",
            file_type="png",
            file_size=source.stat().st_size,
            ingested_at="2026-01-01T00:00:00Z",
            status="pending_ocr",
        )
    db.close()

    runner = CliRunner()
    result = runner.invoke(
        main, ["process", "--workers", "3", "--checkpoint-every", "1", "--extractor", "regex"]
    )
    assert result.exit_code == 0
    assert "Progress checkpoint" in result.output

    db = Database(db_path)
    db.initialize()
    docs = db.list_documents(status="processed", limit=10)
    db.close()
    assert len(docs) == 3


def test_process_empty_queue_shows_hint(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-process-hint")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["process"])
    assert result.exit_code == 0
    assert "No documents pending OCR" in result.output
    assert "Hint: run `localarchive ingest" in result.output

    as_json = runner.invoke(main, ["process", "--json"])
    assert as_json.exit_code == 0
    assert '"status": "noop"' in as_json.output
    assert '"total_candidates": 0' in as_json.output


def test_process_failure_tracks_retries_and_cleans_temp_files(monkeypatch):
    class _FailingOCR:
        def extract_text(self, image_path: Path) -> list[dict]:
            raise RuntimeError("forced ocr failure")

    tmp_path = _workspace_tmp_dir("localarchive-process-failure")
    archive_dir = tmp_path / "archive"
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=archive_dir, db_path=db_path)
    config.reliability.max_retries = 1
    config.runtime.tmp_dir = tmp_path / "tmp"
    config.runtime.tmp_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    temp_images: list[Path] = []

    def _fake_pdf_to_images(_path: Path, tmp_dir: Path | None = None) -> list[Path]:
        root = tmp_dir or tmp_path
        image = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".png", dir=str(root)).name)
        image.write_bytes(b"png")
        temp_images.append(image)
        return [image]

    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _FailingOCR(),
        pdf_to_images=_fake_pdf_to_images,
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    db = Database(db_path)
    db.initialize()
    broken_pdf = tmp_path / "broken.pdf"
    broken_pdf.write_bytes(b"%PDF-1.4 broken")
    doc_id = db.insert_document(
        filename=broken_pdf.name,
        filepath=str(broken_pdf),
        file_hash="broken-pdf-hash",
        file_type="pdf",
        file_size=broken_pdf.stat().st_size,
        ingested_at="2026-01-01T00:00:00Z",
        status="pending_ocr",
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["process", "--workers", "2"])
    assert result.exit_code == 0
    assert "retries" in result.output and "exceeded" in result.output

    db = Database(db_path)
    db.initialize()
    doc = db.get_document(doc_id)
    db.close()
    assert doc["status"] == "error"
    assert doc["processing_attempts"] == 1
    assert "max_retries_exceeded" in doc["error_message"]
    assert temp_images and all(not p.exists() for p in temp_images)


def test_search_fuzzy_finds_ocr_typos(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-fuzzy-search")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.search.enable_fuzzy = True
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.insert_document(
        filename="receipt.pdf",
        filepath=str(tmp_path / "receipt.pdf"),
        file_hash="fuzzy-1",
        file_type="pdf",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="reciept from clinic with total $21.00",
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["search", "receipt clinic", "--fuzzy"])
    assert result.exit_code == 0
    assert "receipt.pdf" in result.output
    assert "Fuzzy search enabled" in result.output


def test_classify_tags_processed_documents(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-classify")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.autopilot.confidence_threshold = 0.6
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    doc_id = db.insert_document(
        filename="invoice-2026.pdf",
        filepath=str(tmp_path / "invoice-2026.pdf"),
        file_hash="classify-1",
        file_type="pdf",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="Invoice due amount total balance",
    )
    db.insert_fields(
        doc_id, [{"field_type": "amount", "value": "$42.00", "start": 1, "raw_match": "$42.00"}]
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["classify", "--limit", "10"])
    assert result.exit_code == 0
    assert "Classification Results" in result.output

    db = Database(config.db_path)
    db.initialize()
    tags = db.get_tags(doc_id)
    db.close()
    assert "invoice" in tags


def test_classify_train_evaluate_and_ml_mode(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-classify-ml")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.autopilot.classification_model = "ml"
    config.autopilot.min_training_samples = 2
    config.autopilot.model_path = tmp_path / "classifier.json"
    config.autopilot.confidence_threshold = 0.2
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    dataset = tmp_path / "train.csv"
    dataset.write_text(
        "text,label\n"
        "invoice balance amount due,invoice\n"
        "patient clinic diagnosis,medical\n",
        encoding="utf-8",
    )

    db = Database(config.db_path)
    db.initialize()
    doc_id = db.insert_document(
        filename="ml-invoice.pdf",
        filepath=str(tmp_path / "ml-invoice.pdf"),
        file_hash="classify-ml-1",
        file_type="pdf",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="invoice amount due balance",
    )
    db.close()

    runner = CliRunner()
    train = runner.invoke(
        main,
        ["classify-train", "--dataset", str(dataset), "--format", "csv", "--json"],
    )
    assert train.exit_code == 0
    assert '"trained": true' in train.output

    evaluate = runner.invoke(
        main,
        ["classify-evaluate", "--dataset", str(dataset), "--format", "csv", "--json"],
    )
    assert evaluate.exit_code == 0
    assert '"accuracy"' in evaluate.output

    run = runner.invoke(main, ["classify", "--limit", "10"])
    assert run.exit_code == 0
    assert "Model:" in run.output
    assert "ml" in run.output

    db = Database(config.db_path)
    db.initialize()
    tags = db.get_tags(doc_id)
    db.close()
    assert "invoice" in tags


def test_process_dry_run_and_max_errors(monkeypatch):
    class _FailingOCR:
        def extract_text(self, image_path: Path) -> list[dict]:
            raise RuntimeError("always fails")

    tmp_path = _workspace_tmp_dir("localarchive-process-controls")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.processing.max_errors_per_run = 1
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _FailingOCR(),
        pdf_to_images=lambda _path, tmp_dir=None: [],
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    db = Database(config.db_path)
    db.initialize()
    for i in range(2):
        source = tmp_path / f"bad-{i}.png"
        source.write_bytes(b"bad")
        db.insert_document(
            filename=source.name,
            filepath=str(source),
            file_hash=f"bad-{i}",
            file_type="png",
            file_size=source.stat().st_size,
            ingested_at="2026-01-01T00:00:00Z",
            status="pending_ocr",
        )
    db.close()

    runner = CliRunner()
    dry = runner.invoke(main, ["process", "--dry-run"])
    assert dry.exit_code == 0
    assert "would process 2 document" in dry.output

    live = runner.invoke(main, ["process", "--workers", "1", "--max-errors", "1"])
    assert live.exit_code == 0
    assert "Processing aborted" in live.output


def test_process_checkpoint_uses_max_completed_doc_id(monkeypatch):
    class _MixedOCR:
        def extract_text(self, image_path: Path) -> list[dict]:
            if image_path.name == "fail-third.png":
                raise RuntimeError("intentional failure")
            time.sleep(0.2)
            return [{"text": "ok text", "confidence": 0.9, "bbox": []}]

    tmp_path = _workspace_tmp_dir("localarchive-checkpoint-max")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.processing.max_errors_per_run = 1
    config.processing.resume_checkpoint_interval = 1
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _MixedOCR(),
        pdf_to_images=lambda _path, tmp_dir=None: [],
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    db = Database(config.db_path)
    db.initialize()
    doc_ids = []
    for name in ["slow-first.png", "slow-second.png", "fail-third.png"]:
        source = tmp_path / name
        source.write_bytes(b"x")
        doc_ids.append(
            db.insert_document(
                filename=name,
                filepath=str(source),
                file_hash=f"hash-{name}",
                file_type="png",
                file_size=source.stat().st_size,
                ingested_at="2026-01-01T00:00:00Z",
                status="pending_ocr",
            )
        )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["process", "--workers", "3", "--max-errors", "1"])
    assert result.exit_code == 0
    assert "Processing aborted" in result.output

    db = Database(config.db_path)
    db.initialize()
    run = db.latest_processing_run()
    db.close()
    assert run is not None
    assert int(run["checkpoint_doc_id"]) == max(doc_ids)


def test_process_resume_messages(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-resume-msg")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _FakeOCR(),
        pdf_to_images=lambda _path, tmp_dir=None: [],
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    db = Database(config.db_path)
    db.initialize()
    src = tmp_path / "doc.png"
    src.write_bytes(b"x")
    db.insert_document(
        filename=src.name,
        filepath=str(src),
        file_hash="resume-msg-1",
        file_type="png",
        file_size=src.stat().st_size,
        ingested_at="2026-01-01T00:00:00Z",
        status="pending_ocr",
    )
    db.close()

    runner = CliRunner()
    first = runner.invoke(main, ["process", "--dry-run", "--resume"])
    assert first.exit_code == 0
    assert "No checkpointed run found" in first.output or "would process" in first.output

    run_live = runner.invoke(main, ["process", "--workers", "1"])
    assert run_live.exit_code == 0

    second = runner.invoke(main, ["process", "--dry-run", "--resume"])
    assert second.exit_code == 0
    assert "Resuming from run" in second.output


def test_doctor_json_output(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-doctor-json")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    config.ensure_dirs()
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    monkeypatch.setattr("importlib.util.find_spec", lambda _name: object())

    runner = CliRunner()
    result = runner.invoke(main, ["doctor", "--json"])
    assert result.exit_code == 0
    assert '"checks"' in result.output


def test_search_json_and_explain_ranking(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-search-json")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.insert_document(
        filename="rank.pdf",
        filepath=str(tmp_path / "rank.pdf"),
        file_hash="rank-1",
        file_type="pdf",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="receipt clinic amount",
    )
    db.close()

    runner = CliRunner()
    as_json = runner.invoke(main, ["search", "receipt", "--json"])
    assert as_json.exit_code == 0
    assert '"results"' in as_json.output

    explained = runner.invoke(main, ["search", "receipt", "--explain-ranking"])
    assert explained.exit_code == 0
    assert "Ranking Explanation" in explained.output


def test_search_handles_invalid_fts_query(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-search-invalid")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.close()

    runner = CliRunner()
    text = runner.invoke(main, ["search", '"'])
    assert text.exit_code == 0
    assert "Invalid search query syntax." in text.output

    as_json = runner.invoke(main, ["search", '"', "--json"])
    assert as_json.exit_code == 0
    payload = json.loads(as_json.output)
    assert payload["invalid_query"] is True
    assert payload["count"] == 0


def test_verify_json_reports_issues(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-verify-json")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.insert_document(
        filename="missing.pdf",
        filepath=str(tmp_path / "missing.pdf"),
        file_hash="missing",
        file_type="pdf",
        file_size=1,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["verify", "--json"])
    assert result.exit_code == 4
    assert '"issues"' in result.output
    assert '"issue_breakdown"' in result.output
    assert '"recommendations"' in result.output


def test_verify_quick_vs_full_hash_mismatch(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-verify-modes")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    existing = tmp_path / "existing.pdf"
    existing.write_bytes(b"%PDF-1.4 content")
    db.insert_document(
        filename=existing.name,
        filepath=str(existing),
        file_hash="incorrect-hash",
        file_type="pdf",
        file_size=existing.stat().st_size,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
    )
    db.close()

    runner = CliRunner()
    quick = runner.invoke(main, ["verify", "--json"])
    assert quick.exit_code == 0
    assert '"full_check": false' in quick.output
    full = runner.invoke(main, ["verify", "--full", "--json"])
    assert full.exit_code == 4
    assert '"full_check": true' in full.output

    full_text = runner.invoke(main, ["verify", "--full"])
    assert full_text.exit_code == 4
    assert "Issue breakdown:" in full_text.output


def test_process_json_summary(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-process-json")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _FakeOCR(),
        pdf_to_images=lambda _path, tmp_dir=None: [],
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    db = Database(config.db_path)
    db.initialize()
    source = tmp_path / "doc.png"
    source.write_bytes(b"x")
    db.insert_document(
        filename=source.name,
        filepath=str(source),
        file_hash="process-json-1",
        file_type="png",
        file_size=source.stat().st_size,
        ingested_at="2026-01-01T00:00:00Z",
        status="pending_ocr",
    )
    db.close()

    runner = CliRunner()
    dry = runner.invoke(main, ["process", "--dry-run", "--json"])
    assert dry.exit_code == 0
    assert '"dry_run": true' in dry.output
    assert '"resumed_from_run": null' in dry.output
    assert '"start_after_doc_id": 0' in dry.output

    run = runner.invoke(main, ["process", "--json", "--workers", "1"])
    assert run.exit_code == 0
    assert '"run_id"' in run.output
    assert '"status": "completed"' in run.output
    assert '"resumed_from_run": null' in run.output
    assert '"ocr_languages": [' in run.output


def test_process_ocr_languages_override(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-process-ocr-lang")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _FakeOCR(),
        pdf_to_images=lambda _path, tmp_dir=None: [],
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    db = Database(config.db_path)
    db.initialize()
    source = tmp_path / "doc.png"
    source.write_bytes(b"x")
    db.insert_document(
        filename=source.name,
        filepath=str(source),
        file_hash="process-ocr-lang-1",
        file_type="png",
        file_size=source.stat().st_size,
        ingested_at="2026-01-01T00:00:00Z",
        status="pending_ocr",
    )
    db.close()

    runner = CliRunner()
    run = runner.invoke(main, ["process", "--json", "--workers", "1", "--ocr-languages", "en,es"])
    assert run.exit_code == 0
    assert '"ocr_languages": [' in run.output
    assert '"en"' in run.output
    assert '"es"' in run.output


def test_process_ocr_languages_validation(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-process-ocr-invalid")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    runner = CliRunner()
    bad = runner.invoke(main, ["process", "--ocr-languages", "en,*", "--dry-run"])
    assert bad.exit_code == 2
    assert "Invalid OCR language code" in bad.output


def test_process_extract_tables_persists_results(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-process-tables")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    fake_ocr_module = types.SimpleNamespace(
        get_ocr_engine=lambda _cfg: _FakeOCR(),
        pdf_to_images=lambda _path, tmp_dir=None: [],
        extract_text_from_pdf_native=lambda _path: "",
    )
    monkeypatch.setitem(sys.modules, "localarchive.core.ocr_engine", fake_ocr_module)

    db = Database(config.db_path)
    db.initialize()
    source = tmp_path / "table.png"
    source.write_bytes(b"x")
    db.insert_document(
        filename=source.name,
        filepath=str(source),
        file_hash="process-tables-1",
        file_type="png",
        file_size=source.stat().st_size,
        ingested_at="2026-01-01T00:00:00Z",
        status="pending_ocr",
    )
    db.close()

    runner = CliRunner()
    run = runner.invoke(main, ["process", "--json", "--workers", "1", "--extract-tables"])
    assert run.exit_code == 0
    assert '"extract_tables": true' in run.output

    db = Database(config.db_path)
    db.initialize()
    tables = db.get_tables(1)
    db.close()
    assert isinstance(tables, list)


def test_export_include_tables_json(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-export-tables")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    db = Database(config.db_path)
    db.initialize()
    source = tmp_path / "export.png"
    source.write_bytes(b"x")
    doc_id = db.insert_document(
        filename=source.name,
        filepath=str(source),
        file_hash="export-tables-1",
        file_type="png",
        file_size=source.stat().st_size,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="Header1 | Header2\nA | B",
    )
    db.set_tables(doc_id, [{"headers": ["Header1", "Header2"], "rows": [["A", "B"]]}])
    db.close()

    runner = CliRunner()
    out_path = tmp_path / "export.json"
    result = runner.invoke(
        main,
        ["export", "--format", "json", "--output", str(out_path), "--include-tables"],
    )
    assert result.exit_code == 0
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload
    assert "tables" in payload[0]


def test_similarity_build_and_query(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-similarity")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)
    db = Database(config.db_path)
    db.initialize()
    p1 = tmp_path / "a.txt"
    p2 = tmp_path / "b.txt"
    p3 = tmp_path / "c.txt"
    p1.write_bytes(b"a")
    p2.write_bytes(b"b")
    p3.write_bytes(b"c")
    db.insert_document(
        filename="a.txt",
        filepath=str(p1),
        file_hash="sim-a",
        file_type="txt",
        file_size=1,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="invoice payment receipt amount total",
    )
    db.insert_document(
        filename="b.txt",
        filepath=str(p2),
        file_hash="sim-b",
        file_type="txt",
        file_size=1,
        ingested_at="2026-01-01T00:00:01Z",
        status="processed",
        ocr_text="invoice amount balance payment",
    )
    db.insert_document(
        filename="c.txt",
        filepath=str(p3),
        file_hash="sim-c",
        file_type="txt",
        file_size=1,
        ingested_at="2026-01-01T00:00:02Z",
        status="processed",
        ocr_text="biology chemistry microscopy experiment",
    )
    db.close()

    runner = CliRunner()
    build = runner.invoke(main, ["similarity", "build", "--json", "--limit", "10"])
    assert build.exit_code == 0
    assert '"built": true' in build.output
    assert '"edges":' in build.output

    related = runner.invoke(main, ["similarity", "for", "1", "--json", "--top-k", "5"])
    assert related.exit_code == 0
    assert '"doc_id": 1' in related.output
    assert '"related_id": 2' in related.output


def test_graph_entities_json(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-entity-graph")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    doc_id = db.insert_document(
        filename="graph.pdf",
        filepath=str(tmp_path / "graph.pdf"),
        file_hash="graph-1",
        file_type="pdf",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="ACME by Alice",
    )
    db.insert_fields(
        doc_id,
        [
            {"field_type": "entity_org", "value": "ACME", "raw_match": "ACME", "start": 0},
            {"field_type": "entity_person", "value": "Alice", "raw_match": "Alice", "start": 10},
        ],
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["graph", "entities", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert int(payload["documents"]) == 1
    node_ids = {str(n.get("id", "")) for n in payload["nodes"]}
    assert "doc:1" in node_ids
    assert "entity:entity_org:acme" in node_ids
    assert "entity:entity_person:alice" in node_ids
    assert payload["edges"]


def test_review_queue_build_list_and_resolve(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-review-queue")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.insert_document(
        filename="low-confidence.pdf",
        filepath=str(tmp_path / "low-confidence.pdf"),
        file_hash="review-low-1",
        file_type="pdf",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="tiny",
    )
    db.close()

    runner = CliRunner()
    built = runner.invoke(main, ["review", "build", "--json", "--threshold", "0.9"])
    assert built.exit_code == 0
    built_payload = json.loads(built.output)
    assert int(built_payload["queued"]) == 1

    listed = runner.invoke(main, ["review", "list", "--json"])
    assert listed.exit_code == 0
    listed_payload = json.loads(listed.output)
    assert int(listed_payload["count"]) == 1
    assert int(listed_payload["items"][0]["document_id"]) == 1
    assert listed_payload["items"][0]["status"] == "pending"

    resolved = runner.invoke(main, ["review", "resolve", "1", "--note", "manually validated"])
    assert resolved.exit_code == 0

    listed_after = runner.invoke(main, ["review", "list", "--status", "resolved", "--json"])
    assert listed_after.exit_code == 0
    listed_after_payload = json.loads(listed_after.output)
    assert int(listed_after_payload["count"]) == 1
    assert listed_after_payload["items"][0]["status"] == "resolved"


def test_citations_extract_json(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-citations")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    doc_id = db.insert_document(
        filename="paper.pdf",
        filepath=str(tmp_path / "paper.pdf"),
        file_hash="cite-1",
        file_type="pdf",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="See DOI 10.1145/1234567.8901234 and arXiv: 2401.01234v2",
    )
    db.insert_fields(
        doc_id,
        [
            {"field_type": "doi", "value": "10.1145/1234567.8901234", "raw_match": "", "start": 0},
        ],
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["citations", "extract", "--format", "json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert int(payload["count"]) >= 2
    types = {str(item.get("type")) for item in payload["citations"]}
    assert "doi" in types
    assert "arxiv" in types


def test_redaction_document_export(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-redaction")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    db = Database(config.db_path)
    db.initialize()
    db.insert_document(
        filename="pii.txt",
        filepath=str(tmp_path / "pii.txt"),
        file_hash="redact-1",
        file_type="txt",
        file_size=10,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="Email me at jane@example.com or call 555-123-4567",
    )
    db.close()

    out = tmp_path / "redacted.txt"
    runner = CliRunner()
    result = runner.invoke(main, ["redaction", "document", "1", "--output", str(out), "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["doc_id"] == 1
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "jane@example.com" not in text
    assert "555-123-4567" not in text
    assert "[REDACTED_EMAIL]" in text
    assert "[REDACTED_PHONE]" in text


def test_duplicates_scan_detects_near_duplicate_images(monkeypatch):
    pytest.importorskip("PIL")
    from PIL import Image

    tmp_path = _workspace_tmp_dir("localarchive-duplicates")
    config = Config(archive_dir=tmp_path / "archive", db_path=tmp_path / "archive.db")
    monkeypatch.setattr("localarchive.cli.get_config", lambda: config)

    img1 = tmp_path / "dup1.png"
    img2 = tmp_path / "dup2.png"
    img3 = tmp_path / "unique.png"
    Image.new("RGB", (48, 48), color=(20, 40, 220)).save(img1)
    Image.new("RGB", (48, 48), color=(20, 40, 220)).save(img2)
    Image.new("RGB", (48, 48), color=(220, 40, 20)).save(img3)

    db = Database(config.db_path)
    db.initialize()
    db.insert_document(
        filename=img1.name,
        filepath=str(img1),
        file_hash="dup-a",
        file_type="png",
        file_size=img1.stat().st_size,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
    )
    db.insert_document(
        filename=img2.name,
        filepath=str(img2),
        file_hash="dup-b",
        file_type="png",
        file_size=img2.stat().st_size,
        ingested_at="2026-01-01T00:00:01Z",
        status="processed",
    )
    db.insert_document(
        filename=img3.name,
        filepath=str(img3),
        file_hash="uniq-c",
        file_type="png",
        file_size=img3.stat().st_size,
        ingested_at="2026-01-01T00:00:02Z",
        status="processed",
    )
    db.close()

    runner = CliRunner()
    result = runner.invoke(main, ["duplicates", "scan", "--json", "--max-distance", "1"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert int(payload["hashed"]) == 3
    assert payload["duplicates"]
    pair_ids = {(int(p["doc_id_a"]), int(p["doc_id_b"])) for p in payload["duplicates"]}
    assert (1, 2) in pair_ids
