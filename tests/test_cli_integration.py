"""Integration tests for CLI wiring."""

import uuid
import types
import sys
from pathlib import Path
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

    result = runner.invoke(main, ["audit"])
    assert result.exit_code in (0, 4)
