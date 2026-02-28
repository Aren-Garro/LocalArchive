"""Integration tests for CLI wiring."""

import json
import sys
import tempfile
import time
import types
import uuid
import zipfile
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
    db.record_backup(path=str(tmp_path / "ghost.zip"), db_hash="", archive_file_count=0, verified=False)
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
    db.record_backup(path=str(tmp_path / "ghost.zip"), db_hash="", archive_file_count=0, verified=False)
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
    result = runner.invoke(main, ["backup", "restore", "--path", str(backup_path), "--dry-run", "--json"])
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
    monkeypatch.setattr("localarchive.cli.SearchEngine.search_hybrid", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("should not run hybrid")))

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
    result = runner.invoke(main, ["process", "--workers", "3", "--checkpoint-every", "1", "--extractor", "regex"])
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
    db.insert_fields(doc_id, [{"field_type": "amount", "value": "$42.00", "start": 1, "raw_match": "$42.00"}])
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
