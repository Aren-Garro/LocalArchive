"""Integration tests for CLI wiring."""

import uuid
import types
import sys
import zipfile
import tempfile
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
