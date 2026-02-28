"""Tests for localarchive.core.ingester."""

import uuid
from pathlib import Path

from localarchive.config import Config
from localarchive.core.ingester import Ingester, watch_directory
from localarchive.db.database import Database


def _workspace_tmp_dir(prefix: str) -> Path:
    root = Path.cwd() / ".test_tmp"
    root.mkdir(exist_ok=True)
    path = root / f"{prefix}-{uuid.uuid4().hex[:8]}"
    path.mkdir(exist_ok=True)
    return path


def test_ingest_files_fast_scan_cache(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-ingester")
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=tmp_path / "archive", db_path=db_path)
    config.ensure_dirs()
    db = Database(db_path)
    db.initialize()
    ingester = Ingester(config, db)

    incoming = tmp_path / "incoming"
    incoming.mkdir()
    source = incoming / "scan.pdf"
    source.write_bytes(b"%PDF-1.4 first")
    calls = {"count": 0}

    def _counting_hash(path: Path) -> str:
        calls["count"] += 1
        return f"hash-{path.stat().st_size}"

    monkeypatch.setattr("localarchive.core.ingester.file_hash", _counting_hash)
    cache: dict[str, dict] = {}

    first = ingester.ingest_files([source], scan_cache=cache)
    second = ingester.ingest_files([source], scan_cache=cache)
    assert len(first) == 1
    assert len(second) == 0
    assert calls["count"] == 1

    source.write_bytes(b"%PDF-1.4 second change")
    third = ingester.ingest_files([source], scan_cache=cache)
    assert len(third) == 1
    assert calls["count"] == 2
    db.close()


def test_watch_manifest_persists_between_runs(monkeypatch):
    tmp_path = _workspace_tmp_dir("localarchive-watch-manifest")
    db_path = tmp_path / "archive.db"
    config = Config(archive_dir=tmp_path / "archive", db_path=db_path)
    config.watch.manifest_path = tmp_path / "watch_manifest.json"
    config.ensure_dirs()
    db = Database(db_path)
    db.initialize()
    ingester = Ingester(config, db)

    incoming = tmp_path / "incoming"
    incoming.mkdir()
    source = incoming / "scan.pdf"
    source.write_bytes(b"%PDF-1.4 first")
    calls = {"count": 0}

    def _counting_hash(path: Path) -> str:
        calls["count"] += 1
        return f"hash-{path.stat().st_size}"

    monkeypatch.setattr("localarchive.core.ingester.file_hash", _counting_hash)

    first = watch_directory(
        ingester,
        path=incoming,
        run_once=True,
        fast_scan=True,
        manifest_path=config.watch.manifest_path,
        manifest_gc_days=30,
    )
    second = watch_directory(
        ingester,
        path=incoming,
        run_once=True,
        fast_scan=True,
        manifest_path=config.watch.manifest_path,
        manifest_gc_days=30,
    )

    assert first == 1
    assert second == 0
    assert calls["count"] == 1
    assert config.watch.manifest_path.exists()
    db.close()
