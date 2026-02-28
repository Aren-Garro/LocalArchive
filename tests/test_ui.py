"""Tests for LocalArchive FastAPI UI routes."""

import re
import uuid
from pathlib import Path

import pytest

from localarchive.config import Config
from localarchive.db.database import Database


def _seed_db(db: Database):
    doc_id = db.insert_document(
        filename="invoice.pdf",
        filepath="/tmp/invoice.pdf",
        file_hash="ui-hash-1",
        file_type="pdf",
        file_size=123,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="Acme invoice 2026 amount $42.00",
    )
    db.add_tag(doc_id, "finance")
    db.insert_fields(
        doc_id,
        [{"field_type": "amount", "value": "$42.00", "raw_match": "$42.00", "start": 20}],
    )
    return doc_id


def _workspace_tmp_dir(prefix: str) -> Path:
    root = Path.cwd() / ".test_tmp"
    root.mkdir(exist_ok=True)
    path = root / f"{prefix}-{uuid.uuid4().hex[:8]}"
    path.mkdir(exist_ok=True)
    return path


def _csrf_token_from_html(html: str) -> str:
    match = re.search(r'name="csrf_token" value="([^"]+)"', html)
    assert match
    return str(match.group(1))


def test_ui_index_and_search():
    pytest.importorskip("fastapi")
    pytest.importorskip("fastapi.testclient")
    from fastapi.testclient import TestClient

    from localarchive.ui.app import create_app

    tmp_path = _workspace_tmp_dir("localarchive-ui")
    db_path = tmp_path / "ui.db"
    config = Config(archive_dir=tmp_path / "archive", db_path=db_path)
    db = Database(db_path)
    db.initialize()
    _seed_db(db)
    db.close()

    app = create_app(config)
    client = TestClient(app)

    res = client.get("/")
    assert res.status_code == 200
    assert "LocalArchive" in res.text
    assert "invoice.pdf" in res.text
    assert 'aria-label="Pagination"' in res.text
    assert "chip processed" in res.text

    res = client.get("/", params={"q": "Acme", "tag": "finance", "file_type": "pdf"})
    assert res.status_code == 200
    assert "invoice.pdf" in res.text

    res = client.get("/", params={"q": "Acme", "tag": "missing-tag", "file_type": "pdf"})
    assert res.status_code == 200
    assert "invoice.pdf" not in res.text
    assert "0 documents found" in res.text


def test_ui_document_detail():
    pytest.importorskip("fastapi")
    pytest.importorskip("fastapi.testclient")
    from fastapi.testclient import TestClient

    from localarchive.ui.app import create_app

    tmp_path = _workspace_tmp_dir("localarchive-ui-detail")
    db_path = tmp_path / "ui-detail.db"
    config = Config(archive_dir=tmp_path / "archive", db_path=db_path)
    db = Database(db_path)
    db.initialize()
    doc_id = _seed_db(db)
    db.close()

    app = create_app(config)
    client = TestClient(app)

    res = client.get(f"/documents/{doc_id}")
    assert res.status_code == 200
    assert "Extracted Fields" in res.text
    assert "$42.00" in res.text
    assert "chip processed" in res.text

    res = client.get("/documents/99999")
    assert res.status_code == 404


def test_ui_document_actions():
    pytest.importorskip("fastapi")
    pytest.importorskip("fastapi.testclient")
    from fastapi.testclient import TestClient

    from localarchive.ui.app import create_app

    tmp_path = _workspace_tmp_dir("localarchive-ui-actions")
    db_path = tmp_path / "ui-actions.db"
    config = Config(archive_dir=tmp_path / "archive", db_path=db_path)
    db = Database(db_path)
    db.initialize()
    doc_id = _seed_db(db)
    db.update_document(doc_id, status="error", error_message="failed")
    db.close()

    app = create_app(config)
    client = TestClient(app)
    detail = client.get(f"/documents/{doc_id}")
    assert detail.status_code == 200
    csrf_token = _csrf_token_from_html(detail.text)

    res = client.post(
        f"/documents/{doc_id}/retry",
        data={"csrf_token": csrf_token},
        follow_redirects=False,
    )
    assert res.status_code == 303

    db = Database(db_path)
    db.initialize()
    doc = db.get_document(doc_id)
    assert doc["status"] == "pending_ocr"
    assert doc["error_message"] == ""
    db.close()

    detail = client.get(f"/documents/{doc_id}")
    assert detail.status_code == 200
    csrf_token = _csrf_token_from_html(detail.text)
    res = client.post(
        f"/documents/{doc_id}/tags",
        data={"tags": "health, urgent", "csrf_token": csrf_token},
        follow_redirects=False,
    )
    assert res.status_code == 303

    db = Database(db_path)
    db.initialize()
    tags = db.get_tags(doc_id)
    db.close()
    assert "health" in tags
    assert "urgent" in tags


def test_ui_actions_reject_cross_origin():
    pytest.importorskip("fastapi")
    pytest.importorskip("fastapi.testclient")
    from fastapi.testclient import TestClient

    from localarchive.ui.app import create_app

    tmp_path = _workspace_tmp_dir("localarchive-ui-csrf")
    db_path = tmp_path / "ui-csrf.db"
    config = Config(archive_dir=tmp_path / "archive", db_path=db_path)
    db = Database(db_path)
    db.initialize()
    doc_id = _seed_db(db)
    db.close()

    app = create_app(config)
    client = TestClient(app)
    detail = client.get(f"/documents/{doc_id}")
    csrf_token = _csrf_token_from_html(detail.text)
    res = client.post(
        f"/documents/{doc_id}/retry",
        data={"csrf_token": csrf_token},
        headers={"origin": "https://evil.example"},
        follow_redirects=False,
    )
    assert res.status_code == 403


def test_ui_status_filter_dropdown():
    pytest.importorskip("fastapi")
    pytest.importorskip("fastapi.testclient")
    from fastapi.testclient import TestClient

    from localarchive.ui.app import create_app

    tmp_path = _workspace_tmp_dir("localarchive-ui-filter")
    db_path = tmp_path / "ui-filter.db"
    config = Config(archive_dir=tmp_path / "archive", db_path=db_path)
    db = Database(db_path)
    db.initialize()
    doc_id = _seed_db(db)
    db.update_document(doc_id, status="error", error_message="oops")
    db.close()

    app = create_app(config)
    client = TestClient(app)
    res = client.get("/", params={"status": "error"})
    assert res.status_code == 200
    assert 'selected">error</option>' in res.text
