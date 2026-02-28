"""Browser smoke test for LocalArchive UI.

Requires Playwright + browser binaries. Skips automatically when unavailable.
"""

import socket
import threading
import time
import uuid
from pathlib import Path

import pytest

from localarchive.config import Config
from localarchive.db.database import Database


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _workspace_tmp_dir(prefix: str) -> Path:
    root = Path.cwd() / ".test_tmp"
    root.mkdir(exist_ok=True)
    path = root / f"{prefix}-{uuid.uuid4().hex[:8]}"
    path.mkdir(exist_ok=True)
    return path


def test_ui_browser_smoke():
    pytest.importorskip("fastapi")
    uvicorn = pytest.importorskip("uvicorn")
    from localarchive.ui.app import create_app

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        pytest.skip("playwright is not installed")

    tmp_path = _workspace_tmp_dir("localarchive-ui-browser")
    db_path = tmp_path / "browser.db"
    config = Config(archive_dir=tmp_path / "archive", db_path=db_path)
    db = Database(db_path)
    db.initialize()
    db.insert_document(
        filename="smoke.pdf",
        filepath="/tmp/smoke.pdf",
        file_hash="smoke-hash-1",
        file_type="pdf",
        file_size=111,
        ingested_at="2026-01-01T00:00:00Z",
        status="processed",
        ocr_text="Acme smoke search text",
    )
    db.close()

    app = create_app(config)
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", lifespan="off")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    time.sleep(1)

    try:
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)
            except Exception as exc:
                pytest.skip(f"playwright browser launch unavailable: {exc}")
            page = browser.new_page()
            page.goto(f"http://127.0.0.1:{port}", wait_until="domcontentloaded")
            assert "LocalArchive" in page.content()
            page.fill("input[name='q']", "Acme")
            page.click("button[type='submit']")
            page.wait_for_timeout(200)
            assert "smoke.pdf" in page.content()
            page.click("a[href^='/documents/']")
            page.wait_for_timeout(200)
            assert "Extracted Fields" in page.content()
            browser.close()
    finally:
        server.should_exit = True
        thread.join(timeout=5)
