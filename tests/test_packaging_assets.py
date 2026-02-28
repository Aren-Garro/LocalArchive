"""Smoke checks for packaging/deployment assets."""

from pathlib import Path


def test_docker_assets_present():
    root = Path.cwd()
    dockerfile = root / "Dockerfile"
    compose = root / "docker-compose.yml"
    ignore = root / ".dockerignore"
    assert dockerfile.exists()
    assert compose.exists()
    assert ignore.exists()

    d = dockerfile.read_text(encoding="utf-8")
    c = compose.read_text(encoding="utf-8")
    assert '"localarchive.cli"' in d
    assert '"serve"' in d
    assert "8877:8877" in c
