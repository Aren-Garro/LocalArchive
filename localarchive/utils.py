"""Shared utilities for LocalArchive."""

import hashlib
from pathlib import Path
from datetime import datetime, timezone

SUPPORTED_EXTENSIONS = {
    ".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".webp", ".gif",
}


def file_hash(filepath: Path) -> str:
    """Compute SHA-256 hash of a file for deduplication."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def is_supported(filepath: Path) -> bool:
    return filepath.suffix.lower() in SUPPORTED_EXTENSIONS


def timestamp_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def truncate(text: str, max_length: int = 200) -> str:
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def safe_filename(name: str) -> str:
    keepchars = (" ", ".", "_", "-")
    return "".join(c for c in name if c.isalnum() or c in keepchars).rstrip()
