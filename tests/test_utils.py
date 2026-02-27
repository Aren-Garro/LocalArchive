"""Tests for localarchive.utils"""
from pathlib import Path
from localarchive.utils import is_supported, truncate, safe_filename


def test_is_supported():
    assert is_supported(Path("doc.pdf")) is True
    assert is_supported(Path("photo.jpg")) is True
    assert is_supported(Path("photo.JPEG")) is True
    assert is_supported(Path("scan.tiff")) is True
    assert is_supported(Path("notes.txt")) is False
    assert is_supported(Path("data.csv")) is False


def test_truncate():
    assert truncate("short", 200) == "short"
    assert len(truncate("a" * 300, 200)) == 200
    assert truncate("a" * 300, 200).endswith("...")


def test_safe_filename():
    assert safe_filename("hello world.pdf") == "hello world.pdf"
    assert safe_filename("bad/file:name") == "badfilename"
