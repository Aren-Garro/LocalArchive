"""Perceptual duplicate detection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PIL import Image


@dataclass
class DuplicateCandidate:
    doc_id: int
    filename: str
    filepath: str
    file_type: str
    phash: int


def _dhash(img: Image.Image) -> int:
    gray = img.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
    pixels = list(gray.tobytes())
    bits = 0
    for row in range(8):
        row_start = row * 9
        for col in range(8):
            left = pixels[row_start + col]
            right = pixels[row_start + col + 1]
            bits <<= 1
            bits |= 1 if left > right else 0
    return bits


def _pdf_first_page_image(pdf_path: Path) -> Image.Image:
    import fitz

    with fitz.open(str(pdf_path)) as doc:
        if len(doc) == 0:
            raise ValueError("empty_pdf")
        page = doc.load_page(0)
        pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
        mode = "RGB" if pix.n >= 3 else "L"
        return Image.frombytes(mode, [pix.width, pix.height], pix.samples)


def perceptual_hash_for_file(path: Path, file_type: str) -> int:
    p = Path(path)
    if file_type.lower() == "pdf":
        img = _pdf_first_page_image(p)
        return _dhash(img)
    with Image.open(p) as img:
        return _dhash(img)


def hamming_distance(left: int, right: int) -> int:
    return int((left ^ right).bit_count())


def find_duplicate_pairs(
    candidates: list[DuplicateCandidate],
    max_distance: int = 6,
) -> list[dict]:
    out: list[dict] = []
    sorted_candidates = sorted(candidates, key=lambda c: c.doc_id)
    for i in range(len(sorted_candidates)):
        a = sorted_candidates[i]
        for j in range(i + 1, len(sorted_candidates)):
            b = sorted_candidates[j]
            dist = hamming_distance(a.phash, b.phash)
            if dist > max_distance:
                continue
            out.append(
                {
                    "doc_id_a": a.doc_id,
                    "doc_id_b": b.doc_id,
                    "filename_a": a.filename,
                    "filename_b": b.filename,
                    "distance": dist,
                }
            )
    out.sort(key=lambda row: (int(row["distance"]), int(row["doc_id_a"]), int(row["doc_id_b"])))
    return out
