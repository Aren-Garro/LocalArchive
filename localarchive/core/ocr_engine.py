"""
OCR engine abstraction.
Supports PaddleOCR (default) and EasyOCR (optional).
"""

from pathlib import Path
from abc import ABC, abstractmethod
import fitz  # PyMuPDF
from rich.console import Console
from localarchive.config import OCRConfig

console = Console()


class BaseOCR(ABC):
    @abstractmethod
    def extract_text(self, image_path: Path) -> list[dict]:
        """Extract text from an image. Returns [{\"text\": str, \"confidence\": float, \"bbox\": list}]"""
        ...


class PaddleOCREngine(BaseOCR):
    def __init__(self, config: OCRConfig):
        self.config = config
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            from paddleocr import PaddleOCR
            self._engine = PaddleOCR(
                use_angle_cls=True,
                lang=self.config.languages[0] if self.config.languages else "en",
                show_log=False,
            )
        return self._engine

    def extract_text(self, image_path: Path) -> list[dict]:
        result = self.engine.ocr(str(image_path), cls=True)
        entries = []
        if result and result[0]:
            for line in result[0]:
                bbox, (text, confidence) = line
                if confidence >= self.config.confidence_threshold:
                    entries.append({"text": text, "confidence": round(confidence, 4), "bbox": bbox})
        return entries


class EasyOCREngine(BaseOCR):
    def __init__(self, config: OCRConfig):
        self.config = config
        self._reader = None

    @property
    def reader(self):
        if self._reader is None:
            import easyocr
            self._reader = easyocr.Reader(self.config.languages or ["en"], gpu=False)
        return self._reader

    def extract_text(self, image_path: Path) -> list[dict]:
        results = self.reader.readtext(str(image_path))
        entries = []
        for bbox, text, confidence in results:
            if confidence >= self.config.confidence_threshold:
                entries.append({"text": text, "confidence": round(confidence, 4), "bbox": bbox})
        return entries


def get_ocr_engine(config: OCRConfig) -> BaseOCR:
    if config.engine == "easyocr":
        return EasyOCREngine(config)
    return PaddleOCREngine(config)


def pdf_to_images(pdf_path: Path, dpi: int = 200) -> list[Path]:
    """Convert each page of a PDF to a temporary PNG image."""
    import tempfile
    doc = fitz.open(str(pdf_path))
    image_paths = []
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        tmp = Path(tempfile.mktemp(suffix=f"_p{page_num}.png"))
        pix.save(str(tmp))
        image_paths.append(tmp)
    doc.close()
    return image_paths


def extract_text_from_pdf_native(pdf_path: Path) -> str:
    """Try to extract embedded text from PDF (no OCR needed)."""
    doc = fitz.open(str(pdf_path))
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    doc.close()
    return text.strip()
