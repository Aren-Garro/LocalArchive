"""
Configuration management for LocalArchive.
Reads from ~/.localarchive/config.toml, with sensible defaults.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field

try:
    import toml
except ImportError:
    toml = None

DEFAULT_ARCHIVE_DIR = Path.home() / ".localarchive" / "data"
DEFAULT_DB_PATH = Path.home() / ".localarchive" / "archive.db"
DEFAULT_CONFIG_PATH = Path.home() / ".localarchive" / "config.toml"


@dataclass
class OCRConfig:
    engine: str = "paddleocr"
    languages: list[str] = field(default_factory=lambda: ["en"])
    confidence_threshold: float = 0.6


@dataclass
class ExtractionConfig:
    use_local_llm: bool = False
    ollama_model: str = "mistral"
    strategy: str = "regex"


@dataclass
class WatchConfig:
    interval_seconds: int = 5


@dataclass
class RuntimeConfig:
    max_workers: int = 1
    tmp_dir: Path = Path.home() / ".localarchive" / "tmp"
    fail_fast: bool = False
    cleanup_temp_files: bool = True


@dataclass
class ProcessingConfig:
    pdf_native_text_min_chars: int = 50
    default_limit: int = 50


@dataclass
class UIConfig:
    host: str = "127.0.0.1"
    port: int = 8877
    default_limit: int = 20
    show_preview_chars: int = 300


@dataclass
class Config:
    archive_dir: Path = DEFAULT_ARCHIVE_DIR
    db_path: Path = DEFAULT_DB_PATH
    ocr: OCRConfig = field(default_factory=OCRConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    ui: UIConfig = field(default_factory=UIConfig)
    watch: WatchConfig = field(default_factory=WatchConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)

    @classmethod
    def load(cls, config_path: Path = DEFAULT_CONFIG_PATH) -> "Config":
        """Load config from TOML file, falling back to defaults."""
        config = cls()
        if config_path.exists() and toml is not None:
            data = toml.load(config_path)
            general = data.get("general", {})
            if "archive_dir" in general:
                config.archive_dir = Path(os.path.expanduser(general["archive_dir"]))
            if "db_path" in general:
                config.db_path = Path(os.path.expanduser(general["db_path"]))
            ocr_data = data.get("ocr", {})
            if ocr_data:
                config.ocr = OCRConfig(
                    engine=ocr_data.get("engine", config.ocr.engine),
                    languages=ocr_data.get("languages", config.ocr.languages),
                    confidence_threshold=ocr_data.get("confidence_threshold", config.ocr.confidence_threshold),
                )
            ext_data = data.get("extraction", {})
            if ext_data:
                config.extraction = ExtractionConfig(
                    use_local_llm=ext_data.get("use_local_llm", config.extraction.use_local_llm),
                    ollama_model=ext_data.get("ollama_model", config.extraction.ollama_model),
                    strategy=ext_data.get("strategy", config.extraction.strategy),
                )
            ui_data = data.get("ui", {})
            if ui_data:
                config.ui = UIConfig(
                    host=ui_data.get("host", config.ui.host),
                    port=ui_data.get("port", config.ui.port),
                    default_limit=int(ui_data.get("default_limit", config.ui.default_limit)),
                    show_preview_chars=int(ui_data.get("show_preview_chars", config.ui.show_preview_chars)),
                )
            watch_data = data.get("watch", {})
            if watch_data:
                config.watch = WatchConfig(
                    interval_seconds=int(watch_data.get("interval_seconds", config.watch.interval_seconds)),
                )
            runtime_data = data.get("runtime", {})
            if runtime_data:
                config.runtime = RuntimeConfig(
                    max_workers=int(runtime_data.get("max_workers", config.runtime.max_workers)),
                    tmp_dir=Path(os.path.expanduser(runtime_data.get("tmp_dir", str(config.runtime.tmp_dir)))),
                    fail_fast=bool(runtime_data.get("fail_fast", config.runtime.fail_fast)),
                    cleanup_temp_files=bool(runtime_data.get("cleanup_temp_files", config.runtime.cleanup_temp_files)),
                )
            processing_data = data.get("processing", {})
            if processing_data:
                config.processing = ProcessingConfig(
                    pdf_native_text_min_chars=int(
                        processing_data.get("pdf_native_text_min_chars", config.processing.pdf_native_text_min_chars)
                    ),
                    default_limit=int(processing_data.get("default_limit", config.processing.default_limit)),
                )
        config.validate()
        return config

    def ensure_dirs(self) -> None:
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.runtime.tmp_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            # Fall back to archive-local temp storage when home-level tmp is not writable.
            self.runtime.tmp_dir = self.archive_dir / ".tmp"
            self.runtime.tmp_dir.mkdir(parents=True, exist_ok=True)

    def validate(self) -> None:
        valid_strategies = {"regex", "spacy", "ollama", "hybrid"}
        if self.extraction.strategy not in valid_strategies:
            raise ValueError(
                f"Invalid extraction.strategy '{self.extraction.strategy}'. "
                f"Expected one of: {', '.join(sorted(valid_strategies))}"
            )
        if self.watch.interval_seconds < 1:
            raise ValueError("watch.interval_seconds must be >= 1")
        if self.runtime.max_workers < 1:
            raise ValueError("runtime.max_workers must be >= 1")
        if self.processing.default_limit < 1:
            raise ValueError("processing.default_limit must be >= 1")
        if self.processing.pdf_native_text_min_chars < 0:
            raise ValueError("processing.pdf_native_text_min_chars must be >= 0")
        if self.ui.default_limit < 1:
            raise ValueError("ui.default_limit must be >= 1")
        if self.ui.show_preview_chars < 20:
            raise ValueError("ui.show_preview_chars must be >= 20")

    def save(self, config_path: Path = DEFAULT_CONFIG_PATH) -> None:
        if toml is None:
            return
        config_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "general": {"archive_dir": str(self.archive_dir), "db_path": str(self.db_path)},
            "ocr": {"engine": self.ocr.engine, "languages": self.ocr.languages, "confidence_threshold": self.ocr.confidence_threshold},
            "extraction": {
                "use_local_llm": self.extraction.use_local_llm,
                "ollama_model": self.extraction.ollama_model,
                "strategy": self.extraction.strategy,
            },
            "ui": {
                "host": self.ui.host,
                "port": self.ui.port,
                "default_limit": self.ui.default_limit,
                "show_preview_chars": self.ui.show_preview_chars,
            },
            "watch": {"interval_seconds": self.watch.interval_seconds},
            "runtime": {
                "max_workers": self.runtime.max_workers,
                "tmp_dir": str(self.runtime.tmp_dir),
                "fail_fast": self.runtime.fail_fast,
                "cleanup_temp_files": self.runtime.cleanup_temp_files,
            },
            "processing": {
                "pdf_native_text_min_chars": self.processing.pdf_native_text_min_chars,
                "default_limit": self.processing.default_limit,
            },
        }
        with open(config_path, "w") as f:
            toml.dump(data, f)
