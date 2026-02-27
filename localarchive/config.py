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


@dataclass
class UIConfig:
    host: str = "127.0.0.1"
    port: int = 8877


@dataclass
class Config:
    archive_dir: Path = DEFAULT_ARCHIVE_DIR
    db_path: Path = DEFAULT_DB_PATH
    ocr: OCRConfig = field(default_factory=OCRConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    ui: UIConfig = field(default_factory=UIConfig)

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
                )
            ui_data = data.get("ui", {})
            if ui_data:
                config.ui = UIConfig(
                    host=ui_data.get("host", config.ui.host),
                    port=ui_data.get("port", config.ui.port),
                )
        return config

    def ensure_dirs(self) -> None:
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, config_path: Path = DEFAULT_CONFIG_PATH) -> None:
        if toml is None:
            return
        config_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "general": {"archive_dir": str(self.archive_dir), "db_path": str(self.db_path)},
            "ocr": {"engine": self.ocr.engine, "languages": self.ocr.languages, "confidence_threshold": self.ocr.confidence_threshold},
            "extraction": {"use_local_llm": self.extraction.use_local_llm, "ollama_model": self.extraction.ollama_model},
            "ui": {"host": self.ui.host, "port": self.ui.port},
        }
        with open(config_path, "w") as f:
            toml.dump(data, f)
