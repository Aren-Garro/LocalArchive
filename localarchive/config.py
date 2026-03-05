"""
Configuration management for LocalArchive.
Reads from ~/.localarchive/config.toml, with sensible defaults.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    import toml
except ImportError:
    toml = None
try:
    import tomllib
except ImportError:
    tomllib = None

DEFAULT_ARCHIVE_DIR = Path.home() / ".localarchive" / "data"
DEFAULT_DB_PATH = Path.home() / ".localarchive" / "archive.db"
DEFAULT_CONFIG_PATH = Path.home() / ".localarchive" / "config.toml"


def _toml_load_file(config_path: Path) -> dict:
    if toml is not None:
        return toml.load(config_path)
    if tomllib is not None:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    return {}


def _toml_serialize(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_serialize(v) for v in value) + "]"
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def _toml_dump_file(config_path: Path, data: dict) -> None:
    if toml is not None:
        with open(config_path, "w", encoding="utf-8") as f:
            toml.dump(data, f)
        return
    with open(config_path, "w", encoding="utf-8") as f:
        for section, values in data.items():
            f.write(f"[{section}]\n")
            for key, value in values.items():
                f.write(f"{key} = {_toml_serialize(value)}\n")
            f.write("\n")


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
    manifest_path: Path = Path.home() / ".localarchive" / "tmp" / "watch_manifest.json"
    manifest_gc_days: int = 30


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
    commit_batch_size: int = 20
    writer_flush_ms: int = 200
    max_errors_per_run: int = 100
    resume_checkpoint_interval: int = 50


@dataclass
class ResearchConfig:
    citation_styles: list[str] = field(default_factory=lambda: ["apa"])
    default_collections: list[str] = field(
        default_factory=lambda: ["Research PDFs", "Needs Review"]
    )
    entity_priority: list[str] = field(default_factory=lambda: ["author", "topic", "journal"])


@dataclass
class AutopilotConfig:
    enabled: bool = True
    classification_model: str = "rules"
    confidence_threshold: float = 0.65
    auto_tag: bool = True
    model_path: Path = Path.home() / ".localarchive" / "models" / "classifier_nb.json"
    min_training_samples: int = 20


@dataclass
class SearchConfig:
    enable_semantic: bool = False
    embedding_model: str = "local-minilm"
    reranker: str = "none"
    snippet_chars: int = 300
    facet_defaults: list[str] = field(default_factory=lambda: ["file_type", "status", "tag"])
    enable_fuzzy: bool = False
    fuzzy_threshold: float = 0.78
    fuzzy_max_candidates: int = 300


@dataclass
class ReliabilityConfig:
    backup_interval: int = 86400
    integrity_check_on_startup: bool = False
    max_retries: int = 2
    checkpoint_batch_size: int = 25
    auto_verify_after_restore: bool = True
    backup_retention_count: int = 10
    backup_verify_on_create: bool = True
    max_imap_message_bytes: int = 25 * 1024 * 1024
    max_imap_attachment_bytes: int = 10 * 1024 * 1024


@dataclass
class PluginsConfig:
    enabled: list[str] = field(default_factory=list)
    search_paths: list[Path] = field(
        default_factory=lambda: [Path.home() / ".localarchive" / "plugins"]
    )


@dataclass
class UIConfig:
    host: str = "127.0.0.1"
    port: int = 8877
    default_limit: int = 20
    show_preview_chars: int = 300
    language: str = "en"
    max_upload_file_bytes: int = 25 * 1024 * 1024


def _expand_path(raw: str) -> Path:
    return Path(os.path.expanduser(raw))


def _apply_general(config: "Config", data: dict) -> None:
    if "archive_dir" in data:
        config.archive_dir = _expand_path(str(data["archive_dir"]))
    if "db_path" in data:
        config.db_path = _expand_path(str(data["db_path"]))


def _apply_ocr(config: "Config", data: dict) -> None:
    if not data:
        return
    config.ocr = OCRConfig(
        engine=data.get("engine", config.ocr.engine),
        languages=data.get("languages", config.ocr.languages),
        confidence_threshold=data.get("confidence_threshold", config.ocr.confidence_threshold),
    )


def _apply_extraction(config: "Config", data: dict) -> None:
    if not data:
        return
    config.extraction = ExtractionConfig(
        use_local_llm=data.get("use_local_llm", config.extraction.use_local_llm),
        ollama_model=data.get("ollama_model", config.extraction.ollama_model),
        strategy=data.get("strategy", config.extraction.strategy),
    )


def _apply_ui(config: "Config", data: dict) -> None:
    if not data:
        return
    config.ui = UIConfig(
        host=data.get("host", config.ui.host),
        port=data.get("port", config.ui.port),
        default_limit=int(data.get("default_limit", config.ui.default_limit)),
        show_preview_chars=int(data.get("show_preview_chars", config.ui.show_preview_chars)),
        language=str(data.get("language", config.ui.language)),
        max_upload_file_bytes=int(
            data.get("max_upload_file_bytes", config.ui.max_upload_file_bytes)
        ),
    )


def _apply_watch(config: "Config", data: dict) -> None:
    if not data:
        return
    config.watch = WatchConfig(
        interval_seconds=int(data.get("interval_seconds", config.watch.interval_seconds)),
        manifest_path=_expand_path(str(data.get("manifest_path", str(config.watch.manifest_path)))),
        manifest_gc_days=int(data.get("manifest_gc_days", config.watch.manifest_gc_days)),
    )


def _apply_runtime(config: "Config", data: dict) -> None:
    if not data:
        return
    config.runtime = RuntimeConfig(
        max_workers=int(data.get("max_workers", config.runtime.max_workers)),
        tmp_dir=_expand_path(str(data.get("tmp_dir", str(config.runtime.tmp_dir)))),
        fail_fast=bool(data.get("fail_fast", config.runtime.fail_fast)),
        cleanup_temp_files=bool(data.get("cleanup_temp_files", config.runtime.cleanup_temp_files)),
    )


def _apply_processing(config: "Config", data: dict) -> None:
    if not data:
        return
    config.processing = ProcessingConfig(
        pdf_native_text_min_chars=int(
            data.get("pdf_native_text_min_chars", config.processing.pdf_native_text_min_chars)
        ),
        default_limit=int(data.get("default_limit", config.processing.default_limit)),
        commit_batch_size=int(data.get("commit_batch_size", config.processing.commit_batch_size)),
        writer_flush_ms=int(data.get("writer_flush_ms", config.processing.writer_flush_ms)),
        max_errors_per_run=int(data.get("max_errors_per_run", config.processing.max_errors_per_run)),
        resume_checkpoint_interval=int(
            data.get("resume_checkpoint_interval", config.processing.resume_checkpoint_interval)
        ),
    )


def _apply_research(config: "Config", data: dict) -> None:
    if not data:
        return
    config.research = ResearchConfig(
        citation_styles=list(data.get("citation_styles", config.research.citation_styles)),
        default_collections=list(
            data.get("default_collections", config.research.default_collections)
        ),
        entity_priority=list(data.get("entity_priority", config.research.entity_priority)),
    )


def _apply_autopilot(config: "Config", data: dict) -> None:
    if not data:
        return
    config.autopilot = AutopilotConfig(
        enabled=bool(data.get("enabled", config.autopilot.enabled)),
        classification_model=data.get("classification_model", config.autopilot.classification_model),
        confidence_threshold=float(
            data.get("confidence_threshold", config.autopilot.confidence_threshold)
        ),
        auto_tag=bool(data.get("auto_tag", config.autopilot.auto_tag)),
        model_path=_expand_path(str(data.get("model_path", str(config.autopilot.model_path)))),
        min_training_samples=int(
            data.get("min_training_samples", config.autopilot.min_training_samples)
        ),
    )


def _apply_search(config: "Config", data: dict) -> None:
    if not data:
        return
    config.search = SearchConfig(
        enable_semantic=bool(data.get("enable_semantic", config.search.enable_semantic)),
        embedding_model=data.get("embedding_model", config.search.embedding_model),
        reranker=data.get("reranker", config.search.reranker),
        snippet_chars=int(data.get("snippet_chars", config.search.snippet_chars)),
        facet_defaults=list(data.get("facet_defaults", config.search.facet_defaults)),
        enable_fuzzy=bool(data.get("enable_fuzzy", config.search.enable_fuzzy)),
        fuzzy_threshold=float(data.get("fuzzy_threshold", config.search.fuzzy_threshold)),
        fuzzy_max_candidates=int(
            data.get("fuzzy_max_candidates", config.search.fuzzy_max_candidates)
        ),
    )


def _apply_reliability(config: "Config", data: dict) -> None:
    if not data:
        return
    config.reliability = ReliabilityConfig(
        backup_interval=int(data.get("backup_interval", config.reliability.backup_interval)),
        integrity_check_on_startup=bool(
            data.get("integrity_check_on_startup", config.reliability.integrity_check_on_startup)
        ),
        max_retries=int(data.get("max_retries", config.reliability.max_retries)),
        checkpoint_batch_size=int(
            data.get("checkpoint_batch_size", config.reliability.checkpoint_batch_size)
        ),
        auto_verify_after_restore=bool(
            data.get("auto_verify_after_restore", config.reliability.auto_verify_after_restore)
        ),
        backup_retention_count=int(
            data.get("backup_retention_count", config.reliability.backup_retention_count)
        ),
        backup_verify_on_create=bool(
            data.get("backup_verify_on_create", config.reliability.backup_verify_on_create)
        ),
        max_imap_message_bytes=int(
            data.get("max_imap_message_bytes", config.reliability.max_imap_message_bytes)
        ),
        max_imap_attachment_bytes=int(
            data.get("max_imap_attachment_bytes", config.reliability.max_imap_attachment_bytes)
        ),
    )


def _apply_plugins(config: "Config", data: dict) -> None:
    if not data:
        return
    config.plugins = PluginsConfig(
        enabled=list(data.get("enabled", config.plugins.enabled)),
        search_paths=[
            _expand_path(str(p))
            for p in data.get("search_paths", [str(x) for x in config.plugins.search_paths])
        ],
    )


_SECTION_APPLIERS = {
    "general": _apply_general,
    "ocr": _apply_ocr,
    "extraction": _apply_extraction,
    "ui": _apply_ui,
    "watch": _apply_watch,
    "runtime": _apply_runtime,
    "processing": _apply_processing,
    "research": _apply_research,
    "autopilot": _apply_autopilot,
    "search": _apply_search,
    "reliability": _apply_reliability,
    "plugins": _apply_plugins,
}


def _apply_sections(config: "Config", data: dict) -> None:
    for section, applier in _SECTION_APPLIERS.items():
        applier(config, data.get(section, {}))


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
    research: ResearchConfig = field(default_factory=ResearchConfig)
    autopilot: AutopilotConfig = field(default_factory=AutopilotConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    reliability: ReliabilityConfig = field(default_factory=ReliabilityConfig)
    plugins: PluginsConfig = field(default_factory=PluginsConfig)

    @classmethod
    def load(cls, config_path: Path = DEFAULT_CONFIG_PATH) -> "Config":
        """Load config from TOML file, falling back to defaults."""
        config = cls()
        if config_path.exists():
            _apply_sections(config, _toml_load_file(config_path))
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
        try:
            self.watch.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            self.watch.manifest_path = self.runtime.tmp_dir / "watch_manifest.json"
            self.watch.manifest_path.parent.mkdir(parents=True, exist_ok=True)

    def validate(self) -> None:
        self._validate_extraction()
        self._validate_watch()
        self._validate_runtime()
        self._validate_processing()
        self._validate_ui()
        self._validate_autopilot()
        self._validate_search()
        self._validate_reliability()
        self._validate_plugins()

    def _validate_extraction(self) -> None:
        if self.extraction.strategy not in {"regex", "spacy", "ollama", "hybrid"}:
            raise ValueError(
                f"Invalid extraction.strategy '{self.extraction.strategy}'. "
                "Expected one of: hybrid, ollama, regex, spacy"
            )

    def _validate_watch(self) -> None:
        if self.watch.interval_seconds < 1:
            raise ValueError("watch.interval_seconds must be >= 1")
        if self.watch.manifest_gc_days < 1:
            raise ValueError("watch.manifest_gc_days must be >= 1")

    def _validate_runtime(self) -> None:
        if self.runtime.max_workers < 1:
            raise ValueError("runtime.max_workers must be >= 1")

    def _validate_processing(self) -> None:
        if self.processing.default_limit < 1:
            raise ValueError("processing.default_limit must be >= 1")
        if self.processing.pdf_native_text_min_chars < 0:
            raise ValueError("processing.pdf_native_text_min_chars must be >= 0")
        if self.processing.commit_batch_size < 1:
            raise ValueError("processing.commit_batch_size must be >= 1")
        if self.processing.writer_flush_ms < 10:
            raise ValueError("processing.writer_flush_ms must be >= 10")
        if self.processing.max_errors_per_run < 1:
            raise ValueError("processing.max_errors_per_run must be >= 1")
        if self.processing.resume_checkpoint_interval < 1:
            raise ValueError("processing.resume_checkpoint_interval must be >= 1")

    def _validate_ui(self) -> None:
        if self.ui.default_limit < 1:
            raise ValueError("ui.default_limit must be >= 1")
        if self.ui.show_preview_chars < 20:
            raise ValueError("ui.show_preview_chars must be >= 20")
        if self.ui.max_upload_file_bytes < 1:
            raise ValueError("ui.max_upload_file_bytes must be >= 1")
        ui_language = str(self.ui.language).strip()
        if len(ui_language) != 2 or not ui_language.isalpha():
            raise ValueError("ui.language must be a 2-letter language code (for example: en, es)")

    def _validate_autopilot(self) -> None:
        if not 0 <= self.autopilot.confidence_threshold <= 1:
            raise ValueError("autopilot.confidence_threshold must be between 0 and 1")
        if self.autopilot.classification_model not in {"rules", "ml"}:
            raise ValueError("autopilot.classification_model must be one of: rules, ml")
        if self.autopilot.min_training_samples < 1:
            raise ValueError("autopilot.min_training_samples must be >= 1")

    def _validate_search(self) -> None:
        if self.search.snippet_chars < 50:
            raise ValueError("search.snippet_chars must be >= 50")
        if not 0 <= self.search.fuzzy_threshold <= 1:
            raise ValueError("search.fuzzy_threshold must be between 0 and 1")
        if self.search.fuzzy_max_candidates < 1:
            raise ValueError("search.fuzzy_max_candidates must be >= 1")

    def _validate_reliability(self) -> None:
        if self.reliability.backup_interval < 60:
            raise ValueError("reliability.backup_interval must be >= 60")
        if self.reliability.max_retries < 0:
            raise ValueError("reliability.max_retries must be >= 0")
        if self.reliability.checkpoint_batch_size < 1:
            raise ValueError("reliability.checkpoint_batch_size must be >= 1")
        if self.reliability.backup_retention_count < 1:
            raise ValueError("reliability.backup_retention_count must be >= 1")
        if self.reliability.max_imap_message_bytes < 1:
            raise ValueError("reliability.max_imap_message_bytes must be >= 1")
        if self.reliability.max_imap_attachment_bytes < 1:
            raise ValueError("reliability.max_imap_attachment_bytes must be >= 1")

    def _validate_plugins(self) -> None:
        if any(not str(name).strip() for name in self.plugins.enabled):
            raise ValueError("plugins.enabled cannot contain empty names")
        if not self.plugins.search_paths:
            raise ValueError("plugins.search_paths must contain at least one path")

    def _to_toml_data(self) -> dict:
        return {
            "general": {"archive_dir": str(self.archive_dir), "db_path": str(self.db_path)},
            "ocr": {
                "engine": self.ocr.engine,
                "languages": self.ocr.languages,
                "confidence_threshold": self.ocr.confidence_threshold,
            },
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
                "language": self.ui.language,
                "max_upload_file_bytes": self.ui.max_upload_file_bytes,
            },
            "watch": {
                "interval_seconds": self.watch.interval_seconds,
                "manifest_path": str(self.watch.manifest_path),
                "manifest_gc_days": self.watch.manifest_gc_days,
            },
            "runtime": {
                "max_workers": self.runtime.max_workers,
                "tmp_dir": str(self.runtime.tmp_dir),
                "fail_fast": self.runtime.fail_fast,
                "cleanup_temp_files": self.runtime.cleanup_temp_files,
            },
            "processing": {
                "pdf_native_text_min_chars": self.processing.pdf_native_text_min_chars,
                "default_limit": self.processing.default_limit,
                "commit_batch_size": self.processing.commit_batch_size,
                "writer_flush_ms": self.processing.writer_flush_ms,
                "max_errors_per_run": self.processing.max_errors_per_run,
                "resume_checkpoint_interval": self.processing.resume_checkpoint_interval,
            },
            "research": {
                "citation_styles": self.research.citation_styles,
                "default_collections": self.research.default_collections,
                "entity_priority": self.research.entity_priority,
            },
            "autopilot": {
                "enabled": self.autopilot.enabled,
                "classification_model": self.autopilot.classification_model,
                "confidence_threshold": self.autopilot.confidence_threshold,
                "auto_tag": self.autopilot.auto_tag,
                "model_path": str(self.autopilot.model_path),
                "min_training_samples": self.autopilot.min_training_samples,
            },
            "search": {
                "enable_semantic": self.search.enable_semantic,
                "embedding_model": self.search.embedding_model,
                "reranker": self.search.reranker,
                "snippet_chars": self.search.snippet_chars,
                "facet_defaults": self.search.facet_defaults,
                "enable_fuzzy": self.search.enable_fuzzy,
                "fuzzy_threshold": self.search.fuzzy_threshold,
                "fuzzy_max_candidates": self.search.fuzzy_max_candidates,
            },
            "reliability": {
                "backup_interval": self.reliability.backup_interval,
                "integrity_check_on_startup": self.reliability.integrity_check_on_startup,
                "max_retries": self.reliability.max_retries,
                "checkpoint_batch_size": self.reliability.checkpoint_batch_size,
                "auto_verify_after_restore": self.reliability.auto_verify_after_restore,
                "backup_retention_count": self.reliability.backup_retention_count,
                "backup_verify_on_create": self.reliability.backup_verify_on_create,
                "max_imap_message_bytes": self.reliability.max_imap_message_bytes,
                "max_imap_attachment_bytes": self.reliability.max_imap_attachment_bytes,
            },
            "plugins": {
                "enabled": self.plugins.enabled,
                "search_paths": [str(p) for p in self.plugins.search_paths],
            },
        }

    def save(self, config_path: Path = DEFAULT_CONFIG_PATH) -> None:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        _toml_dump_file(config_path, self._to_toml_data())
