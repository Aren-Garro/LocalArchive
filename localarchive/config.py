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
    research: ResearchConfig = field(default_factory=ResearchConfig)
    autopilot: AutopilotConfig = field(default_factory=AutopilotConfig)
    search: SearchConfig = field(default_factory=SearchConfig)
    reliability: ReliabilityConfig = field(default_factory=ReliabilityConfig)

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
                    confidence_threshold=ocr_data.get(
                        "confidence_threshold", config.ocr.confidence_threshold
                    ),
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
                    show_preview_chars=int(
                        ui_data.get("show_preview_chars", config.ui.show_preview_chars)
                    ),
                )
            watch_data = data.get("watch", {})
            if watch_data:
                config.watch = WatchConfig(
                    interval_seconds=int(
                        watch_data.get("interval_seconds", config.watch.interval_seconds)
                    ),
                    manifest_path=Path(
                        os.path.expanduser(
                            watch_data.get("manifest_path", str(config.watch.manifest_path))
                        )
                    ),
                    manifest_gc_days=int(
                        watch_data.get("manifest_gc_days", config.watch.manifest_gc_days)
                    ),
                )
            runtime_data = data.get("runtime", {})
            if runtime_data:
                config.runtime = RuntimeConfig(
                    max_workers=int(runtime_data.get("max_workers", config.runtime.max_workers)),
                    tmp_dir=Path(
                        os.path.expanduser(runtime_data.get("tmp_dir", str(config.runtime.tmp_dir)))
                    ),
                    fail_fast=bool(runtime_data.get("fail_fast", config.runtime.fail_fast)),
                    cleanup_temp_files=bool(
                        runtime_data.get("cleanup_temp_files", config.runtime.cleanup_temp_files)
                    ),
                )
            processing_data = data.get("processing", {})
            if processing_data:
                config.processing = ProcessingConfig(
                    pdf_native_text_min_chars=int(
                        processing_data.get(
                            "pdf_native_text_min_chars", config.processing.pdf_native_text_min_chars
                        )
                    ),
                    default_limit=int(
                        processing_data.get("default_limit", config.processing.default_limit)
                    ),
                    commit_batch_size=int(
                        processing_data.get(
                            "commit_batch_size", config.processing.commit_batch_size
                        )
                    ),
                    writer_flush_ms=int(
                        processing_data.get("writer_flush_ms", config.processing.writer_flush_ms)
                    ),
                    max_errors_per_run=int(
                        processing_data.get(
                            "max_errors_per_run", config.processing.max_errors_per_run
                        )
                    ),
                    resume_checkpoint_interval=int(
                        processing_data.get(
                            "resume_checkpoint_interval",
                            config.processing.resume_checkpoint_interval,
                        )
                    ),
                )
            research_data = data.get("research", {})
            if research_data:
                config.research = ResearchConfig(
                    citation_styles=list(
                        research_data.get("citation_styles", config.research.citation_styles)
                    ),
                    default_collections=list(
                        research_data.get(
                            "default_collections", config.research.default_collections
                        )
                    ),
                    entity_priority=list(
                        research_data.get("entity_priority", config.research.entity_priority)
                    ),
                )
            autopilot_data = data.get("autopilot", {})
            if autopilot_data:
                config.autopilot = AutopilotConfig(
                    enabled=bool(autopilot_data.get("enabled", config.autopilot.enabled)),
                    classification_model=autopilot_data.get(
                        "classification_model", config.autopilot.classification_model
                    ),
                    confidence_threshold=float(
                        autopilot_data.get(
                            "confidence_threshold", config.autopilot.confidence_threshold
                        )
                    ),
                    auto_tag=bool(autopilot_data.get("auto_tag", config.autopilot.auto_tag)),
                    model_path=Path(
                        os.path.expanduser(
                            autopilot_data.get("model_path", str(config.autopilot.model_path))
                        )
                    ),
                    min_training_samples=int(
                        autopilot_data.get(
                            "min_training_samples", config.autopilot.min_training_samples
                        )
                    ),
                )
            search_data = data.get("search", {})
            if search_data:
                config.search = SearchConfig(
                    enable_semantic=bool(
                        search_data.get("enable_semantic", config.search.enable_semantic)
                    ),
                    embedding_model=search_data.get(
                        "embedding_model", config.search.embedding_model
                    ),
                    reranker=search_data.get("reranker", config.search.reranker),
                    snippet_chars=int(
                        search_data.get("snippet_chars", config.search.snippet_chars)
                    ),
                    facet_defaults=list(
                        search_data.get("facet_defaults", config.search.facet_defaults)
                    ),
                    enable_fuzzy=bool(search_data.get("enable_fuzzy", config.search.enable_fuzzy)),
                    fuzzy_threshold=float(
                        search_data.get("fuzzy_threshold", config.search.fuzzy_threshold)
                    ),
                    fuzzy_max_candidates=int(
                        search_data.get("fuzzy_max_candidates", config.search.fuzzy_max_candidates)
                    ),
                )
            reliability_data = data.get("reliability", {})
            if reliability_data:
                config.reliability = ReliabilityConfig(
                    backup_interval=int(
                        reliability_data.get("backup_interval", config.reliability.backup_interval)
                    ),
                    integrity_check_on_startup=bool(
                        reliability_data.get(
                            "integrity_check_on_startup",
                            config.reliability.integrity_check_on_startup,
                        )
                    ),
                    max_retries=int(
                        reliability_data.get("max_retries", config.reliability.max_retries)
                    ),
                    checkpoint_batch_size=int(
                        reliability_data.get(
                            "checkpoint_batch_size", config.reliability.checkpoint_batch_size
                        )
                    ),
                    auto_verify_after_restore=bool(
                        reliability_data.get(
                            "auto_verify_after_restore",
                            config.reliability.auto_verify_after_restore,
                        )
                    ),
                    backup_retention_count=int(
                        reliability_data.get(
                            "backup_retention_count", config.reliability.backup_retention_count
                        )
                    ),
                    backup_verify_on_create=bool(
                        reliability_data.get(
                            "backup_verify_on_create", config.reliability.backup_verify_on_create
                        )
                    ),
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
        try:
            self.watch.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            self.watch.manifest_path = self.runtime.tmp_dir / "watch_manifest.json"
            self.watch.manifest_path.parent.mkdir(parents=True, exist_ok=True)

    def validate(self) -> None:
        valid_strategies = {"regex", "spacy", "ollama", "hybrid"}
        if self.extraction.strategy not in valid_strategies:
            raise ValueError(
                f"Invalid extraction.strategy '{self.extraction.strategy}'. "
                f"Expected one of: {', '.join(sorted(valid_strategies))}"
            )
        if self.watch.interval_seconds < 1:
            raise ValueError("watch.interval_seconds must be >= 1")
        if self.watch.manifest_gc_days < 1:
            raise ValueError("watch.manifest_gc_days must be >= 1")
        if self.runtime.max_workers < 1:
            raise ValueError("runtime.max_workers must be >= 1")
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
        if self.ui.default_limit < 1:
            raise ValueError("ui.default_limit must be >= 1")
        if self.ui.show_preview_chars < 20:
            raise ValueError("ui.show_preview_chars must be >= 20")
        if not 0 <= self.autopilot.confidence_threshold <= 1:
            raise ValueError("autopilot.confidence_threshold must be between 0 and 1")
        if self.autopilot.classification_model not in {"rules", "ml"}:
            raise ValueError("autopilot.classification_model must be one of: rules, ml")
        if self.autopilot.min_training_samples < 1:
            raise ValueError("autopilot.min_training_samples must be >= 1")
        if self.search.snippet_chars < 50:
            raise ValueError("search.snippet_chars must be >= 50")
        if not 0 <= self.search.fuzzy_threshold <= 1:
            raise ValueError("search.fuzzy_threshold must be between 0 and 1")
        if self.search.fuzzy_max_candidates < 1:
            raise ValueError("search.fuzzy_max_candidates must be >= 1")
        if self.reliability.backup_interval < 60:
            raise ValueError("reliability.backup_interval must be >= 60")
        if self.reliability.max_retries < 0:
            raise ValueError("reliability.max_retries must be >= 0")
        if self.reliability.checkpoint_batch_size < 1:
            raise ValueError("reliability.checkpoint_batch_size must be >= 1")
        if self.reliability.backup_retention_count < 1:
            raise ValueError("reliability.backup_retention_count must be >= 1")

    def save(self, config_path: Path = DEFAULT_CONFIG_PATH) -> None:
        if toml is None:
            return
        config_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
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
            },
        }
        with open(config_path, "w") as f:
            toml.dump(data, f)
