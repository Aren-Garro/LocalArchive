# 📦 LocalArchive

**Turn your chaotic pile of PDFs, scans, and documents into a private, offline, searchable library.**

LocalArchive is a **local-first document intelligence pipeline** — it runs entirely on your machine, requires no accounts, no subscriptions, and no cloud uploads. Your data stays yours.

## What It Does

1. **Scans & reads documents** — Drop in PDFs, images, scans. OCR extracts the text automatically.
2. **Pulls out key fields** — Dates, names, amounts, invoice numbers — structured extraction without cloud APIs.
3. **Makes everything searchable** — Full-text search across your entire document library, offline.
4. **Exports clean data** — Output to CSV, JSON, or Markdown for use anywhere.

## Who Is This For?

- **Researchers** digitizing archives and papers
- **Freelancers** managing invoices and receipts
- **Patients / caregivers** organizing medical records
- **Anyone** drowning in paperwork

## Architecture

```
localarchive/
├── cli.py              # Command-line interface (main entry point)
├── core/
│   ├── ingester.py     # File ingestion — watches folders, imports docs
│   ├── ocr_engine.py   # OCR processing (PaddleOCR / EasyOCR)
│   ├── extractor.py    # Structured field extraction (dates, amounts, names)
│   └── exporter.py     # Export to CSV, JSON, Markdown
├── db/
│   ├── database.py     # SQLite database manager
│   ├── models.py       # Data models (Document, Tag, Field)
│   └── search.py       # Full-text search engine (FTS5)
├── ui/
│   └── app.py          # Web UI (FastAPI + HTMX, optional)
├── config.py           # Configuration & settings
└── utils.py            # Shared utilities (hashing, file type detection)
```

## Quick Start

### Prerequisites

- Python 3.10+
- pip

### Install

```bash
# Clone
git clone https://github.com/Aren-Garro/LocalArchive.git
cd LocalArchive

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize database
python -m localarchive.cli init
```

### Basic Usage

```bash
# Initialize database and config
python -m localarchive.cli init

# Ingest a single file
python -m localarchive.cli ingest invoice.pdf

# Ingest with research profile (auto research tagging)
python -m localarchive.cli ingest paper.pdf --profile research

# Ingest an entire folder
python -m localarchive.cli ingest ./documents/

# Watch a folder for new files (continuous)
python -m localarchive.cli watch ./documents/

# Watch once and exit (single scan cycle)
python -m localarchive.cli watch ./documents/ --once

# Process pending files (OCR + field extraction)
python -m localarchive.cli process

# Process in parallel with batched DB commits
python -m localarchive.cli process --workers 4 --commit-batch-size 20

# Dry-run / resume controls for robust long runs
python -m localarchive.cli process --dry-run
python -m localarchive.cli process --resume
python -m localarchive.cli process --from-run 12 --max-errors 10
python -m localarchive.cli process --json

# Requeue failed documents for OCR retry
python -m localarchive.cli reprocess --status error

# Choose extractor strategy
python -m localarchive.cli process --extractor hybrid

# Search your archive
python -m localarchive.cli search "dentist 2024"

# Hybrid search flags (semantic routing if enabled in config)
python -m localarchive.cli search "graph neural nets" --semantic --bm25-weight 0.6 --vector-weight 0.4

# OCR-tolerant fuzzy search
python -m localarchive.cli search "reciept clinic" --fuzzy

# JSON and explainability output for automation/debugging
python -m localarchive.cli search "receipt" --json
python -m localarchive.cli search "receipt" --explain-ranking

# Export results to CSV
python -m localarchive.cli export --query "receipts" --format csv --output results.csv

# Tag a document
python -m localarchive.cli tag DOC_ID "medical" "2024"

# Auto-classify processed docs and apply category tags
python -m localarchive.cli classify --limit 500
python -m localarchive.cli classify --limit 200 --explain

# Launch web UI (optional)
python -m localarchive.cli serve

# Environment and dependency checks
python -m localarchive.cli doctor
python -m localarchive.cli doctor --json

# Build and inspect smart collections
python -m localarchive.cli collections auto-build
python -m localarchive.cli collections list

# Timeline view by extracted entity
python -m localarchive.cli timeline --entity topic

# Integrity audit and optional repair
python -m localarchive.cli audit
python -m localarchive.cli audit --repair
python -m localarchive.cli verify --json
python -m localarchive.cli verify --full --json

# Local backup / restore
python -m localarchive.cli backup create --path localarchive-backup.zip
python -m localarchive.cli backup list
python -m localarchive.cli backup restore --path localarchive-backup.zip

# Document detail page
# http://127.0.0.1:8877/documents/<DOC_ID>
```

## Core Principles

1. **Local by default** — Everything runs on your machine. No network calls.
2. **Zero accounts** — No sign-ups, no API keys for basic usage.
3. **Open formats** — Data stored in SQLite, exports to CSV/JSON/Markdown.
4. **Privacy first** — Your documents never leave your device.
5. **Offline always** — Full functionality without internet.
6. **Free forever** — No fees, no subscriptions, no paywalls.

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | Python 3.10+ | Broad ecosystem, accessible to contributors |
| Database | SQLite + FTS5 | Zero-config, battle-tested, runs everywhere |
| OCR | PaddleOCR / EasyOCR | Open-source, 80+ languages, good accuracy |
| Extraction | regex + spaCy (optional local LLM) | No cloud dependency |
| CLI | Click | Clean, composable commands |
| Web UI | FastAPI + HTMX | Lightweight, no JS framework needed |
| PDF parsing | PyMuPDF (fitz) | Fast, reliable PDF text + image extraction |

## Configuration

Settings are stored in `~/.localarchive/config.toml`:

```toml
[general]
archive_dir = "~/.localarchive/data"
db_path = "~/.localarchive/archive.db"

[ocr]
engine = "paddleocr"           # "paddleocr" or "easyocr"
languages = ["en"]
confidence_threshold = 0.6

[extraction]
use_local_llm = false          # Enable Ollama-based extraction
ollama_model = "mistral"
strategy = "regex"             # regex | spacy | ollama | hybrid

[ui]
host = "127.0.0.1"
port = 8877
default_limit = 20
show_preview_chars = 300

[watch]
interval_seconds = 5
manifest_path = "~/.localarchive/tmp/watch_manifest.json"
manifest_gc_days = 30

[runtime]
max_workers = 1
tmp_dir = "~/.localarchive/tmp"
fail_fast = false
cleanup_temp_files = true

[processing]
pdf_native_text_min_chars = 50
default_limit = 50
commit_batch_size = 20
writer_flush_ms = 200
max_errors_per_run = 100
resume_checkpoint_interval = 50

[research]
citation_styles = ["apa"]
default_collections = ["Research PDFs", "Needs Review"]
entity_priority = ["author", "topic", "journal"]

[autopilot]
enabled = true
classification_model = "rules"
confidence_threshold = 0.65
auto_tag = true

[search]
enable_semantic = false
embedding_model = "local-minilm"
reranker = "none"
snippet_chars = 300
facet_defaults = ["file_type", "status", "tag"]
enable_fuzzy = false
fuzzy_threshold = 0.78
fuzzy_max_candidates = 300

[reliability]
backup_interval = 86400
integrity_check_on_startup = false
max_retries = 2
checkpoint_batch_size = 25
auto_verify_after_restore = true
backup_retention_count = 10
backup_verify_on_create = true
```

## Roadmap

### Completed ✓
- [x] Project architecture & scaffolding
- [x] CLI with init, ingest, search, export, tag, process, reprocess, watch, doctor, collections, timeline, audit, backup, serve commands
- [x] SQLite + FTS5 database layer
- [x] PDF / image ingestion pipeline
- [x] OCR integration (PaddleOCR / EasyOCR abstraction)
- [x] Structured field extraction (regex + optional spaCy/Ollama strategies)
- [x] Web UI with search and document detail viewer
- [x] Folder watcher (auto-ingest new files)
- [x] Ollama integration for smart extraction (optional/local)
- [x] Parallel processing with worker threads and batched DB writes
- [x] Fuzzy OCR-tolerant search fallback
- [x] Rules-based document classification command (`classify`)

### Core Features (High Priority)
- [ ] **Learned smart classification** — Replace rules with stronger local ML model quality
- [ ] **Table extraction** — Pull structured data from tables in PDFs and images
- [ ] **Document similarity** — Auto-discover related documents in your archive
- [ ] **Enhanced web UI** — Drag-and-drop upload, thumbnail previews, mobile-responsive design
- [ ] **Multi-language OCR** — Expand beyond English with tested support for Spanish, French, German, Chinese, Arabic
- [ ] **Accessibility features** — Screen reader support, keyboard navigation, high-contrast themes

### Community & Ecosystem (Medium Priority)
- [ ] **Plugin architecture** — Allow community-built extractors, exporters, and custom processors
- [ ] **Localization (i18n)** — Translate UI and documentation to make LocalArchive globally accessible
- [ ] **Docker containers** — One-command deployment for less technical users
- [ ] **Pre-built binaries** — Windows .exe, macOS .app, Linux AppImage for non-Python users
- [ ] **Community template library** — Share extraction rules for common document types (W-2s, insurance forms, research papers)
- [ ] **Integration connectors** — Email ingestion (IMAP), folder sync daemons, workflow automation hooks
- [ ] **Educational resources** — Video tutorials, use-case guides, academic partnership programs

### Advanced Intelligence (Future)
- [ ] **Entity relationship graphs** — Visualize connections between documents (invoice → payment → receipt)
- [ ] **Multi-device sync (CRDT)** — Local-first sync without cloud dependency
- [ ] **Duplicate detection** — Perceptual hashing to identify duplicate scans
- [ ] **Data validation** — Confidence scoring and manual review queues for low-confidence extractions
- [ ] **Redaction tools** — Privacy-safe document sharing with automated PII removal
- [ ] **Version control** — Track document changes over time
- [ ] **Citation extraction** — Auto-build bibliographies from research papers

## License

MIT License — see [LICENSE](LICENSE) for details.
