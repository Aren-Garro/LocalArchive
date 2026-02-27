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

# Ingest an entire folder
python -m localarchive.cli ingest ./documents/

# Watch a folder for new files (continuous)
python -m localarchive.cli watch ./documents/

# Watch once and exit (single scan cycle)
python -m localarchive.cli watch ./documents/ --once

# Process pending files (OCR + field extraction)
python -m localarchive.cli process

# Requeue failed documents for OCR retry
python -m localarchive.cli reprocess --status error

# Choose extractor strategy
python -m localarchive.cli process --extractor hybrid

# Search your archive
python -m localarchive.cli search "dentist 2024"

# Export results to CSV
python -m localarchive.cli export --query "receipts" --format csv --output results.csv

# Tag a document
python -m localarchive.cli tag DOC_ID "medical" "2024"

# Launch web UI (optional)
python -m localarchive.cli serve

# Environment and dependency checks
python -m localarchive.cli doctor

# Document detail page
# http://127.0.0.1:8877/documents/<DOC_ID>
```

## Core Principles

1. **Local by default** — Everything runs on your machine. No network calls.
2. **Zero accounts** — No sign-ups, no API keys for basic usage.
3. **Open formats** — Data stored in SQLite, exports to CSV/JSON/Markdown.
4. **Privacy first** — Your documents never leave your device.
5. **Offline always** — Full functionality without internet.

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

[runtime]
max_workers = 1
tmp_dir = "~/.localarchive/tmp"
fail_fast = false
cleanup_temp_files = true

[processing]
pdf_native_text_min_chars = 50
default_limit = 50
```

## Roadmap

- [x] Project architecture & scaffolding
- [x] CLI with init, ingest, search, export, tag, process, reprocess, watch, doctor, serve commands
- [x] SQLite + FTS5 database layer
- [x] PDF / image ingestion pipeline
- [x] OCR integration (PaddleOCR / EasyOCR abstraction)
- [x] Structured field extraction (regex + optional spaCy/Ollama strategies)
- [x] Web UI with search and document detail viewer
- [x] Folder watcher (auto-ingest new files)
- [x] Ollama integration for smart extraction (optional/local)
- [ ] CRDT-based sync for multi-device (future)

## License

MIT License — see [LICENSE](LICENSE) for details.
