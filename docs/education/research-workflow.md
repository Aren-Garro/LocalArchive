# Research Workflow Guide

Recommended flow for academic papers:

1. Ingest papers:
   - `python -m localarchive ingest ./papers`
2. Process with multilingual OCR:
   - `python -m localarchive process --ocr-languages en,es,fr,de --ocr-engine easyocr`
3. Apply research template tags:
   - `python -m localarchive templates apply --template research_paper --all`
4. Extract citations:
   - `python -m localarchive citations extract --format json`
5. Build entity graph:
   - `python -m localarchive graph entities --json`

Use `python -m localarchive review build --threshold 0.6` to queue low-confidence extractions.
