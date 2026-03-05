# Getting Started with LocalArchive

1. Initialize your workspace:
   - `python -m localarchive init`
2. Ingest one file or a folder:
   - `python -m localarchive ingest ./documents`
3. Process OCR + extraction:
   - `python -m localarchive process --workers 2`
4. Search your archive:
   - `python -m localarchive search "invoice 2026"`
5. Open local UI:
   - `python -m localarchive serve`

Tips:
- Use `python -m localarchive doctor` before first OCR run.
- Use `python -m localarchive backup create --path backup.zip` regularly.
