# Medical Records Organization Guide

1. Ingest scans from provider portals and phone captures:
   - `python -m localarchive ingest ./medical-records`
2. Process OCR:
   - `python -m localarchive process --workers 2`
3. Apply tags for faster retrieval:
   - `python -m localarchive tag <DOC_ID> medical urgent`
4. Run PII redaction before sharing:
   - `python -m localarchive redaction document <DOC_ID> --output redacted.txt`
5. Verify archive integrity:
   - `python -m localarchive verify --full`

Privacy tip:
- Keep backups encrypted at rest if stored outside your main workstation.
