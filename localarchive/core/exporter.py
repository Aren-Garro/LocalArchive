"""Data exporter. Exports to CSV, JSON, Markdown, CSL-JSON, BibTeX, and RIS."""

import csv
import json
from pathlib import Path

from rich.console import Console

console = Console()


def export_csv(documents: list[dict], output_path: Path) -> None:
    if not documents:
        console.print("[yellow]No documents to export.[/yellow]")
        return
    fieldnames = list(documents[0].keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(documents)
    console.print(f"[green]Exported {len(documents)} documents to {output_path}[/green]")


def export_json(documents: list[dict], output_path: Path) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(documents, f, indent=2, ensure_ascii=False, default=str)
    console.print(f"[green]Exported {len(documents)} documents to {output_path}[/green]")


def export_markdown(documents: list[dict], output_path: Path) -> None:
    lines = ["# LocalArchive Export\n"]
    for doc in documents:
        lines.append(f"## {doc.get('filename', 'Untitled')}\n")
        lines.append(f"- **ID:** {doc.get('id', 'N/A')}")
        lines.append(f"- **Type:** {doc.get('file_type', 'N/A')}")
        lines.append(f"- **Ingested:** {doc.get('ingested_at', 'N/A')}")
        if doc.get("tags"):
            lines.append(f"- **Tags:** {doc['tags']}")
        if doc.get("ocr_text"):
            lines.append(f"\n> {doc['ocr_text'][:500]}\n")
        lines.append("---\n")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    console.print(f"[green]Exported {len(documents)} documents to {output_path}[/green]")


def export_csljson(documents: list[dict], output_path: Path) -> None:
    payload = []
    for doc in documents:
        meta = doc.get("metadata", {}) or {}
        title = str(meta.get("title", {}).get("value", "") or doc.get("filename", "Untitled"))
        author_raw = str(meta.get("author", {}).get("value", "")).strip()
        authors = []
        for token in [x.strip() for x in author_raw.split(";") if x.strip()]:
            parts = token.split(maxsplit=1)
            family = parts[-1] if parts else token
            given = parts[0] if len(parts) > 1 else ""
            authors.append({"family": family, "given": given})
        year = str(meta.get("year", {}).get("value", "")).strip()
        entry = {
            "id": f"localarchive-{doc.get('id', '')}",
            "type": "article",
            "title": title,
            "author": authors,
            "issued": {"date-parts": [[int(year)]]} if year.isdigit() else {},
        }
        payload.append(entry)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    console.print(f"[green]Exported {len(documents)} documents to {output_path}[/green]")


def export_bibtex(documents: list[dict], output_path: Path) -> None:
    lines: list[str] = []
    for doc in documents:
        meta = doc.get("metadata", {}) or {}
        doc_id = int(doc.get("id", 0))
        title = str(meta.get("title", {}).get("value", "") or doc.get("filename", "Untitled"))
        author = str(meta.get("author", {}).get("value", "")).strip()
        year = str(meta.get("year", {}).get("value", "")).strip()
        lines.append(f"@article{{localarchive{doc_id},")
        lines.append(f"  title = {{{title}}},")
        if author:
            lines.append(f"  author = {{{author}}},")
        if year:
            lines.append(f"  year = {{{year}}},")
        lines.append("}\n")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    console.print(f"[green]Exported {len(documents)} documents to {output_path}[/green]")


def export_ris(documents: list[dict], output_path: Path) -> None:
    lines: list[str] = []
    for doc in documents:
        meta = doc.get("metadata", {}) or {}
        title = str(meta.get("title", {}).get("value", "") or doc.get("filename", "Untitled"))
        author = str(meta.get("author", {}).get("value", "")).strip()
        year = str(meta.get("year", {}).get("value", "")).strip()
        lines.append("TY  - JOUR")
        lines.append(f"ID  - localarchive-{doc.get('id', '')}")
        lines.append(f"TI  - {title}")
        if author:
            lines.append(f"AU  - {author}")
        if year:
            lines.append(f"PY  - {year}")
        lines.append("ER  - ")
        lines.append("")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    console.print(f"[green]Exported {len(documents)} documents to {output_path}[/green]")
