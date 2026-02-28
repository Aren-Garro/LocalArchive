"""Data exporter. Exports to CSV, JSON, and Markdown."""

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
