"""Search command implementation."""

from localarchive import cli as c
from localarchive.db.search import SearchEngine


def run_search(
    *,
    query: str,
    tag: str | None,
    file_type: str | None,
    legacy_file_type: str | None,
    limit: int | None,
    semantic: bool,
    bm25_weight: float,
    vector_weight: float,
    fuzzy: bool,
    fuzzy_threshold: float | None,
    fuzzy_max_candidates: int | None,
    explain_ranking: bool,
    as_json: bool,
) -> None:
    config = c.get_config()
    max_results = limit if limit is not None else config.ui.default_limit
    c._validate_limit(max_results)
    if legacy_file_type and not file_type:
        file_type = legacy_file_type
        c.console.print("[yellow]`--type` is deprecated. Use `--file-type`.[/yellow]")
    db = c.get_db(config)
    engine = SearchEngine(db)
    if semantic:
        bm25_weight, vector_weight = c._validate_hybrid_weights(bm25_weight, vector_weight)
    if semantic and config.search.enable_semantic:
        results = engine.search_hybrid(
            query,
            limit=max_results,
            tag=tag,
            file_type=file_type,
            bm25_weight=bm25_weight,
            vector_weight=vector_weight,
        )
    else:
        results = engine.search(query, limit=max_results, tag=tag, file_type=file_type)
    fuzzy_enabled = fuzzy or config.search.enable_fuzzy
    if fuzzy_enabled:
        threshold = fuzzy_threshold if fuzzy_threshold is not None else config.search.fuzzy_threshold
        max_candidates = (
            fuzzy_max_candidates
            if fuzzy_max_candidates is not None
            else config.search.fuzzy_max_candidates
        )
        c._validate_threshold("fuzzy-threshold", threshold)
        c._validate_limit(max_candidates)
        fuzzy_results = engine.search_fuzzy(
            query,
            limit=max_results,
            tag=tag,
            file_type=file_type,
            threshold=threshold,
            max_candidates=max_candidates,
        )
        if results:
            seen = {int(doc["id"]) for doc in results}
            for doc in fuzzy_results:
                if int(doc["id"]) in seen:
                    continue
                results.append(doc)
                seen.add(int(doc["id"]))
                if len(results) >= max_results:
                    break
        else:
            results = fuzzy_results
    if semantic and not config.search.enable_semantic:
        c.console.print(
            "[yellow]Semantic search is disabled in config.search.enable_semantic; using BM25 only.[/yellow]"
        )
    if semantic:
        c.console.print(
            f"[dim]Hybrid search request: bm25_weight={bm25_weight:.2f}, vector_weight={vector_weight:.2f}[/dim]"
        )
    if fuzzy_enabled:
        c.console.print(
            f"[dim]Fuzzy search enabled: threshold={threshold:.2f} candidates={max_candidates}[/dim]"
        )
    if not results:
        if as_json:
            c._emit_json(
                {
                    "query": query,
                    "count": 0,
                    "semantic": bool(semantic and config.search.enable_semantic),
                    "fuzzy": bool(fuzzy_enabled),
                    "results": [],
                }
            )
        else:
            c.console.print("[yellow]No results found.[/yellow]")
            c.console.print(
                '[dim]Hint: try `localarchive search "<term>" --fuzzy` or broaden filters.[/dim]'
            )
        db.close()
        return
    if as_json:
        payload = {
            "query": query,
            "count": len(results),
            "semantic": bool(semantic and config.search.enable_semantic),
            "fuzzy": bool(fuzzy_enabled),
            "results": [],
        }
        for doc in results:
            item = {
                "id": int(doc["id"]),
                "filename": doc["filename"],
                "file_type": doc.get("file_type"),
                "ingested_at": doc.get("ingested_at"),
                "preview": (doc.get("ocr_text") or "")[:120],
            }
            if "rank" in doc:
                item["rank"] = doc["rank"]
            if "hybrid_score" in doc:
                item["hybrid_score"] = doc["hybrid_score"]
            if "fuzzy_score" in doc:
                item["fuzzy_score"] = doc["fuzzy_score"]
            payload["results"].append(item)
        c._emit_json(payload)
        db.close()
        return
    table = c.Table(title=f"Search: {query}")
    table.add_column("ID", style="cyan", width=6)
    table.add_column("Filename", style="bold")
    table.add_column("Type", width=6)
    table.add_column("Ingested", width=22)
    table.add_column("Preview", max_width=50)
    for doc in results:
        preview = (doc.get("ocr_text") or "")[:80]
        table.add_row(
            str(doc["id"]),
            doc["filename"],
            doc.get("file_type", "?"),
            doc.get("ingested_at", ""),
            preview,
        )
    c.console.print(table)
    if explain_ranking:
        rank_table = c.Table(title="Ranking Explanation")
        rank_table.add_column("ID", style="cyan", width=6)
        rank_table.add_column("rank", width=12)
        rank_table.add_column("hybrid", width=12)
        rank_table.add_column("fuzzy", width=12)
        for doc in results:
            rank_table.add_row(
                str(doc["id"]),
                str(round(float(doc.get("rank", 0.0)), 6)) if "rank" in doc else "-",
                str(doc.get("hybrid_score", "-")),
                str(doc.get("fuzzy_score", "-")),
            )
        c.console.print(rank_table)
    c.console.print(f"\n[dim]{len(results)} result(s)[/dim]")
    db.close()

