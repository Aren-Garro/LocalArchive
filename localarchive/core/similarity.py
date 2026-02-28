"""Local document similarity utilities."""

from __future__ import annotations

import re


def _tokens(text: str) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(t) > 1}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def build_similarity_edges(
    docs: list[dict],
    *,
    top_k: int = 5,
    min_score: float = 0.15,
    model: str = "token-jaccard",
) -> list[dict]:
    """
    Build pairwise similarity edges for docs.

    Output uses unique undirected edges as (doc_id_a, doc_id_b).
    """
    top_k = max(1, int(top_k))
    min_score = max(0.0, min(1.0, float(min_score)))
    prepared: list[tuple[int, set[str]]] = []
    for doc in docs:
        doc_id = int(doc["id"])
        text = f"{doc.get('filename', '')} {doc.get('ocr_text', '')}"
        prepared.append((doc_id, _tokens(text)))

    neighbors: dict[int, list[tuple[int, float]]] = {doc_id: [] for doc_id, _ in prepared}
    for i in range(len(prepared)):
        a_id, a_tokens = prepared[i]
        for j in range(i + 1, len(prepared)):
            b_id, b_tokens = prepared[j]
            score = _jaccard(a_tokens, b_tokens)
            if score < min_score:
                continue
            neighbors[a_id].append((b_id, score))
            neighbors[b_id].append((a_id, score))

    for doc_id, links in neighbors.items():
        links.sort(key=lambda item: item[1], reverse=True)
        neighbors[doc_id] = links[:top_k]

    seen: set[tuple[int, int]] = set()
    edges: list[dict] = []
    for src_id, links in neighbors.items():
        for dst_id, score in links:
            a, b = (src_id, dst_id) if src_id < dst_id else (dst_id, src_id)
            if (a, b) in seen:
                continue
            seen.add((a, b))
            edges.append({"doc_id_a": a, "doc_id_b": b, "score": round(score, 6), "model": model})
    return edges

