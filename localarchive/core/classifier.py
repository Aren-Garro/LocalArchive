"""Local text classifier (multinomial Naive Bayes) for document labeling."""

from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path


def tokenize(text: str) -> list[str]:
    return [t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(t) > 1]


def _softmax(log_scores: dict[str, float]) -> dict[str, float]:
    if not log_scores:
        return {}
    max_log = max(log_scores.values())
    exps = {k: math.exp(v - max_log) for k, v in log_scores.items()}
    total = sum(exps.values())
    if total <= 0:
        return {k: 0.0 for k in log_scores}
    return {k: v / total for k, v in exps.items()}


def train_model(examples: list[dict]) -> dict:
    """
    Train multinomial Naive Bayes model.

    Example schema: {"text": "...", "label": "invoice"}.
    """
    label_doc_counts: dict[str, int] = {}
    label_token_counts: dict[str, dict[str, int]] = {}
    label_total_tokens: dict[str, int] = {}
    vocab: set[str] = set()
    total_docs = 0

    for item in examples:
        label = str(item.get("label", "")).strip().lower()
        text = str(item.get("text", ""))
        if not label or not text:
            continue
        total_docs += 1
        label_doc_counts[label] = label_doc_counts.get(label, 0) + 1
        token_map = label_token_counts.setdefault(label, {})
        for token in tokenize(text):
            vocab.add(token)
            token_map[token] = token_map.get(token, 0) + 1
            label_total_tokens[label] = label_total_tokens.get(label, 0) + 1

    if total_docs < 1:
        raise ValueError("No valid training examples found.")
    labels = sorted(label_doc_counts)
    model = {
        "type": "naive_bayes",
        "labels": labels,
        "total_docs": total_docs,
        "label_doc_counts": label_doc_counts,
        "label_total_tokens": label_total_tokens,
        "label_token_counts": label_token_counts,
        "vocab": sorted(vocab),
        "vocab_size": len(vocab),
    }
    return model


def predict(model: dict, text: str) -> dict:
    labels = list(model.get("labels", []))
    if not labels:
        return {"label": "other", "confidence": 0.0}
    total_docs = max(1, int(model.get("total_docs", 1)))
    label_doc_counts = model.get("label_doc_counts", {})
    label_total_tokens = model.get("label_total_tokens", {})
    label_token_counts = model.get("label_token_counts", {})
    vocab_size = max(1, int(model.get("vocab_size", 1)))
    toks = tokenize(text)
    log_scores: dict[str, float] = {}
    for label in labels:
        prior = (int(label_doc_counts.get(label, 0)) + 1) / (total_docs + len(labels))
        score = math.log(prior)
        total_toks = int(label_total_tokens.get(label, 0))
        token_map = label_token_counts.get(label, {})
        denom = total_toks + vocab_size
        for token in toks:
            count = int(token_map.get(token, 0))
            score += math.log((count + 1) / denom)
        log_scores[label] = score
    probs = _softmax(log_scores)
    best = max(probs, key=probs.get)
    return {"label": best, "confidence": round(float(probs.get(best, 0.0)), 4)}


def evaluate(model: dict, examples: list[dict]) -> dict:
    total = 0
    correct = 0
    for item in examples:
        label = str(item.get("label", "")).strip().lower()
        text = str(item.get("text", ""))
        if not label or not text:
            continue
        total += 1
        pred = predict(model, text)
        if pred["label"] == label:
            correct += 1
    accuracy = (correct / total) if total else 0.0
    return {"total": total, "correct": correct, "accuracy": round(accuracy, 4)}


def save_model(model: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(model, f, indent=2, ensure_ascii=False)


def load_model(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_labeled_examples(path: Path, fmt: str = "csv") -> list[dict]:
    fmt = fmt.lower()
    if fmt == "json":
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, list):
            raise ValueError("JSON dataset must be a list of objects with `text` and `label`.")
        return [dict(item) for item in payload if isinstance(item, dict)]
    if fmt == "csv":
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]
    raise ValueError("Unsupported dataset format. Use csv or json.")

