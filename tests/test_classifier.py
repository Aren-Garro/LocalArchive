"""Tests for local Naive Bayes classifier helpers."""

from localarchive.core.classifier import evaluate, predict, train_model


def test_classifier_train_predict_evaluate():
    examples = [
        {"text": "invoice due amount", "label": "invoice"},
        {"text": "invoice balance total", "label": "invoice"},
        {"text": "patient clinic diagnosis", "label": "medical"},
        {"text": "hospital patient rx", "label": "medical"},
    ]
    model = train_model(examples)
    pred = predict(model, "invoice amount due")
    assert pred["label"] == "invoice"
    report = evaluate(model, examples)
    assert report["total"] == 4
    assert report["accuracy"] >= 0.5

