"""Tests for localarchive.core.extractor"""
from localarchive.core.extractor import extract_fields


def test_extract_dates():
    text = "Invoice date: 01/15/2025. Due by March 1, 2025."
    fields = extract_fields(text)
    dates = [f for f in fields if f.field_type == "date"]
    assert len(dates) >= 1


def test_extract_amounts():
    text = "Total: $1,250.00. Tax: $87.50. Subtotal was 1,162.50"
    fields = extract_fields(text)
    amounts = [f for f in fields if f.field_type == "amount"]
    assert len(amounts) >= 2


def test_extract_emails():
    text = "Contact us at billing@example.com or support@acme.org"
    fields = extract_fields(text)
    emails = [f for f in fields if f.field_type == "email"]
    assert len(emails) == 2


def test_extract_phones():
    text = "Call (555) 123-4567 or +1 800-555-0199"
    fields = extract_fields(text)
    phones = [f for f in fields if f.field_type == "phone"]
    assert len(phones) >= 1


def test_extract_hybrid_includes_regex():
    text = "Invoice total $12.50 on 01/15/2025"
    fields = extract_fields(text, mode="hybrid")
    assert any(f.field_type == "amount" for f in fields)
    assert any(f.field_type == "date" for f in fields)
