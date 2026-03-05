"""Metadata profile rules and validators."""

from __future__ import annotations


PROFILE_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "core": ("title",),
    "research": ("title", "author", "year"),
    "archival": ("title", "creator", "date_created", "rights"),
}


def list_profiles() -> list[dict]:
    return [
        {"id": profile, "required_fields": list(required)}
        for profile, required in PROFILE_REQUIRED_FIELDS.items()
    ]


def validate_profile(profile: str, metadata: dict[str, dict], citations: list[dict]) -> dict:
    key = profile.strip().lower()
    required = PROFILE_REQUIRED_FIELDS.get(key)
    if not required:
        return {
            "ok": False,
            "profile": key,
            "missing_fields": [],
            "issues": [f"unknown_profile:{key}"],
        }
    missing: list[str] = []
    for field in required:
        value = str(metadata.get(field, {}).get("value", "")).strip()
        if not value:
            missing.append(field)
    issues: list[str] = []
    if missing:
        issues.append("missing_required_metadata")
    if key == "research":
        unresolved = [c for c in citations if str(c.get("status", "")) != "resolved"]
        if unresolved:
            issues.append("unresolved_citations")
    return {
        "ok": not issues,
        "profile": key,
        "missing_fields": missing,
        "issues": issues,
    }
