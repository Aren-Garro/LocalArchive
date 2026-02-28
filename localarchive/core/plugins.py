"""Local plugin discovery and metadata helpers."""

from __future__ import annotations

from pathlib import Path

from localarchive.config import Config

try:
    import toml
except ImportError:  # pragma: no cover
    toml = None


def _load_manifest(path: Path) -> dict | None:
    if not path.exists():
        return None
    if toml is not None:
        try:
            payload = toml.load(path)
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    # Minimal fallback parser for simple key="value" manifests.
    out: dict[str, str] = {}
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            out[key] = value
    except Exception:
        return None
    return out or None


def discover_plugins(config: Config) -> list[dict]:
    plugins: list[dict] = []
    enabled = set(config.plugins.enabled)
    seen: set[str] = set()
    for root in config.plugins.search_paths:
        root_path = Path(root).expanduser()
        if not root_path.exists() or not root_path.is_dir():
            continue
        for child in sorted(root_path.iterdir()):
            if not child.is_dir():
                continue
            manifest = _load_manifest(child / "plugin.toml")
            if not manifest:
                continue
            name = str(manifest.get("name", "")).strip()
            if not name or name in seen:
                continue
            seen.add(name)
            plugins.append(
                {
                    "name": name,
                    "version": str(manifest.get("version", "0.0.0")),
                    "kind": str(manifest.get("kind", "unknown")),
                    "description": str(manifest.get("description", "")),
                    "entrypoint": str(manifest.get("entrypoint", "")),
                    "path": str(child),
                    "enabled": name in enabled,
                }
            )
    return plugins


def get_plugin_by_name(config: Config, name: str) -> dict | None:
    target = name.strip().lower()
    for plugin in discover_plugins(config):
        if plugin["name"].strip().lower() == target:
            return plugin
    return None
