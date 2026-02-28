"""Tests for plugin discovery and CLI management."""

import uuid
from pathlib import Path

from click.testing import CliRunner

from localarchive.cli import main


def _workspace_tmp_dir(prefix: str) -> Path:
    root = Path.cwd() / ".test_tmp"
    root.mkdir(exist_ok=True)
    path = root / f"{prefix}-{uuid.uuid4().hex[:8]}"
    path.mkdir(exist_ok=True)
    return path


def _write_plugin(plugin_root: Path, name: str) -> None:
    p = plugin_root / name
    p.mkdir(parents=True, exist_ok=True)
    (p / "plugin.toml").write_text(
        (
            f'name = "{name}"\n'
            'version = "0.1.0"\n'
            'kind = "extractor"\n'
            f'description = "{name} plugin"\n'
            f'entrypoint = "{name}.main"\n'
        ),
        encoding="utf-8",
    )


def test_plugins_list_inspect_enable_disable():
    tmp_path = _workspace_tmp_dir("localarchive-plugins")
    cfg_path = tmp_path / "config.toml"
    plugin_root = tmp_path / "plugins"
    _write_plugin(plugin_root, "demo_plugin")

    cfg_path.write_text(
        (
            "[general]\n"
            f'archive_dir = "{(tmp_path / "archive").as_posix()}"\n'
            f'db_path = "{(tmp_path / "archive.db").as_posix()}"\n\n'
            "[plugins]\n"
            "enabled = []\n"
            f'search_paths = ["{plugin_root.as_posix()}"]\n'
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    listed = runner.invoke(main, ["--config", str(cfg_path), "plugins", "list", "--json"])
    assert listed.exit_code == 0
    assert '"name": "demo_plugin"' in listed.output
    assert '"enabled": false' in listed.output

    inspected = runner.invoke(
        main, ["--config", str(cfg_path), "plugins", "inspect", "demo_plugin", "--json"]
    )
    assert inspected.exit_code == 0
    assert '"entrypoint": "demo_plugin.main"' in inspected.output

    enabled = runner.invoke(main, ["--config", str(cfg_path), "plugins", "enable", "demo_plugin"])
    assert enabled.exit_code == 0

    after_enable = runner.invoke(main, ["--config", str(cfg_path), "plugins", "list", "--json"])
    assert after_enable.exit_code == 0
    assert '"enabled": true' in after_enable.output

    disabled = runner.invoke(
        main, ["--config", str(cfg_path), "plugins", "disable", "demo_plugin"]
    )
    assert disabled.exit_code == 0
