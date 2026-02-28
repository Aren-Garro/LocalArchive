"""Backup command implementations."""

import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

from localarchive import cli as c
from localarchive.config import DEFAULT_CONFIG_PATH


def run_backup_restore(
    *,
    backup_path: Path | None,
    use_latest: bool,
    dry_run: bool,
    as_json: bool,
) -> None:
    config = c.get_config()
    config.ensure_dirs()
    cfg_path = c._runtime_ctx().get("config_path") or DEFAULT_CONFIG_PATH
    if bool(backup_path) == bool(use_latest):
        raise c.CLIError("Specify exactly one of --path or --latest.", exit_code=2)
    if use_latest:
        db = c.get_db(config)
        rows = db.list_backups(limit=1)
        db.close()
        if not rows:
            raise c.CLIError(
                "No tracked backups found. Create one with `backup create` first.", exit_code=2
            )
        selected = Path(str(rows[0].get("path", "")))
        if not selected.exists():
            raise c.CLIError(
                f"Newest tracked backup is missing on disk: {selected}. Run `backup list --prune-missing` and retry.",
                exit_code=2,
            )
        backup_path = selected
    if backup_path is None or not backup_path.exists():
        raise c.CLIError(f"Backup path does not exist: {backup_path}", exit_code=2)
    try:
        with zipfile.ZipFile(backup_path, "r") as zf:
            infos = {info.filename: info for info in zf.infolist()}
            members = set(infos)
            for name in members:
                posix = PurePosixPath(name)
                if posix.is_absolute() or ".." in posix.parts:
                    raise c.CLIError(f"Unsafe backup entry path: {name}", exit_code=2)
            has_db = "archive.db" in members
            has_config = "config.toml" in members
            create_count = 0
            overwrite_count = 0
            archive_entries: list[str] = []
            for name in members:
                if not name.startswith("archive_data/") or name.endswith("/"):
                    continue
                rel = Path(*PurePosixPath(name).parts[1:])
                archive_entries.append(name)
                dest = config.archive_dir / rel
                if dest.exists():
                    overwrite_count += 1
                else:
                    create_count += 1
            if len(archive_entries) > c.BACKUP_RESTORE_MAX_ARCHIVE_FILES:
                raise c.CLIError(
                    "Backup restore failed: archive contains too many files to restore safely.",
                    exit_code=4,
                )
            summary = {
                "backup": str(backup_path),
                "has_database": has_db,
                "has_config": has_config,
                "archive_files": len(archive_entries),
                "would_create": create_count,
                "would_overwrite": overwrite_count,
            }
    except c.CLIError:
        raise
    except Exception as exc:
        raise c.CLIError(f"Backup restore failed: {exc}", exit_code=4) from exc

    if dry_run:
        payload = {"dry_run": True, **summary}
        if as_json:
            c._emit_json(payload)
            return
        c.console.print("[bold]Restore Dry Run[/bold]")
        c.console.print(f"Backup: {summary['backup']}")
        c.console.print(f"Contains DB: {'yes' if summary['has_database'] else 'no'}")
        c.console.print(f"Contains config: {'yes' if summary['has_config'] else 'no'}")
        c.console.print(f"Archive files: {summary['archive_files']}")
        c.console.print(f"Would create: {summary['would_create']}")
        c.console.print(f"Would overwrite: {summary['would_overwrite']}")
        return

    staging_dir = Path(tempfile.mkdtemp(prefix="restore-", dir=str(config.runtime.tmp_dir)))
    rollback_dir = Path(tempfile.mkdtemp(prefix="rollback-", dir=str(config.runtime.tmp_dir)))
    moved_pairs: list[tuple[Path, Path]] = []
    created_paths: list[Path] = []
    verify_issue_count = 0
    limits = {"total": 0}
    try:
        with zipfile.ZipFile(backup_path, "r") as zf:
            infos = {info.filename: info for info in zf.infolist()}
            members = set(infos)
            if "archive.db" in members:
                c._copy_zip_member_limited(
                    zf,
                    infos["archive.db"],
                    staging_dir / "archive.db",
                    limits,
                )
            if "config.toml" in members:
                c._copy_zip_member_limited(
                    zf,
                    infos["config.toml"],
                    staging_dir / "config.toml",
                    limits,
                )
            for name in members:
                if not name.startswith("archive_data/") or name.endswith("/"):
                    continue
                rel = Path(*PurePosixPath(name).parts[1:])
                staged = staging_dir / "archive_data" / rel
                c._copy_zip_member_limited(
                    zf,
                    infos[name],
                    staged,
                    limits,
                )

        staged_db = staging_dir / "archive.db"
        if staged_db.exists():
            config.db_path.parent.mkdir(parents=True, exist_ok=True)
            if config.db_path.exists():
                backup_existing = rollback_dir / "archive.db.old"
                shutil.move(str(config.db_path), str(backup_existing))
                moved_pairs.append((backup_existing, config.db_path))
            shutil.move(str(staged_db), str(config.db_path))

        staged_cfg = staging_dir / "config.toml"
        if staged_cfg.exists():
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            if cfg_path.exists():
                backup_existing = rollback_dir / "config.toml.old"
                shutil.move(str(cfg_path), str(backup_existing))
                moved_pairs.append((backup_existing, cfg_path))
            shutil.move(str(staged_cfg), str(cfg_path))

        staged_archive_root = staging_dir / "archive_data"
        if staged_archive_root.exists():
            for staged in staged_archive_root.rglob("*"):
                if not staged.is_file():
                    continue
                rel = staged.relative_to(staged_archive_root)
                dest = config.archive_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    backup_existing = rollback_dir / "archive_data" / rel
                    backup_existing.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(dest), str(backup_existing))
                    moved_pairs.append((backup_existing, dest))
                else:
                    created_paths.append(dest)
                shutil.move(str(staged), str(dest))

        if config.reliability.auto_verify_after_restore:
            verify_db = c.get_db(config)
            verify_report = verify_db.audit_verify(repair=False, full_check=False)
            verify_db.close()
            verify_issue_count = len(verify_report.get("issues") or [])
            if verify_report["issues"]:
                raise c.CLIError(
                    f"Restore completed but verify found {len(verify_report['issues'])} issue(s).",
                    exit_code=4,
                )
        payload = {"restored": True, **summary, "verify_issues": verify_issue_count}
        if as_json:
            c._emit_json(payload)
        else:
            c.console.print(f"[green]Backup restored from:[/green] {backup_path}")
    except Exception as exc:
        for created in created_paths:
            try:
                if created.exists():
                    created.unlink()
            except Exception:
                pass
        for src, dest in reversed(moved_pairs):
            try:
                if src.exists():
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if dest.exists() and dest.is_file():
                        dest.unlink()
                    shutil.move(str(src), str(dest))
            except Exception:
                pass
        if isinstance(exc, c.CLIError):
            raise
        raise c.CLIError(f"Backup restore failed: {exc}", exit_code=4) from exc
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)
        shutil.rmtree(rollback_dir, ignore_errors=True)

