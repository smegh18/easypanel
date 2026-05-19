from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from download_full_ranges import _download_window_with_retries, _import_tick_vault, _parse_utc_datetime
from export_monthly_csv import _build_export_frame, _month_start, _next_month


PIPET_SCALE_OVERRIDES: dict[str, float] = {
    "USATECHIDXUSD": 0.1,
}


def _resolve_symbol_and_start(raw_symbol: str, explicit_start: datetime | None) -> tuple[str, datetime]:
    symbol = raw_symbol.strip().upper()
    if symbol in {"NAS100", "NASDAQ100", "US100"}:
        resolved = "USATECHIDXUSD"
        default_start = datetime(2013, 1, 1, 5, tzinfo=UTC)
    elif symbol in {"XAUUSD", "EURUSD"}:
        resolved = symbol
        default_start = datetime(2006, 1, 1, tzinfo=UTC)
    else:
        resolved = symbol
        default_start = datetime(2006, 1, 1, tzinfo=UTC)

    return resolved, explicit_start or default_start


def _extract_drive_folder_id(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("Drive folder value cannot be empty")

    if "drive.google.com" not in raw:
        return raw

    patterns = [
        r"/folders/([a-zA-Z0-9_-]+)",
        r"[?&]id=([a-zA-Z0-9_-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            return match.group(1)

    raise ValueError(f"Could not extract Google Drive folder ID from: {value}")


def _configure_rclone_remote(remote: str, folder_value: str | None) -> str:
    remote_name = remote[:-1] if remote.endswith(":") else remote
    remote_env_name = remote_name.upper()

    os.environ.setdefault(f"RCLONE_CONFIG_{remote_env_name}_TYPE", "drive")
    os.environ.setdefault(f"RCLONE_CONFIG_{remote_env_name}_SCOPE", "drive")

    if folder_value:
        folder_id = _extract_drive_folder_id(folder_value)
        os.environ[f"RCLONE_CONFIG_{remote_env_name}_ROOT_FOLDER_ID"] = folder_id

    return f"{remote_name}:"


def _upload_file_to_drive(local_file: Path, remote: str, remote_subdir: str | None, remote_name: str | None = None) -> None:
    destination = remote
    clean_subdir = (remote_subdir or "").strip("/")
    filename = remote_name or local_file.name
    if clean_subdir:
        destination = f"{destination}{clean_subdir}/{filename}"
    else:
        destination = f"{destination}{filename}"

    subprocess.run(
        [
            "rclone",
            "copyto",
            str(local_file),
            destination,
            "--retries",
            "3",
            "--low-level-retries",
            "10",
            "--drive-use-trash=false",
        ],
        check=True,
    )


def _upload_raw_month_to_drive(
    symbol: str,
    month_start: datetime,
    month_end: datetime,
    tick_vault_dir: Path,
    remote: str,
    raw_subdir: str | None,
) -> int:
    os.environ["TICK_VAULT_BASE_DIRECTORY"] = str(tick_vault_dir)

    from tick_vault.metadata import MetadataDB  # noqa: PLC0415

    uploads = 0
    downloads_base = tick_vault_dir / "downloads"
    symbol_base = downloads_base / symbol
    clean_subdir = (raw_subdir or "").strip("/")

    with MetadataDB() as db:
        chunks = db.get_available_chunks(symbol, month_start, month_end)

    for chunk in chunks:
        local_path = chunk.path(base=downloads_base)
        if not local_path.exists():
            continue

        relative_path = local_path.relative_to(symbol_base).as_posix()
        remote_path = f"{clean_subdir}/{relative_path}" if clean_subdir else relative_path
        _upload_file_to_drive(local_path, remote, None, remote_name=remote_path)
        uploads += 1

    return uploads


def _purge_raw_month(symbol: str, month_start: datetime, month_end: datetime, tick_vault_dir: Path) -> None:
    os.environ["TICK_VAULT_BASE_DIRECTORY"] = str(tick_vault_dir)

    from tick_vault.metadata import MetadataDB  # noqa: PLC0415

    downloads_base = tick_vault_dir / "downloads"

    with MetadataDB() as db:
        chunks = db.get_available_chunks(symbol, month_start, month_end)
        for chunk in chunks:
            path = chunk.path(base=downloads_base)
            if path.exists():
                path.unlink()

            parent = path.parent
            while parent != downloads_base and parent.exists():
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent

        db._ensure_table_exists(symbol)
        table_name = db._get_table_name(symbol)
        db.conn.execute(
            f"DELETE FROM {table_name} WHERE timestamp >= ? AND timestamp < ?",
            (int(month_start.timestamp()), int(month_end.timestamp())),
        )
        db.conn.commit()


def _write_summary_row(summary_path: Path, symbol: str, month_start: datetime, rows: int, csv_path: Path) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not summary_path.exists()
    with summary_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["symbol", "month", "rows", "csv_path"])
        writer.writerow([symbol, f"{month_start.year:04d}-{month_start.month:02d}", rows, str(csv_path)])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Automate month-by-month Dukascopy download -> CSV -> Google Drive upload -> raw cleanup."
    )
    parser.add_argument("--symbol", default="XAUUSD", help="Symbol to process, e.g. XAUUSD or NAS100.")
    parser.add_argument(
        "--start",
        default=None,
        help="Optional UTC start datetime. If omitted, uses the default for the symbol.",
    )
    parser.add_argument(
        "--end",
        default=datetime.now(tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        help="UTC end datetime (default: now).",
    )
    parser.add_argument(
        "--tick-vault-dir",
        default=str(Path("data") / "tick_vault_data"),
        help="TickVault base directory (default: data/tick_vault_data).",
    )
    parser.add_argument(
        "--csv-dir",
        default=str(Path("data") / "dukascopy_monthly_csv"),
        help="Directory for monthly CSV output (default: data/dukascopy_monthly_csv).",
    )
    parser.add_argument("--workers", type=int, default=2, help="Downloader workers (default: 2).")
    parser.add_argument("--window-retries", type=int, default=12, help="Retries per month on download failure.")
    parser.add_argument("--retry-sleep-seconds", type=int, default=180, help="Sleep between failed retries.")
    parser.add_argument("--show-progress", action="store_true", help="Show TickVault read progress for each month.")
    parser.add_argument("--overwrite-csv", action="store_true", help="Rebuild existing monthly CSV files.")
    parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="Keep raw TickVault month files after CSV creation. By default they are deleted.",
    )
    parser.add_argument(
        "--drive-remote",
        default="gdrive:",
        help="Rclone remote name/path, default gdrive:.",
    )
    parser.add_argument(
        "--drive-folder",
        default=None,
        help="Google Drive folder URL or folder ID. Sets the rclone root_folder_id for the remote.",
    )
    parser.add_argument(
        "--drive-subdir",
        default=None,
        help="Legacy common subdirectory inside the Drive folder. If raw/csv subdirs are not set, both default from this.",
    )
    parser.add_argument(
        "--drive-raw-subdir",
        default=None,
        help="Drive subdirectory for raw .bi5 files. Defaults to raw/<SYMBOL>.",
    )
    parser.add_argument(
        "--drive-csv-subdir",
        default=None,
        help="Drive subdirectory for monthly CSV files. Defaults to csv/<SYMBOL>.",
    )
    args = parser.parse_args()

    explicit_start = _parse_utc_datetime(args.start) if args.start else None
    end = _parse_utc_datetime(args.end)
    tick_vault_dir = Path(args.tick_vault_dir)
    csv_root = Path(args.csv_dir)

    symbol, start = _resolve_symbol_and_start(args.symbol, explicit_start)
    drive_remote = _configure_rclone_remote(args.drive_remote, args.drive_folder)
    base_drive_subdir = (args.drive_subdir or "").strip("/")
    if args.drive_raw_subdir:
        drive_raw_subdir = args.drive_raw_subdir.strip("/")
    elif base_drive_subdir:
        drive_raw_subdir = f"{base_drive_subdir}/raw/{symbol}"
    else:
        drive_raw_subdir = f"raw/{symbol}"

    if args.drive_csv_subdir:
        drive_csv_subdir = args.drive_csv_subdir.strip("/")
    elif base_drive_subdir:
        drive_csv_subdir = f"{base_drive_subdir}/csv"
    else:
        drive_csv_subdir = f"csv/{symbol}"

    download_range = _import_tick_vault(tick_vault_dir, workers=int(args.workers))

    os.environ["TICK_VAULT_BASE_DIRECTORY"] = str(tick_vault_dir)
    from tick_vault import read_tick_data  # noqa: PLC0415
    from tick_vault.metadata import MetadataDB  # noqa: PLC0415

    out_dir = csv_root / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / "SUMMARY.csv"

    current = _month_start(start)
    end_month = _month_start(end)

    while current <= end_month:
        month_end = _next_month(current)
        csv_path = out_dir / f"{symbol}_Ticks_{current.year:04d}-{current.month:02d}.csv"

        if not csv_path.exists() or args.overwrite_csv:
            print(f"Processing {symbol} {current.year:04d}-{current.month:02d}")
            _download_window_with_retries(
                download_range=download_range,
                symbol=symbol,
                start=current,
                end=month_end,
                max_attempts=int(args.window_retries),
                retry_sleep_seconds=int(args.retry_sleep_seconds),
            )

            with MetadataDB() as db:
                available = db.get_available_chunks(symbol, current, month_end)

            if not available:
                print(f"No data downloaded for {symbol} {current.year:04d}-{current.month:02d}; skipping.")
                current = month_end
                continue

            df = read_tick_data(
                symbol=symbol,
                start=current,
                end=month_end,
                pipet_scale=PIPET_SCALE_OVERRIDES.get(symbol),
                strict=False,
                show_progress=bool(args.show_progress),
            )
            export_df = _build_export_frame(df)
            export_df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
            _write_summary_row(summary_path, symbol, current, int(export_df.shape[0]), csv_path)
            print(f"Wrote {csv_path} ({export_df.shape[0]} rows)")
        else:
            print(f"Using existing CSV {csv_path}")

        raw_uploaded = _upload_raw_month_to_drive(
            symbol=symbol,
            month_start=current,
            month_end=month_end,
            tick_vault_dir=tick_vault_dir,
            remote=drive_remote,
            raw_subdir=drive_raw_subdir,
        )
        print(f"Uploaded {raw_uploaded} raw files for {symbol} {current.year:04d}-{current.month:02d} to {drive_remote}{drive_raw_subdir}/")

        _upload_file_to_drive(csv_path, drive_remote, drive_csv_subdir)
        print(f"Uploaded {csv_path.name} to {drive_remote}{drive_csv_subdir}/")

        if not args.keep_raw:
            _purge_raw_month(symbol, current, month_end, tick_vault_dir)
            print(f"Deleted raw data for {symbol} {current.year:04d}-{current.month:02d}")

        current = month_end

    print(f"Done. Local CSVs: {out_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
