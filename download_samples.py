from __future__ import annotations

import argparse
import asyncio
import csv
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


def _import_tick_vault() -> tuple[object, object]:
    os.environ.setdefault("TICK_VAULT_BASE_DIRECTORY", str(Path("data") / "tick_vault_data"))
    os.environ.setdefault("TICK_VAULT_WORKER_PER_PROXY", "5")

    from tick_vault import download_range, read_tick_data  # noqa: PLC0415

    return download_range, read_tick_data


def _parse_utc_datetime(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1]

    if len(raw) == 10:
        dt = datetime.fromisoformat(raw)
        return dt.replace(tzinfo=UTC)

    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt


def _format_iso_ms_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class ExportResult:
    symbol: str
    csv_path: Path
    rows: int
    start_utc: datetime
    end_utc: datetime


def _export_symbol_csv(symbol: str, start: datetime, end: datetime, out_dir: Path) -> ExportResult:
    download_range, read_tick_data = _import_tick_vault()
    asyncio.run(download_range(symbol=symbol, start=start, end=end))

    df = read_tick_data(
        symbol=symbol,
        start=start,
        end=end,
        strict=False,
        show_progress=True,
    )

    expected = {"time", "bid", "ask", "bid_volume", "ask_volume"}
    missing = sorted(expected - set(df.columns))
    if missing:
        raise RuntimeError(f"{symbol}: missing expected columns from TickVault: {missing}")

    ts = pd.to_datetime(df["time"], utc=True, errors="raise")
    df = df.assign(
        DateTime=ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str.slice(0, 23) + "Z",
        Bid=df["bid"],
        Ask=df["ask"],
        BidVolume=df["bid_volume"],
        AskVolume=df["ask_volume"],
    )[["DateTime", "Bid", "Ask", "BidVolume", "AskVolume"]].sort_values("DateTime")

    out_dir.mkdir(parents=True, exist_ok=True)
    start_tag = _format_iso_ms_z(start).split("T", 1)[0]
    end_tag = _format_iso_ms_z(end).split("T", 1)[0]
    out_path = out_dir / f"{symbol}_Ticks_{start_tag}_{end_tag}.csv"
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    return ExportResult(
        symbol=symbol,
        csv_path=out_path,
        rows=int(df.shape[0]),
        start_utc=start,
        end_utc=end,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Dukascopy tick samples via TickVault and export CSVs.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["EURUSD", "GBPUSD", "XAUUSD"],
        help="Symbols to download (e.g. EURUSD GBPUSD XAUUSD).",
    )
    parser.add_argument(
        "--start",
        default="2026-05-08T00:00:00Z",
        help="UTC start datetime (default: 2026-05-08T00:00:00Z).",
    )
    parser.add_argument(
        "--end",
        default="2026-05-08T02:00:00Z",
        help="UTC end datetime (default: 2026-05-08T02:00:00Z).",
    )
    parser.add_argument(
        "--tick-vault-dir",
        default=str(Path("data") / "tick_vault_data"),
        help="Directory where TickVault stores downloads/metadata (default: data/tick_vault_data).",
    )
    parser.add_argument(
        "--out-dir",
        default=str(Path("data") / "dukascopy_tick_samples"),
        help="Directory to write exported CSVs (default: data/dukascopy_tick_samples).",
    )

    args = parser.parse_args()

    start = _parse_utc_datetime(args.start)
    end = _parse_utc_datetime(args.end)
    if end <= start:
        raise SystemExit("--end must be after --start")

    tick_vault_dir = Path(args.tick_vault_dir)
    out_dir = Path(args.out_dir)
    os.environ["TICK_VAULT_BASE_DIRECTORY"] = str(tick_vault_dir)
    os.environ.setdefault("TICK_VAULT_WORKER_PER_PROXY", "5")

    results: list[ExportResult] = []
    for symbol in args.symbols:
        results.append(_export_symbol_csv(symbol.upper(), start, end, out_dir))

    summary_path = out_dir / "SUMMARY.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "rows", "csv_path", "start_utc", "end_utc"])
        for result in results:
            writer.writerow(
                [
                    result.symbol,
                    result.rows,
                    str(result.csv_path),
                    _format_iso_ms_z(result.start_utc),
                    _format_iso_ms_z(result.end_utc),
                ]
            )

    print(f"Wrote {len(results)} CSV(s) under: {out_dir.resolve()}")
    print(f"Summary: {summary_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
