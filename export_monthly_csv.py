from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


def _infer_pipet_scale(symbol: str) -> float | None:
    upper_symbol = symbol.upper()

    explicit: dict[str, float] = {
        "USATECHIDXUSD": 0.1,
        "XAUUSD": 0.001,
        "XAGUSD": 0.001,
        "BTCUSD": 0.1,
        "ETHUSD": 0.1,
    }
    if upper_symbol in explicit:
        return explicit[upper_symbol]

    # Generic FX handling: most non-JPY pairs use 1e-5 pipet precision,
    # while JPY-quoted pairs use 1e-3.
    if len(upper_symbol) == 6 and upper_symbol.isalpha():
        return 0.001 if upper_symbol.endswith("JPY") else 0.00001

    return None


def _month_start(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(dt: datetime) -> datetime:
    year = dt.year + (1 if dt.month == 12 else 0)
    month = 1 if dt.month == 12 else dt.month + 1
    return dt.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _import_tick_vault(base_directory: Path):
    os.environ["TICK_VAULT_BASE_DIRECTORY"] = str(base_directory)

    from tick_vault import read_tick_data  # noqa: PLC0415
    from tick_vault.metadata import MetadataDB  # noqa: PLC0415

    return read_tick_data, MetadataDB


@dataclass(frozen=True)
class MonthExport:
    month_start: datetime
    csv_path: Path
    rows: int


def _build_export_frame(df: pd.DataFrame) -> pd.DataFrame:
    expected = {"time", "bid", "ask", "bid_volume", "ask_volume"}
    missing = sorted(expected - set(df.columns))
    if missing:
        raise RuntimeError(f"Missing expected TickVault columns: {missing}")

    ts = pd.to_datetime(df["time"], utc=True, errors="raise")
    return df.assign(
        DateTime=ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str.slice(0, 23) + "Z",
        Bid=df["bid"],
        Ask=df["ask"],
        BidVolume=df["bid_volume"],
        AskVolume=df["ask_volume"],
    )[["DateTime", "Bid", "Ask", "BidVolume", "AskVolume"]].sort_values("DateTime")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export already-downloaded TickVault data into monthly CSV files."
    )
    parser.add_argument("--symbol", default="XAUUSD", help="Symbol to export (default: XAUUSD).")
    parser.add_argument(
        "--tick-vault-dir",
        default=str(Path("data") / "tick_vault_data"),
        help="TickVault base directory (default: data/tick_vault_data).",
    )
    parser.add_argument(
        "--out-dir",
        default=str(Path("data") / "dukascopy_monthly_csv"),
        help="Directory for monthly CSV output (default: data/dukascopy_monthly_csv).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite monthly CSVs that already exist.",
    )
    parser.add_argument(
        "--show-progress",
        action="store_true",
        help="Show TickVault read progress for each month.",
    )
    args = parser.parse_args()

    symbol = args.symbol.strip().upper()
    tick_vault_dir = Path(args.tick_vault_dir)
    out_dir = Path(args.out_dir) / symbol
    out_dir.mkdir(parents=True, exist_ok=True)

    read_tick_data, MetadataDB = _import_tick_vault(tick_vault_dir)

    with MetadataDB() as db:
        first_chunk = db.first_chunk(symbol)
        last_chunk = db.last_chunk(symbol)

    if first_chunk is None or last_chunk is None:
        raise SystemExit(f"No downloaded data found for {symbol} in {tick_vault_dir.resolve()}")

    current = _month_start(first_chunk.time)
    end_month = _month_start(last_chunk.time)
    exports: list[MonthExport] = []

    while current <= end_month:
        month_end = _next_month(current)
        output_path = out_dir / f"{symbol}_Ticks_{current.year:04d}-{current.month:02d}.csv"

        if output_path.exists() and not args.overwrite:
            print(f"Skipping existing CSV: {output_path}")
            current = month_end
            continue

        with MetadataDB() as db:
            available_chunks = db.get_available_chunks(symbol, current, month_end)

        if not available_chunks:
            print(f"No downloaded ticks for {symbol} in {current.year:04d}-{current.month:02d}; skipping.")
            current = month_end
            continue

        df = read_tick_data(
            symbol=symbol,
            start=current,
            end=month_end,
            pipet_scale=_infer_pipet_scale(symbol),
            strict=False,
            show_progress=bool(args.show_progress),
        )
        export_df = _build_export_frame(df)
        export_df.to_csv(output_path, index=False, quoting=csv.QUOTE_MINIMAL)

        exports.append(
            MonthExport(
                month_start=current,
                csv_path=output_path,
                rows=int(export_df.shape[0]),
            )
        )
        print(f"Wrote {output_path} ({export_df.shape[0]} rows)")
        current = month_end

    summary_path = out_dir / "SUMMARY.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "month", "rows", "csv_path"])
        for export in exports:
            writer.writerow(
                [
                    symbol,
                    f"{export.month_start.year:04d}-{export.month_start.month:02d}",
                    export.rows,
                    str(export.csv_path),
                ]
            )

    print(f"Done. Monthly CSVs: {out_dir.resolve()}")
    print(f"Summary: {summary_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
