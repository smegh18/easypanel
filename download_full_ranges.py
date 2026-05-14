from __future__ import annotations

import argparse
import asyncio
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


def _parse_utc_datetime(value: str) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1]

    if len(raw) == 10:
        return datetime.fromisoformat(raw).replace(tzinfo=UTC)

    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _import_tick_vault(base_directory: Path, workers: int):
    """
    TickVault imports CONFIG by value in some modules, so runtime reload_config()
    is not reliable. Use env vars before importing TickVault.
    """
    os.environ["TICK_VAULT_BASE_DIRECTORY"] = str(base_directory)
    os.environ["TICK_VAULT_WORKER_PER_PROXY"] = str(workers)
    os.environ.setdefault("TICK_VAULT_WORKER_QUEUE_TIMEOUT", "1800")
    os.environ.setdefault("TICK_VAULT_METADATA_UPDATE_BATCH_TIMEOUT", "5")
    os.environ.setdefault("TICK_VAULT_METADATA_UPDATE_BATCH_SIZE", "200")
    os.environ.setdefault("TICK_VAULT_FETCH_MAX_RETRY_ATTEMPTS", "8")
    os.environ.setdefault("TICK_VAULT_FETCH_BASE_RETRY_DELAY", "2")

    from tick_vault.chunk import TickChunk  # noqa: PLC0415
    from tick_vault.config import CONFIG  # noqa: PLC0415
    from tick_vault.download_worker import download_worker  # noqa: PLC0415
    from tick_vault.logger import logger  # noqa: PLC0415
    from tick_vault.metadata import MetadataDB  # noqa: PLC0415
    from tick_vault.metadata_worker import metadata_worker  # noqa: PLC0415
    from tqdm.asyncio import tqdm  # noqa: PLC0415

    async def resilient_download_range(
        symbol: str,
        start: datetime,
        end: datetime,
        proxies: list[str] | None = None,
    ) -> None:
        logger.info(f"Starting download for {symbol} from {start.date()} to {end.date()}")

        worker_proxies = [None] if not proxies else proxies

        with MetadataDB() as db:
            chunks_to_download = db.find_not_attempted_chunks(symbol, start, end)

        if not chunks_to_download:
            logger.info(f"All data for {symbol} from {start.date()} to {end.date()} already downloaded")
            return

        total_chunks = len(chunks_to_download)
        logger.info(f"Found {total_chunks} chunks to download for {symbol}")

        downloader_input_queue: asyncio.Queue[TickChunk | None] = asyncio.Queue()
        downloader_output_queue: asyncio.Queue[TickChunk] = asyncio.Queue()
        metadata_queue: asyncio.Queue[TickChunk | None] = asyncio.Queue()

        max_workers = len(worker_proxies) * CONFIG.worker_per_proxy
        actual_workers = min(max_workers, total_chunks)
        logger.debug(f"Using {actual_workers} workers across {len(worker_proxies)} proxies")

        metadata_task = asyncio.create_task(metadata_worker(metadata_queue))
        download_tasks = []
        worker_index = 0

        logger.debug("Starting download workers and metadata worker")
        for _ in range(CONFIG.worker_per_proxy):
            for proxy in worker_proxies:
                if worker_index >= actual_workers:
                    break
                task = asyncio.create_task(download_worker(proxy, downloader_input_queue, downloader_output_queue))
                download_tasks.append(task)
                worker_index += 1
            if worker_index >= actual_workers:
                break

        all_tasks = download_tasks + [metadata_task]
        chunks_remaining = list(chunks_to_download)
        for _ in range(actual_workers):
            if chunks_remaining:
                await downloader_input_queue.put(chunks_remaining.pop(0))

        pbar = tqdm(
            total=total_chunks,
            desc=f"Downloading {symbol}",
            unit="chunk",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            colour="green",
        )

        completed = 0
        error_occurred = False

        try:
            while completed < total_chunks:
                for task in all_tasks:
                    if task.done():
                        task.result()

                try:
                    chunk = await asyncio.wait_for(downloader_output_queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue

                await metadata_queue.put(chunk)
                completed += 1
                pbar.update(1)

                if chunks_remaining:
                    await downloader_input_queue.put(chunks_remaining.pop(0))

            pbar.close()
            logger.info(f"Successfully downloaded {total_chunks} chunks for {symbol}")

        except Exception:
            error_occurred = True
            pbar.close()
            raise

        finally:
            logger.debug("Sending stop signals to workers")
            for _ in range(actual_workers):
                await downloader_input_queue.put(None)
            await metadata_queue.put(None)

            logger.debug("Waiting for workers to finish")
            try:
                if error_occurred:
                    for task in download_tasks:
                        task.cancel()
                    metadata_task.cancel()
                    await asyncio.gather(*download_tasks, metadata_task, return_exceptions=True)
                else:
                    await asyncio.gather(*download_tasks, metadata_task)
            except asyncio.CancelledError:
                pass

            logger.debug("Workers cancelled")

    return resilient_download_range


@dataclass(frozen=True)
class Job:
    symbol: str
    start: datetime
    end: datetime


def _download_window_with_retries(
    download_range,
    symbol: str,
    start: datetime,
    end: datetime,
    max_attempts: int,
    retry_sleep_seconds: int,
) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            asyncio.run(download_range(symbol=symbol, start=start, end=end))
            return
        except Exception as exc:
            if attempt == max_attempts:
                raise

            print(
                f"{symbol} window {start.isoformat()} -> {end.isoformat()} failed "
                f"(attempt {attempt}/{max_attempts}) with: {exc}"
            )
            print(f"Sleeping {retry_sleep_seconds}s before retrying the same window...")
            time.sleep(retry_sleep_seconds)


def _iter_windows(start: datetime, end: datetime, window_days: int):
    if end <= start:
        return
    step = timedelta(days=window_days)
    current = start
    while current < end:
        nxt = min(end, current + step)
        yield current, nxt
        current = nxt


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Resume-capable full-range Dukascopy tick downloads using TickVault (chunked to avoid SQLite limits)."
    )
    parser.add_argument(
        "--tick-vault-dir",
        default=str(Path("data") / "tick_vault_data"),
        help="TickVault base directory (default: data/tick_vault_data).",
    )
    parser.add_argument(
        "--end",
        default=datetime.now(tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        help="UTC end datetime (default: now).",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Download in N-day windows to avoid SQLite placeholder limits (default: 30).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Concurrent workers per proxy (default: 5; higher may trigger rate limits).",
    )
    parser.add_argument(
        "--window-retries",
        type=int,
        default=8,
        help="How many times to retry a failing time window before stopping (default: 8).",
    )
    parser.add_argument(
        "--retry-sleep-seconds",
        type=int,
        default=120,
        help="Sleep between failed window retries to let Dukascopy recover (default: 120).",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=["XAUUSD", "EURUSD", "NAS100"],
        help="Symbols to download. NAS100 is mapped to Dukascopy instrument USATECHIDXUSD.",
    )
    args = parser.parse_args()

    base_dir = Path(args.tick_vault_dir)
    end = _parse_utc_datetime(args.end)
    window_days = int(args.window_days)
    workers = int(args.workers)
    window_retries = int(args.window_retries)
    retry_sleep_seconds = int(args.retry_sleep_seconds)

    download_range = _import_tick_vault(base_dir, workers=workers)

    def resolve_symbol(sym: str) -> str:
        s = sym.strip().upper()
        if s in {"NAS100", "NASDAQ100", "US100"}:
            return "USATECHIDXUSD"
        return s

    jobs: list[Job] = []
    for raw in args.symbols:
        symbol = resolve_symbol(raw)
        if symbol in {"EURUSD", "XAUUSD"}:
            start = datetime(2006, 1, 1, tzinfo=UTC)
        elif symbol == "USATECHIDXUSD":
            start = datetime(2013, 1, 1, 5, tzinfo=UTC)
        else:
            raise SystemExit(f"Unsupported symbol: {raw} (resolved to {symbol})")

        jobs.append(Job(symbol=symbol, start=start, end=end))

    for job in jobs:
        windows = list(_iter_windows(job.start, job.end, window_days=window_days))
        print(f"{job.symbol}: {job.start.isoformat()} -> {job.end.isoformat()} ({len(windows)} windows)")
        for w_start, w_end in windows:
            _download_window_with_retries(
                download_range=download_range,
                symbol=job.symbol,
                start=w_start,
                end=w_end,
                max_attempts=window_retries,
                retry_sleep_seconds=retry_sleep_seconds,
            )

    print(f"Done. TickVault store: {base_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
