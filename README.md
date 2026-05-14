# Easypanel Dukascopy Upload

This folder is a standalone repo for running the Dukascopy downloader and monthly CSV exporter on an Easypanel server.

## Files

- `download_full_ranges.py` - resume-capable historical tick downloader
- `export_monthly_csv.py` - converts downloaded TickVault data into monthly CSV files
- `download_samples.py` - small sample downloader/exporter for quick checks
- `Dockerfile` - container build for Easypanel
- `requirements.txt` - Python dependencies

## Easypanel setup

Create a new GitHub repo using the contents of this folder as the repo root.

In Easypanel:

1. Create a new project.
2. Add a new `App` service.
3. Connect the GitHub repo.
4. Build method: `Dockerfile`
5. Dockerfile path: `Dockerfile`
6. Add one persistent volume mounted at `/app/data`
7. Do not configure domains or ports.

## Download command

Use this as the service command:

```bash
python /app/download_full_ranges.py --symbols XAUUSD --tick-vault-dir /app/data/tick_vault_data --window-days 14 --workers 2 --window-retries 12 --retry-sleep-seconds 180
```

## Monthly CSV export command

After data is downloaded, change the service command to:

```bash
python /app/export_monthly_csv.py --symbol XAUUSD --tick-vault-dir /app/data/tick_vault_data --out-dir /app/data/dukascopy_monthly_csv --show-progress
```

## Output locations

- Raw downloaded data: `/app/data/tick_vault_data`
- Monthly CSV files: `/app/data/dukascopy_monthly_csv/XAUUSD`

