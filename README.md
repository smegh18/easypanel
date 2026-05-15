# Easypanel Dukascopy Upload

This folder is a standalone repo for running the Dukascopy downloader, monthly CSV exporter, and Google Drive upload pipeline on an Easypanel server.

## Files

- `download_full_ranges.py` - resume-capable historical tick downloader
- `export_monthly_csv.py` - converts downloaded TickVault data into monthly CSV files
- `pipeline_to_drive.py` - month-by-month download -> CSV -> Google Drive upload -> raw cleanup
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

## Automated Google Drive pipeline

The pipeline stores monthly CSV files in a Google Drive folder and deletes each month's raw TickVault data after CSV creation.

### Easypanel environment variables

Set these in the service:

```text
RCLONE_CONFIG_GDRIVE_TYPE=drive
RCLONE_CONFIG_GDRIVE_SCOPE=drive
RCLONE_CONFIG_GDRIVE_TOKEN=<paste your rclone Google Drive token JSON here>
```

If you do not want to pass the folder URL as a command argument, you can also set:

```text
RCLONE_CONFIG_GDRIVE_ROOT_FOLDER_ID=<your_google_drive_folder_id>
```

### Pipeline command

```bash
python /app/pipeline_to_drive.py --symbol XAUUSD --tick-vault-dir /app/data/tick_vault_data --csv-dir /app/data/dukascopy_monthly_csv --drive-remote gdrive: --drive-folder https://drive.google.com/drive/folders/YOUR_FOLDER_ID --drive-subdir XAUUSD --workers 2 --window-retries 12 --retry-sleep-seconds 180 --show-progress
```

### What the pipeline does

For each month:

1. Downloads that month from Dukascopy into `/app/data/tick_vault_data`
2. Exports a monthly CSV into `/app/data/dukascopy_monthly_csv/<SYMBOL>`
3. Uploads the CSV to the configured Google Drive folder
4. Deletes that month's raw `.bi5` files and metadata rows

The local monthly CSV files remain in place. The raw tick cache is removed month by month unless you add `--keep-raw`.

## Output locations

- Raw downloaded data: `/app/data/tick_vault_data`
- Monthly CSV files: `/app/data/dukascopy_monthly_csv/XAUUSD`
