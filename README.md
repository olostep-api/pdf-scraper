# Olostep PDF Scraper

Scrape PDFs via the Olostep API and export structured content plus per-format files.

## Features

- Single PDF scrape via `/v1/scrapes`
- Batch scraping via `/v1/batches` and per-item content via `/v1/retrieve`
- Writes one aggregate JSON file plus extracted content files under `output/`
- Supports formats like `markdown`, `text`, `html`, `json` (batch retrieve formats are normalized to `html|markdown|json`)

## Requirements

- Python 3.10+
- `pip`

## Quickstart

```bash
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`:

```bash
OLOSTEP_API_KEY="YOUR_KEY"
```

## Environment Variables

- `OLOSTEP_API_KEY` (required): Olostep API key used for authentication.

## Usage

### Single PDF

```bash
python main.py --url "https://example.com/file.pdf"
```

Specify formats and output name:

```bash
python main.py --url "https://example.com/file.pdf" --formats markdown,text --out single.json
```

### Batch PDFs

Create `urls.txt` (one URL per line, `#` comments supported):

```text
https://site.com/a.pdf
https://site.com/b.pdf
```

Run batch:

```bash
python main.py --urls-file urls.txt --out batch.json
```

### Repeatable `--url`

```bash
python main.py --url "https://site.com/a.pdf" --url "https://site.com/b.pdf"
```

## CLI Reference

- `--url`: PDF URL (repeatable)
- `--urls-file`: text file with 1 URL per line
- `--out`: output JSON filename (written under `output/`)
- `--formats`: comma-separated formats (example: `markdown,text,html`)
  - Single mode: passed through to `/v1/scrapes`
  - Batch mode: retrieve formats are filtered to `html|markdown|json`
    - If none match, `formats` is omitted when calling `/v1/retrieve` (Olostep returns all formats)
- `--poll-seconds`: batch polling interval in seconds
- `--items-limit`: batch item page size

## Outputs

- Aggregate JSON:
  - If `--out` is set: `output/<out>`
  - If `--out` is omitted and single mode: `output/single_{HH-MM}_{YYYY-MM-DD}.json`
  - If `--out` is omitted and batch mode: `output/batch{count}_{HH-MM}_{YYYY-MM-DD}.json`
- Extracted content files:
  - Written under `output/` as `<custom_id>.<ext>` when that format is present (example: `pdf-1.md`, `0.md`)
  - In single mode `custom_id` is `pdf-1`
  - In batch mode `custom_id` defaults to the item index (`0`, `1`, `2`, ...)

## Configuration Defaults (in code)

Defaults live in `config/config.py` (only `OLOSTEP_API_KEY` comes from `.env`):

- `OLOSTEP_API_BASE`: `https://api.olostep.com`
- `OUTPUT_DIR`: `output`
- `DEFAULT_FORMATS`: `markdown,text`
- `DEFAULT_OUT_FILE`: `output.json`
- `DEFAULT_POLL_SECONDS`: `5`
- `DEFAULT_ITEMS_LIMIT`: `50`
- `LOG_LEVEL`: `INFO` (`DEBUG`, `INFO`, `WARNING`, `ERROR`)

To change these values, edit `config/config.py` (env overrides are not supported right now).

## Security Notes

- Do not commit `.env` or API tokens.
- If a token was exposed in git history, rotate it immediately.

## License

TBD (no license file added yet).

## Behavior Notes

- Single mode uses the `/v1/scrapes` response directly and does not call `/v1/retrieve`.
- Batch mode calls `/v1/retrieve` for each completed item.
