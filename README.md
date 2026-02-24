# Olostep PDF Scraper

Extract structured content from PDF links with a single command.

This tool is designed for users who need to scrape one or many PDFs and keep results in a predictable JSON output for downstream use.

- For one PDF URL, it uses `/v1/scrapes`.
- For multiple PDF URLs, it uses `/v1/batches` and `/v1/retrieve`.
- It stores outputs in the `output/` folder.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`:

```bash
OLOSTEP_API_KEY="YOUR_KEY"
```

## Environment variables

- `OLOSTEP_API_KEY` or `OLOSTEP_API_TOKEN`: Olostep API token (required)
- `OLOSTEP_API_BASE`: default `https://api.olostep.com`
- `OUTPUT_DIR`: default `output`
- `DEFAULT_FORMATS`: default `markdown,text`
- `DEFAULT_OUT_FILE`: default `output.json`
- `DEFAULT_POLL_SECONDS`: default `5`
- `DEFAULT_ITEMS_LIMIT`: default `50`
- `LOG_LEVEL`: default `INFO` (`DEBUG`, `INFO`, `WARNING`, `ERROR`)

## Usage

### Single PDF

```bash
python main.py --url "https://example.com/file.pdf"
```

Optional:

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

## CLI options

- `--url`: PDF URL, repeatable
- `--urls-file`: text file containing URLs
- `--out`: output JSON filename (saved under `output/`)
- `--formats`: comma-separated formats (example: `markdown,text,html`)
- `--poll-seconds`: batch polling interval
- `--items-limit`: batch item page size

## Output

- JSON output:
  - If `--out` is set: `output/<out>`
  - If `--out` is omitted and single mode: `output/single_{HH-MM}_{YYYY-MM-DD}.json`
  - If `--out` is omitted and batch mode: `output/batch{count}_{HH-MM}_{YYYY-MM-DD}.json`
- Extracted files: `output/`

For each result, markdown/text content is saved when available (`markdown_content`, `text_content`, etc.).

## Logging

Uses `loguru` for runtime logs.

```bash
LOG_LEVEL=DEBUG python main.py --url "https://example.com/file.pdf"
```

## Behavior notes

- Single mode does not use `/v1/retrieve`; it uses the `/v1/scrapes` response directly.
- Batch mode uses `/v1/retrieve` per completed item.
- Retrieve formats are normalized to Olostep-supported values (`html`, `markdown`, `json`).
