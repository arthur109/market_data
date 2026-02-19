# Insider Trades

SEC Form 4 insider trade filings downloaded from [sec-api.io](https://sec-api.io).

Data is organized by month:

```
data/
├── 2009/
│   ├── 2009-01.jsonl.gz
│   ├── 2009-02.jsonl.gz
│   └── ...
├── 2010/
└── ...
```

Each file is a gzip-compressed newline-delimited JSON (JSONL) containing Form 4 transaction records for that month.

## Setup

```bash
cp .env.template .env
# Add your sec-api.io key to .env
```

## Downloading

```bash
# Download everything
python download.py

# Download a specific range
python download.py --from 2024-01 --to 2024-12

# Force re-download of existing files
python download.py --force
```

The script skips files that are already present with the correct size. If interrupted, simply re-run — completed files are skipped and the interrupted file is re-downloaded from scratch.

A fresh index is fetched from sec-api.io each run and saved to `temp/index.json` (not tracked in git).

## Requirements

- Python 3.x
- `requests` (`pip install requests`)
- A valid API key from [sec-api.io](https://sec-api.io)
