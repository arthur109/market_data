# Market Cap

Historical daily market capitalization per stock ticker, downloaded from
[Financial Modeling Prep](https://financialmodelingprep.com) (FMP).

One CSV file per ticker:

```
data/
├── AAPL.csv
├── MSFT.csv
└── ...
```

Each file has two columns:

```
date,market_cap
2024-12-31,3756890000000
2024-12-30,3701230000000
...
```

## Setup

```bash
cp .env.template .env
# Add your FMP API key to .env
```

## Downloading

Tickers are auto-discovered from the stocks data source (`../stocks/data/*.zip`).
Populate that directory first, or pass an explicit ticker list.

```bash
# Download all tickers found in the stocks data source
python download.py

# Limit to a specific date range
python download.py --from 2010-01-01 --to 2024-12-31

# Use an explicit ticker list (one per line)
python download.py --tickers my_tickers.txt

# Re-download files that already exist
python download.py --force

# Retry tickers that failed in a previous run
python download.py --retry-failed
```

The script skips tickers whose CSV already exists. If interrupted, re-run — completed
files are skipped and interrupted downloads are restarted from scratch.

Failed tickers are logged to `temp/failed.json` (not tracked in git).

## Requirements

- Python 3.x
- `requests` (`pip install requests`)
- A valid API key from [financialmodelingprep.com](https://financialmodelingprep.com)
