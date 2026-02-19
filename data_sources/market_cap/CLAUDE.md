# market_cap/

Daily market capitalization per ticker.

**Source**: Financial Modeling Prep (FMP) API — requires `FMP_API_TOKEN` in `.env`

## Directory Layout

```
market_cap/
├── .env.template
├── download.py
└── data/
    ├── A.csv
    ├── AA.csv
    ├── AAPL.csv
    └── ... (~7,250 files, one per ticker)
```

## File Format

CSV with header, 2 columns:

```
date,market_cap
2000-01-03,21295890000
2000-01-04,19390630000
2000-01-05,18451170000
```

| Column | Type | Description |
|---|---|---|
| date | DATE | Trading day (`YYYY-MM-DD`) |
| market_cap | BIGINT | Market cap in USD (integer, no decimal point) |

## Key Details

- One file per ticker, named `{TICKER}.csv`
- Data from 2000 to present
- `download.py` auto-discovers tickers from the stock ZIPs and fetches via the FMP API
- Set `FMP_API_TOKEN` in a `.env` file (see `.env.template`) before running `download.py`
