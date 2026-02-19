# etfs/

Hourly OHLCV price data for ETFs, adjusted for splits and dividends.

**Source**: firstratedata.com (manual download required)

## Directory Layout

```
etfs/
└── data/
    ├── etf_A_full_1hour_adjsplitdiv_*.zip
    ├── etf_B_full_1hour_adjsplitdiv_*.zip
    └── ... (26 ZIP files, one per letter A-Z)
```

## ZIP Contents

Each ZIP contains one `.txt` file per ticker:

```
SPY_full_1hour_adjsplitdiv.txt
QQQ_full_1hour_adjsplitdiv.txt
...
```

Ticker is extracted from the filename by stripping the `_full_1hour_adjsplitdiv.txt` suffix.

## File Format

Identical to `stocks/` — headerless CSV, 6 columns, comma-delimited:

```
2000-01-03 09:00:00,148.25,149.00,147.80,148.90,12345678
```

| Position | Name | Type | Description |
|---|---|---|---|
| 1 | timestamp | TIMESTAMP | Bar open time (`YYYY-MM-DD HH:MM:SS`) |
| 2 | open | FLOAT | Opening price |
| 3 | high | FLOAT | High price |
| 4 | low | FLOAT | Low price |
| 5 | close | FLOAT | Closing price |
| 6 | volume | NUMERIC | Share volume (stored as integer in some files, float with `.0` in others) |

## Key Details

- 1-hour bars
- Adjusted for splits and dividends
- Raw source includes pre- and after-market hours
- When the same ticker exists in both `stocks/` and `etfs/`, the ETF version takes precedence over the stock version
