# stocks/

Hourly OHLCV price data for individual stocks, adjusted for splits and dividends.

**Source**: firstratedata.com (manual download required)

## Directory Layout

```
stocks/
└── data/
    ├── stock_A_full_1hour_adjsplitdiv_*.zip
    ├── stock_B_full_1hour_adjsplitdiv_*.zip
    └── ... (26 ZIP files, one per letter A-Z)
```

## ZIP Contents

Each ZIP contains one `.txt` file per ticker:

```
AAPL_full_1hour_adjsplitdiv.txt
MSFT_full_1hour_adjsplitdiv.txt
...
```

Ticker is extracted from the filename by stripping the `_full_1hour_adjsplitdiv.txt` suffix.

## File Format

Headerless CSV, 6 columns, comma-delimited:

```
2000-01-03 09:00:00,182.50,183.10,181.90,182.80,87140028
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
- When the same ticker exists in both `stocks/` and `etfs/`, the ETF version takes precedence
