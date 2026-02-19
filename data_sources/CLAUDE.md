# data_sources/

Raw market data from four external providers. Each subdirectory contains data from one source.

| Directory | Contents | Source |
|---|---|---|
| `stocks/` | Hourly OHLCV price data for individual stocks | firstratedata.com (manual download) |
| `etfs/` | Hourly OHLCV price data for ETFs | firstratedata.com (manual download) |
| `market_cap/` | Daily market capitalization per ticker | Financial Modeling Prep API |
| `insider_trades/` | SEC Form 4 insider trade filings | sec-api.io |

See the CLAUDE.md in each subdirectory for detailed schema and file structure.
