# insider_trades/

SEC Form 4 insider trade filings (purchases and sales by directors, officers, and 10%+ owners).

**Source**: sec-api.io — requires `SEC_API_TOKEN` in `.env`

## Directory Layout

```
insider_trades/
├── .env.template
├── download.py
└── data/
    ├── 2009/
    │   ├── 2009-01.jsonl.gz
    │   ├── 2009-02.jsonl.gz
    │   └── ...
    ├── 2010/
    └── ... (through 2026)
```

## File Format

Gzip-compressed newline-delimited JSON (JSONL.GZ). Each line is one complete Form 4 filing. Decompress with `gunzip -c` or `zcat`.

### JSON Structure

```json
{
  "issuer": { "tradingSymbol": "AAPL" },
  "periodOfReport": "2024-01-15",
  "reportingOwner": {
    "cik": "0001234567",
    "name": "John Doe",
    "relationship": {
      "isDirector": true,
      "isOfficer": false,
      "isTenPercentOwner": false,
      "isOther": false
    }
  },
  "nonDerivativeTable": {
    "transactions": [
      {
        "transactionDate": "2024-01-15",
        "coding": {
          "code": "S"
        },
        "amounts": {
          "shares": 1000,
          "pricePerShare": 185.50,
          "acquiredDisposedCode": "D"
        },
        "postTransactionAmounts": {
          "sharesOwnedFollowingTransaction": 50000
        },
        "ownershipNature": {
          "directOrIndirectOwnership": "D"
        }
      }
    ]
  }
}
```

### Key Field Paths

| Field path | Description |
|---|---|
| `issuer.tradingSymbol` | Ticker symbol |
| `periodOfReport` | Filing period date |
| `reportingOwner.cik` | Insider CIK identifier |
| `reportingOwner.name` | Insider name |
| `reportingOwner.relationship.isDirector` | Boolean |
| `reportingOwner.relationship.isOfficer` | Boolean |
| `reportingOwner.relationship.isTenPercentOwner` | Boolean |
| `nonDerivativeTable.transactions[].transactionDate` | Trade date |
| `nonDerivativeTable.transactions[].coding.code` | Transaction code |
| `nonDerivativeTable.transactions[].amounts.shares` | Shares traded |
| `nonDerivativeTable.transactions[].amounts.pricePerShare` | Price per share |
| `nonDerivativeTable.transactions[].amounts.acquiredDisposedCode` | `A` (acquired) or `D` (disposed) |
| `nonDerivativeTable.transactions[].postTransactionAmounts.sharesOwnedFollowingTransaction` | Shares owned after trade |
| `nonDerivativeTable.transactions[].ownershipNature.directOrIndirectOwnership` | `D` (direct) or `I` (indirect) |

### Transaction Codes (`coding.code`)

| Code | Meaning |
|---|---|
| `P` | Purchase |
| `S` | Sale |
| `A`, `F`, `G`, `J`, `M` | Other transaction types |

## Key Details

- Files span 2009–2026 (not all months may be present)
- Each filing may contain multiple transactions in the `nonDerivativeTable.transactions` array
- Set `SEC_API_TOKEN` in a `.env` file (see `.env.template`) before running `download.py`
