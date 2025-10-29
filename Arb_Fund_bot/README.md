# Arb_Fund_bot Funding Analyzer

Utility script for aggregating historical perpetual funding rates across a mix
of centralized (CEX) and decentralized (DEX) exchanges. The tool ingests a
ticker (for example `BTC`) and an optional lookback window in days, gathers the
longest available history up to the requested window, and prints annualized
average funding rates (APY) per venue.

## Features
- Pulls CEX funding history via `ccxt` for Bybit, OKX, HTX, Bitget, KuCoin
  Futures, and MEXC Futures.
- Queries DEX funding data from Loris Tools when available, Hyperliquid via its
  public API, and ApeX Pro through the REST endpoints.
- Falls back to progressively shorter windows (down to 1 day) when the
  requested period is not available.
- Outputs APY in percentage terms alongside the effective day count, number of
  samples, and coverage.
- Tracks whether each DEX is listed on Loris (`is in loris.tools` column).
- Auto-appends newly discovered Loris DEX protocols to `exchanges.txt`, keeping
  the local configuration in sync.

## Requirements
- Python 3.11+ (tested with 3.13).
- Python packages:
  ```bash
  pip install ccxt requests urllib3
  ```
- Network access to exchange/public APIs (Loris Tools, Hyperliquid, ApeX Pro,
  etc.).

## Configuration (`exchanges.txt`)
`exchanges.txt` provides the curated list of exchanges to query. It is split
into CEX and DEX sections, separated by `---`:

```
# CEX
bybit
okx
...
---
# DEX
Lighter Perps|source=loris|loris=lighter_1_perp|interval_hours=8
Hyperliquid|source=hyperliquid
...
```

- Each CEX entry is the lowercase ccxt exchange identifier. Optional display
  names can be attached with `|Display Name`.
- Each DEX entry defines a display name and optional attributes:
  - `source`: `loris`, `hyperliquid`, `apex`, or left unset (no fetcher yet).
  - `loris`: Loris exchange identifier (required for `source=loris`).
  - `interval_hours`, `symbol`, `ticker`, `quote`, etc. override defaults.
- When the script discovers a new Loris DEX that is not already in the list (and
  not a known CEX), it appends a new line under the `# DEX` section with
  `source=loris` and the appropriate interval.

## Usage
```bash
py -3 funding_analysis.py BTC 180
```
- `ticker`: base asset (case-insensitive).
- `days` (optional): target window in days. Defaults to 180. Values are clamped
  between 1 and 730.
The script prints two tables:
1. **Centralized Exchanges** - APY, window length, and execution details for
   each configured CEX.
2. **DEX** - Same metrics plus an is in loris.tools flag indicating Loris
   coverage.

If an exchange lacks data for the requested window, the tool steps down through
smaller windows (180 -> 150 -> ... -> 30 -> 1). Failures are reported with
descriptive messages in the output table.

## Notes
- Install `ccxt` before running the script; otherwise it will exit with an
  informative message.
- Some exchanges (Apex Protocol, Avantis, Reya, Orderly, GRVT, etc.) may still
  be marked as "No fetcher configured" pending dedicated integrations.
- Funding APY is derived from the compounded average of the normalized funding
  rate over the selected window; ensure you understand the cadence (hourly
  versus 8-hourly) when comparing with external dashboards.

