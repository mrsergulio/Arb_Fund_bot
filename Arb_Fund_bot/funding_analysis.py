#!/usr/bin/env python3
"""
Funding rate aggregation tool.

Fetches historical perpetual funding rates for a given ticker across a
curated list of centralized and decentralized exchanges, computing the
average rate over the longest lookback window available between the requested
span and 1 day.

Centralized exchanges are accessed via ccxt. Hyperliquid is queried directly.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import math
import sys
import time
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Set
from pathlib import Path

try:
    import ccxt  # type: ignore
except ImportError:
    print("ccxt is required. Install it with: pip install ccxt", file=sys.stderr)
    sys.exit(1)

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DAY_MS = 24 * 60 * 60 * 1000
NOW_MS = int(time.time() * 1000)
HTTP_TIMEOUT = 25
REQUEST_RETRY_TOTAL = 3
REQUEST_BACKOFF_FACTOR = 0.8
LORIS_REQUEST_DELAY = 0.2


def _build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=REQUEST_RETRY_TOTAL,
        read=REQUEST_RETRY_TOTAL,
        connect=REQUEST_RETRY_TOTAL,
        backoff_factor=REQUEST_BACKOFF_FACTOR,
        status_forcelist=(408, 425, 429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP_SESSION = _build_http_session()

DEFAULT_DEX_ORDER = [
    "Lighter",
    "Aster",
    "Hyperliquid",
    "Bluefin",
    "EdgeX",
    "Apex Omni",
    "Reya Perps",
    "GRVT",
    "Paradex",
    "Drift",
    "Extended",
    "Vest",
    "Kuma",
    "Variational",
    "Woofi Pro",
    "Pacifica",
    "Hibachi",
]

DEFAULT_CEX_ORDER = ["bybit", "okx", "htx", "bitget", "kucoinfutures", "mexc"]

KNOWN_LORIS_CEX_DISPLAYS: Set[str] = {
    "binance",
    "bingx",
    "bitget",
    "bybit",
    "cryptocom",
    "gateio",
    "htx",
    "kucoin",
    "mexc",
    "okx",
    "phemex",
}

LORIS_DISPLAY_OVERRIDES: Dict[str, str] = {
    "aster": "Aster Perps",
    "lighter": "Lighter Perps",
    "hyperliquid": "Hyperliquid",
    "bluefin": "Bluefin",
    "edgex": "EdgeX Perps",
    "apex omni": "Apex Protocol",
    "apex": "Apex Protocol",
    "reya": "Reya Perps",
    "grvt": "GRVT Perps",
    "paradex": "Paradex Perps",
    "drift": "Drift Trade",
    "extended": "Extended",
    "vest": "Vest",
    "kuma": "Kuma",
    "variational": "Variational",
    "woofi": "Woofi Pro",
    "pacifica": "Pacifica",
    "hibachi": "Hibachi",
    "orderly": "Orderly Perps",
}


def parse_config_line(raw_line: str) -> Tuple[str, Dict[str, str]]:
    """Split a config line into the first token and additional attributes."""
    parts = [part.strip() for part in raw_line.split("|") if part.strip()]
    if not parts:
        return "", {}
    head = parts[0]
    attributes: Dict[str, str] = {}
    for part in parts[1:]:
        if "=" in part:
            key, value = part.split("=", 1)
            attributes[key.strip().lower()] = value.strip()
        else:
            attributes["display"] = part.strip()
    return head, attributes


def _normalize_loris_display(display: str) -> str:
    normalized = display.strip()
    if not normalized:
        return normalized
    key = normalized.lower()
    override = LORIS_DISPLAY_OVERRIDES.get(key)
    if override:
        return override
    if normalized.isupper():
        return normalized.title()
    return normalized


def _stringify_config_value(value: object) -> str:
    if isinstance(value, float):
        if math.isfinite(value) and value.is_integer():
            return str(int(value))
        return str(value)
    return str(value)


def _format_dex_config_line(name: str, attrs: Dict[str, object]) -> str:
    parts = [name]
    priority_keys = ("source", "loris", "interval_hours", "symbol", "quote", "ticker")
    handled: Set[str] = set()
    for key in priority_keys:
        if key in attrs:
            parts.append(f"{key}={_stringify_config_value(attrs[key])}")
            handled.add(key)
    for key in sorted(attrs.keys()):
        if key in handled:
            continue
        parts.append(f"{key}={_stringify_config_value(attrs[key])}")
    return "|".join(parts)


def append_new_dex_entries_to_file(entries: Sequence[Dict[str, object]]) -> None:
    if not entries:
        return

    path = Path(__file__).with_name("exchanges.txt")
    try:
        existing_text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        existing_text = ""
    except OSError:
        return

    lines_to_append: List[str] = []
    for entry in entries:
        name = str(entry.get("name") or "").strip()
        attrs = entry.get("attrs", {})
        if not name or not isinstance(attrs, dict):
            continue
        lines_to_append.append(_format_dex_config_line(name, attrs))

    if not lines_to_append:
        return

    prefix = ""
    if not existing_text:
        base_lines = ["# CEX", *DEFAULT_CEX_ORDER, "---", "# DEX"]
        prefix = "\n".join(base_lines) + "\n"
    elif not existing_text.endswith("\n"):
        prefix = "\n"

    try:
        with path.open("a", encoding="utf-8") as handle:
            if prefix:
                handle.write(prefix)
            for line in lines_to_append:
                handle.write(f"{line}\n")
    except OSError:
        return


def _default_symbol(ticker: str) -> str:
    return f"{ticker}/USDT:USDT"


CEX_SETTINGS: Dict[str, Dict] = {
    "bybit": {"display": "Bybit", "symbol_fn": _default_symbol},
    "okx": {"display": "OKX", "symbol_fn": _default_symbol},
    "htx": {"display": "HTX", "symbol_fn": _default_symbol, "ccxt_kwargs": {"verify": False}},
    "bitget": {"display": "Bitget", "symbol_fn": _default_symbol, "params": {"productType": "umcbl"}},
    "kucoinfutures": {"display": "KuCoin Futures", "symbol_fn": _default_symbol},
    "mexc": {"display": "MEXC Futures", "symbol_fn": _default_symbol},
}

_loris_exchange_cache: Optional[Dict[str, Dict[str, Optional[float]]]] = None


def _load_loris_exchange_metadata() -> Dict[str, Dict[str, Optional[float]]]:
    global _loris_exchange_cache
    if _loris_exchange_cache is not None:
        return _loris_exchange_cache
    try:
        resp = HTTP_SESSION.get("https://loris.tools/api/funding", timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        exchange_info = payload.get("exchanges", {}).get("exchange_names", [])
        metadata: Dict[str, Dict[str, Optional[float]]] = {}
        for entry in exchange_info:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name", "")).strip()
            if not name:
                continue
            display = str(entry.get("display", "")).strip()
            interval_raw = entry.get("interval")
            try:
                interval_val: Optional[float] = float(interval_raw) if interval_raw is not None else None
            except (TypeError, ValueError):
                interval_val = None
            metadata[name] = {"display": display, "interval": interval_val}
    except (requests.RequestException, json.JSONDecodeError, ValueError):
        metadata = {}
    _loris_exchange_cache = metadata
    return metadata


def get_loris_interval(exchange_id: str) -> Optional[float]:
    metadata = _load_loris_exchange_metadata()
    info = metadata.get(exchange_id)
    if not info:
        return None
    return info.get("interval")


def get_loris_entry_by_display(display_name: str) -> Optional[Dict[str, Optional[float]]]:
    metadata = _load_loris_exchange_metadata()
    display_lower = display_name.lower()
    for name, info in metadata.items():
        display = (info.get("display") or "").lower()
        if display and display == display_lower:
            return {"name": name, "display": info.get("display"), "interval": info.get("interval")}
    return None



def load_exchange_config() -> Dict[str, List]:
    path = Path(__file__).with_name("exchanges.txt")
    cex_entries: List[Dict[str, object]] = []
    dex_entries: List[Dict[str, object]] = []
    current = "cex"

    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line == "---":
                    current = "dex"
                    continue
                head, attrs = parse_config_line(line)
                if not head:
                    continue
                if current == "cex":
                    exchange_id = head.lower()
                    display_name = attrs.pop("display", None)
                    cex_entries.append(
                        {
                            "id": exchange_id,
                            "display": display_name,
                            "attrs": attrs,
                        }
                    )
                else:
                    display_name = attrs.pop("display", head)
                    dex_entries.append(
                        {
                            "name": display_name,
                            "attrs": attrs,
                        }
                    )

    if not cex_entries:
        cex_entries = [
            {
                "id": exchange_id,
                "display": CEX_SETTINGS.get(exchange_id, {}).get("display", exchange_id.upper()),
                "attrs": {},
            }
            for exchange_id in DEFAULT_CEX_ORDER
        ]
    else:
        for entry in cex_entries:
            if not entry.get("display"):
                settings = CEX_SETTINGS.get(entry["id"], {})
                entry["display"] = settings.get("display", entry["id"].upper())

    if not dex_entries:
        dex_entries = [{"name": name, "attrs": {}} for name in DEFAULT_DEX_ORDER]

    return {"cex": cex_entries, "dex": dex_entries}


@dataclasses.dataclass
class FundingWindowResult:
    average_rate: float
    entries: int
    window_days: float
    coverage_days: float
    apy: float


@dataclasses.dataclass
class FundingResult:
    exchange: str
    success: bool
    message: str
    window: Optional[FundingWindowResult] = None


RatePoint = Tuple[int, float]  # timestamp in ms, funding rate as float


def _estimate_interval_hours(entries: int, coverage_days: float, default_interval_hours: float) -> float:
    if entries > 1 and coverage_days > 0:
        hours = (coverage_days * 24.0) / (entries - 1)
        if hours > 0:
            return hours
    return default_interval_hours


def _compute_apy(
    avg_rate: float,
    entries: int,
    coverage_days: float,
    default_interval_hours: float,
    forced_interval_hours: Optional[float] = None,
) -> float:
    interval_hours = forced_interval_hours or _estimate_interval_hours(entries, coverage_days, default_interval_hours)
    periods_per_year = (24.0 / interval_hours) * 365.0
    if periods_per_year <= 0:
        return 0.0
    base = 1.0 + avg_rate
    if base <= 0.0:
        return avg_rate * periods_per_year
    return math.pow(base, periods_per_year) - 1.0


def _day_sequence(max_days: int) -> List[int]:
    return list(range(max_days, 0, -1))


def _ms_to_days(duration_ms: int) -> float:
    return duration_ms / (24 * 60 * 60 * 1000)


def choose_window(
    rates: Sequence[RatePoint],
    day_seq: Sequence[int],
    default_interval_hours: float,
    forced_interval_hours: Optional[float] = None,
) -> Optional[FundingWindowResult]:
    if not rates:
        return None
    ordered = sorted(rates, key=lambda x: x[0])
    end_ts = ordered[-1][0]

    for days in day_seq:
        window_start = end_ts - days * DAY_MS
        subset = [r for r in ordered if r[0] >= window_start]
        if not subset:
            continue
        coverage = subset[-1][0] - subset[0][0]
        required = days * DAY_MS
        # allow partial coverage; accept if at least half of the window is covered
        if coverage >= 0.5 * required or days == 1 or len(subset) >= 2:
            avg = sum(rate for _, rate in subset) / len(subset)
            coverage_days = _ms_to_days(coverage)
            apy = _compute_apy(avg, len(subset), coverage_days, default_interval_hours, forced_interval_hours)
            return FundingWindowResult(
                average_rate=avg,
                entries=len(subset),
                window_days=days,
                coverage_days=coverage_days,
                apy=apy,
            )

    # as a fallback, use the entire dataset
    coverage = ordered[-1][0] - ordered[0][0]
    avg = sum(rate for _, rate in ordered) / len(ordered)
    fallback_days = max(coverage / DAY_MS, 0.0)
    coverage_days = _ms_to_days(coverage)
    apy = _compute_apy(avg, len(ordered), coverage_days, default_interval_hours, forced_interval_hours)
    return FundingWindowResult(
        average_rate=avg,
        entries=len(ordered),
        window_days=fallback_days,
        coverage_days=coverage_days,
        apy=apy,
    )


def _aggregate_hourly_to_eight_hour(data: Sequence[RatePoint]) -> List[RatePoint]:
    if not data:
        return []
    aggregated: List[RatePoint] = []
    bucket_rates: List[float] = []
    bucket_times: List[int] = []

    for ts, rate in sorted(data, key=lambda x: x[0]):
        bucket_rates.append(rate)
        bucket_times.append(ts)
        if len(bucket_rates) == 8:
            factor = math.prod((1.0 + r) for r in bucket_rates)
            agg_rate = factor - 1.0
            aggregated.append((bucket_times[-1], agg_rate))
            bucket_rates.clear()
            bucket_times.clear()

    if bucket_rates:
        factor = math.prod((1.0 + r) for r in bucket_rates)
        if factor <= 0.0:
            agg_rate = sum(bucket_rates) / len(bucket_rates)
        else:
            agg_rate = math.pow(factor, 8.0 / len(bucket_rates)) - 1.0
        aggregated.append((bucket_times[-1], agg_rate))

    return aggregated


class CCXTFundingFetcher:
    def __init__(
        self,
        exchange_id: str,
        display_name: str,
        market_symbol_fn: Callable[[str], str],
        *,
        ccxt_kwargs: Optional[Dict] = None,
        params: Optional[Dict] = None,
        limit: int = 200,
        default_interval_hours: float = 8.0,
    ) -> None:
        self.exchange_id = exchange_id
        self.display_name = display_name
        self.market_symbol_fn = market_symbol_fn
        self.ccxt_kwargs = ccxt_kwargs or {}
        self.params = params or {}
        self.limit = limit
        self.default_interval_hours = default_interval_hours

    def fetch(self, ticker: str, days: int) -> FundingResult:
        try:
            exchange_cls = getattr(ccxt, self.exchange_id)
        except AttributeError:
            return FundingResult(self.display_name, False, f"ccxt exchange '{self.exchange_id}' is unavailable")

        exchange = exchange_cls({"enableRateLimit": True, **self.ccxt_kwargs})
        symbol = self.market_symbol_fn(ticker)
        since = NOW_MS - days * DAY_MS
        try:
            raw = exchange.fetch_funding_rate_history(
                symbol,
                since=since,
                limit=self.limit,
                params=self.params,
            )
        except Exception as exc:  # pylint: disable=broad-except
            return FundingResult(self.display_name, False, f"API error: {exc}")

        data: List[RatePoint] = []
        for item in raw:
            ts = int(item.get("timestamp") or item.get("timepoint") or item.get("fundingTime") or NOW_MS)
            rate_val = item.get("fundingRate")
            try:
                rate = float(rate_val)
            except (TypeError, ValueError):
                continue
            if math.isnan(rate):
                continue
            if ts <= NOW_MS:
                data.append((ts, rate))

        if not data:
            return FundingResult(self.display_name, False, "No funding data retrieved")

        days_seq = _day_sequence(days)
        window = choose_window(data, days_seq, self.default_interval_hours)
        if window is None:
            return FundingResult(self.display_name, False, "Unable to compute average window")
        return FundingResult(self.display_name, True, "ok", window)


class HyperliquidFetcher:
    BASE_URL = "https://api.hyperliquid.xyz/info"
    PAGE_LIMIT = 500
    MAX_PAGES = 24

    def fetch(self, ticker: str, days: int) -> FundingResult:
        target_start = NOW_MS - days * DAY_MS
        cursor = target_start
        data: List[RatePoint] = []
        last_ts_seen: Optional[int] = None
        pages = 0

        while cursor <= NOW_MS and pages < self.MAX_PAGES:
            payload = {
                "type": "fundingHistory",
                "coin": ticker.upper(),
                "startTime": cursor,
            }
            try:
                resp = HTTP_SESSION.post(self.BASE_URL, json=payload, timeout=HTTP_TIMEOUT)
                resp.raise_for_status()
                raw = resp.json()
            except requests.RequestException as exc:
                return FundingResult("Hyperliquid", False, f"API error: {exc}")
            except json.JSONDecodeError as exc:
                return FundingResult("Hyperliquid", False, f"Invalid JSON: {exc}")

            if not isinstance(raw, list) or not raw:
                break

            last_raw_ts: Optional[int] = None
            for item in raw:
                if not isinstance(item, dict):
                    continue
                ts = item.get("time")
                rate = item.get("fundingRate")
                try:
                    ts_int = int(ts)
                    rate_f = float(rate)
                except (TypeError, ValueError):
                    continue
                last_raw_ts = ts_int
                if ts_int > NOW_MS:
                    continue
                if ts_int < target_start:
                    continue
                if last_ts_seen is not None and ts_int <= last_ts_seen:
                    continue
                data.append((ts_int, rate_f))
                last_ts_seen = ts_int

            if last_raw_ts is None:
                break

            pages += 1
            if last_ts_seen is not None and last_ts_seen >= NOW_MS:
                break

            cursor = last_raw_ts + 1
            if len(raw) < self.PAGE_LIMIT:
                break

        if not data:
            return FundingResult("Hyperliquid", False, "No funding data retrieved")

        data.sort(key=lambda x: x[0])

        aggregated = _aggregate_hourly_to_eight_hour(data)
        if not aggregated:
            return FundingResult("Hyperliquid", False, "No funding data retrieved")

        days_seq = _day_sequence(days)
        window = choose_window(aggregated, days_seq, default_interval_hours=8.0, forced_interval_hours=8.0)
        if window is None:
            return FundingResult("Hyperliquid", False, "Unable to compute average window")
        return FundingResult("Hyperliquid", True, "ok", window)


class LorisFundingFetcher:
    BASE_URL = "https://loris.tools/api/funding/historical"
    MAX_CHUNK_DAYS = 30

    def __init__(self, exchange_id: str, default_interval_hours: Optional[float] = None):
        self.exchange_id = exchange_id
        metadata_interval = get_loris_interval(exchange_id)
        if default_interval_hours is None:
            default_interval_hours = metadata_interval
        if not default_interval_hours or default_interval_hours <= 0:
            default_interval_hours = 1.0
        self.default_interval_hours = float(default_interval_hours)
        self.forced_interval_hours = self.default_interval_hours

    def fetch(
        self,
        display_name: str,
        symbol: str,
        days: int,
        forced_interval_hours: Optional[float] = None,
    ) -> FundingResult:
        symbol = symbol.upper()
        if days <= 0:
            days = 1
        now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        start = now - dt.timedelta(days=days)

        data_points: Dict[int, float] = {}
        chunk_delta = dt.timedelta(days=self.MAX_CHUNK_DAYS)
        fetch_start = start

        while fetch_start < now:
            fetch_end = min(fetch_start + chunk_delta, now)
            params = {
                "symbol": symbol,
                "start": fetch_start.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "end": fetch_end.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "exchanges": self.exchange_id,
            }
            try:
                resp = HTTP_SESSION.get(self.BASE_URL, params=params, timeout=HTTP_TIMEOUT)
                resp.raise_for_status()
                payload = resp.json()
            except requests.RequestException as exc:
                return FundingResult(display_name, False, f"Loris API error: {exc}")
            except json.JSONDecodeError as exc:
                return FundingResult(display_name, False, f"Loris API invalid JSON: {exc}")

            series = payload.get("series", {}).get(self.exchange_id, [])
            for point in series:
                if not isinstance(point, dict):
                    continue
                ts_str = point.get("t")
                rate_val = point.get("y")
                if ts_str is None or rate_val is None:
                    continue
                try:
                    ts_dt = dt.datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                    ts_ms = int(ts_dt.timestamp() * 1000)
                    rate_decimal = float(rate_val) / 10_000
                except (ValueError, TypeError):
                    continue
                data_points[ts_ms] = rate_decimal

            fetch_start = fetch_end
            time.sleep(LORIS_REQUEST_DELAY)

        if not data_points:
            return FundingResult(display_name, False, "No funding data available")

        ordered = sorted(data_points.items())
        data: List[RatePoint] = [(ts, rate) for ts, rate in ordered]

        days_seq = _day_sequence(days)
        effective_interval = forced_interval_hours or self.forced_interval_hours

        window = choose_window(
            data,
            days_seq,
            default_interval_hours=self.default_interval_hours,
            forced_interval_hours=effective_interval,
        )
        if window is None:
            return FundingResult(display_name, False, "Unable to compute average window")
        return FundingResult(display_name, True, "ok", window)


class ApexFundingFetcher:
    BASE_URL = "https://pro.apex.exchange/api/v3/history-funding"
    PAGE_SIZE = 100

    def __init__(self, default_interval_hours: Optional[float] = None):
        self.default_interval_hours = default_interval_hours or 1.0

    def fetch(
        self,
        display_name: str,
        symbol: str,
        days: int,
        forced_interval_hours: Optional[float] = None,
    ) -> FundingResult:
        symbol = symbol.upper()
        if days <= 0:
            days = 1

        now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        start = now - dt.timedelta(days=days)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)

        data_points: Dict[int, float] = {}
        page = 1
        safety_counter = 0

        while True:
            params = {"symbol": symbol}
            if page > 1:
                params["page"] = page
            try:
                resp = HTTP_SESSION.get(self.BASE_URL, params=params, timeout=HTTP_TIMEOUT)
                resp.raise_for_status()
                payload = resp.json()
            except requests.RequestException as exc:
                return FundingResult(display_name, False, f"Apex API error: {exc}")
            except json.JSONDecodeError as exc:
                return FundingResult(display_name, False, f"Apex API invalid JSON: {exc}")

            entries = payload.get("data", {}).get("historyFunds", [])
            if not entries:
                break

            stop = False
            for entry in entries:
                try:
                    ts = int(entry.get("fundingTime") or entry.get("fundingTimestamp"))
                    rate = float(entry.get("rate"))
                except (TypeError, ValueError):
                    continue

                if ts > end_ms:
                    continue
                if ts < start_ms:
                    stop = True
                    continue
                data_points[ts] = rate

            if stop or len(entries) < self.PAGE_SIZE:
                break

            page += 1
            safety_counter += 1
            if safety_counter > 500:
                break

        if not data_points:
            return FundingResult(display_name, False, "No funding data available")

        ordered = sorted(data_points.items())
        data: List[RatePoint] = [(ts, rate) for ts, rate in ordered]
        days_seq = _day_sequence(days)
        effective_interval = forced_interval_hours or self.default_interval_hours
        window = choose_window(
            data,
            days_seq,
            default_interval_hours=self.default_interval_hours,
            forced_interval_hours=effective_interval,
        )
        if window is None:
            return FundingResult(display_name, False, "Unable to compute average window")
        return FundingResult(display_name, True, "ok", window)


def build_dex_entries(
    dex_config: List[Dict[str, object]],
    limit: Optional[int] = None,
    cex_displays: Optional[Iterable[str]] = None,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    loris_metadata = _load_loris_exchange_metadata()
    cex_display_set = {name.lower() for name in cex_displays} if cex_displays else set()
    auto_added_entries: List[Dict[str, object]] = []

    if dex_config:
        entries = [
            {"name": entry["name"], "attrs": dict(entry.get("attrs", {}))}
            for entry in dex_config
            if entry.get("name")
        ]
    else:
        remote_names: List[str] = []
        try:
            resp = HTTP_SESSION.get("https://api.llama.fi/protocols", timeout=max(HTTP_TIMEOUT, 30))
            resp.raise_for_status()
            payload = resp.json()
            if isinstance(payload, list):
                for protocol in payload:
                    if not isinstance(protocol, dict):
                        continue
                    name = str(protocol.get("name") or "").strip()
                    if not name:
                        continue
                    module = str(protocol.get("module") or "").lower()
                    category = str(protocol.get("category") or "").lower()
                    if any(keyword in name.lower() for keyword in ("perp", "perpetual")):
                        remote_names.append(name)
                    elif "perp" in module:
                        remote_names.append(name)
                    elif category in {"perpetual", "perps", "perpetuals"}:
                        remote_names.append(name)
        except (requests.RequestException, json.JSONDecodeError):
            remote_names = []

        ordered: List[str] = []
        seen = set()

        for preferred in DEFAULT_DEX_ORDER:
            key = preferred.lower()
            if key not in seen:
                ordered.append(preferred)
                seen.add(key)

        for name in remote_names:
            key = name.lower()
            if key in seen:
                continue
            ordered.append(name)
            seen.add(key)

        if "bluefin" not in seen:
            ordered.insert(0, "Bluefin")

        entries = [{"name": name, "attrs": {}} for name in ordered]

    seen_names = {entry["name"].lower() for entry in entries}
    seen_loris_ids: Set[str] = set()
    for entry in entries:
        attrs = entry.get("attrs", {})
        if not isinstance(attrs, dict):
            continue
        loris_attr = attrs.get("loris")
        if loris_attr:
            seen_loris_ids.add(str(loris_attr).lower())

    for loris_name, info in loris_metadata.items():
        display = str(info.get("display") or loris_name)
        normalized_name = _normalize_loris_display(display)
        if not normalized_name:
            continue
        display_key = normalized_name.lower()
        raw_display_key = str(display).lower()
        loris_key = str(loris_name).lower()
        if (
            display_key in seen_names
            or display_key in cex_display_set
            or raw_display_key in KNOWN_LORIS_CEX_DISPLAYS
            or display_key in KNOWN_LORIS_CEX_DISPLAYS
            or loris_key in seen_loris_ids
        ):
            continue
        attrs: Dict[str, object] = {"source": "loris", "loris": str(loris_name)}
        interval_val = info.get("interval")
        if interval_val is not None:
            attrs["interval_hours"] = _stringify_config_value(interval_val)
        entry = {"name": normalized_name, "attrs": attrs}
        entries.append(entry)
        auto_added_entries.append(entry)
        seen_names.add(display_key)
        seen_loris_ids.add(loris_key)

    if limit is not None:
        entries = entries[:limit]
    return entries, auto_added_entries


def format_result(result: FundingResult, in_loris: Optional[bool] = None) -> str:
    if not result.success or result.window is None:
        columns = [
            f"{result.exchange:<18}",
            f"{'-':>10}",
            f"{'-':>8}",
        ]
        if in_loris is not None:
            loris_col = "yes" if in_loris else "no"
            columns.append(f"{loris_col:^17}")
        return " | ".join(columns) + f" | {result.message}"

    apy_pct = result.window.apy * 100.0
    columns = [
        f"{result.exchange:<18}",
        f"{apy_pct:>10.3f}",
        f"{result.window.window_days:>8.2f}",
    ]
    if in_loris is not None:
        loris_col = "yes" if in_loris else "no"
        columns.append(f"{loris_col:^17}")
    details = f"{result.window.entries:>4} pts over {result.window.coverage_days:>6.1f}d"
    return " | ".join(columns) + f" | {details}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute average funding rates across exchanges.")
    parser.add_argument("ticker", help="Base asset ticker, e.g. BTC")
    parser.add_argument(
        "days",
        nargs="?",
        type=int,
        default=None,
        help="Lookback window in days (default: 180).",
    )
    args = parser.parse_args()

    ticker = args.ticker.upper()
    target_days = args.days if args.days is not None else 180
    target_days = max(1, min(target_days, 365 * 2))

    exchange_config = load_exchange_config()

    cex_fetchers: List[CCXTFundingFetcher] = []
    cex_display_names: Set[str] = set()
    for entry in exchange_config["cex"]:
        exchange_id = entry["id"]
        settings = CEX_SETTINGS.get(exchange_id, {})
        display_name = settings.get("display") or entry.get("display") or exchange_id.upper()
        symbol_fn = settings.get("symbol_fn", _default_symbol)
        cex_display_names.add(display_name.lower())
        fetcher = CCXTFundingFetcher(
            exchange_id,
            display_name,
            symbol_fn,
            ccxt_kwargs=settings.get("ccxt_kwargs"),
            params=settings.get("params"),
            default_interval_hours=settings.get("default_interval_hours", 8.0),
        )
        cex_fetchers.append(fetcher)

    for exchange_id, settings in CEX_SETTINGS.items():
        display_name = settings.get("display") or exchange_id.upper()
        cex_display_names.add(display_name.lower())

    print(f"\nTarget ticker: {ticker}")
    print(f"Attempting funding average using up to {target_days} day window.\n")
    print("Centralized Exchanges")
    print("-" * 76)
    print(f"{'Exchange':<18} | {'Avg APY (%)':>10} | {'Days':>8} | Details")
    print("-" * 76)
    for fetcher in cex_fetchers:
        result = fetcher.fetch(ticker, target_days)
        print(format_result(result))
    print("-" * 76)

    dex_entries, newly_added_dex_entries = build_dex_entries(
        exchange_config["dex"], cex_displays=cex_display_names
    )
    append_new_dex_entries_to_file(newly_added_dex_entries)
    hyperliquid_fetcher = HyperliquidFetcher()
    loris_fetcher_cache: Dict[Tuple[str, Optional[float]], LorisFundingFetcher] = {}
    apex_fetcher = ApexFundingFetcher()

    print("\nDEX")
    if not dex_entries:
        print("Unable to retrieve DEX list from DefiLlama.")
        return

    loris_metadata = _load_loris_exchange_metadata()

    print("-" * 96)
    print(f"{'Protocol':<18} | {'Avg APY (%)':>10} | {'Days':>8} | {'is in loris.tools':^17} | Details")
    print("-" * 96)

    for entry in dex_entries:
        name = str(entry.get("name") or "").strip()
        attrs = entry.get("attrs", {}) or {}
        if not name:
            continue

        raw_symbol = attrs.get("symbol")
        base_token = attrs.get("ticker") or ticker
        interval_value = None
        if "interval_hours" in attrs:
            try:
                interval_value = float(attrs["interval_hours"])
            except (TypeError, ValueError):
                interval_value = None

        source = (attrs.get("source") or "").lower()
        loris_info = None
        loris_id = attrs.get("loris")
        if loris_id and loris_id in loris_metadata:
            info = loris_metadata.get(loris_id)
            loris_info = {"name": loris_id, "display": info.get("display"), "interval": info.get("interval")}
        elif not loris_id:
            loris_info = get_loris_entry_by_display(name)

        if source == "hyperliquid":
            symbol_param = raw_symbol or base_token
            result = hyperliquid_fetcher.fetch(symbol_param, target_days)
            result.exchange = name
        elif source == "loris":
            if not loris_id:
                result = FundingResult(name, False, "Missing Loris exchange id")
            else:
                cache_key = (loris_id, interval_value)
                fetcher = loris_fetcher_cache.get(cache_key)
                if fetcher is None:
                    fetcher = LorisFundingFetcher(loris_id, interval_value)
                    loris_fetcher_cache[cache_key] = fetcher
                symbol_param = raw_symbol or base_token
                result = fetcher.fetch(name, symbol_param, target_days, interval_value)
        elif source == "apex":
            quote = attrs.get("quote")
            if raw_symbol:
                apex_symbol = raw_symbol
            else:
                quote_token = (quote or "USDT").upper()
                apex_symbol = f"{base_token.upper()}-{quote_token}"
            result = apex_fetcher.fetch(name, apex_symbol, target_days, interval_value)
        else:
            result = FundingResult(name, False, "No fetcher configured")

        is_in_loris = loris_info is not None
        print(format_result(result, in_loris=is_in_loris))
    print("-" * 96)


if __name__ == "__main__":
    main()
