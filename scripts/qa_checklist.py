#!/usr/bin/env python3
import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from backend.providers import SteamPriceProvider, SteamSalesProvider, build_price_cache_key  # noqa: E402


@dataclass
class CheckResult:
    code: str
    title: str
    status: str  # PASS | FAIL | SKIP
    details: str


def load_data(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"settings": {}, "wishlist": [], "price_cache": {}}
    with open(path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        return {"settings": {}, "wishlist": [], "price_cache": {}}
    return raw


def parse_ts(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def to_float(raw: Any) -> Optional[float]:
    try:
        return round(float(raw), 2)
    except (TypeError, ValueError):
        return None


def computed_discount(initial: float, price: float) -> int:
    if initial <= 0 or price >= (initial - 0.01):
        return 0
    value = int(round(((initial - price) * 100.0) / initial))
    return max(0, min(value, 99))


def cache_for_region(price_cache: Dict[str, Any], appid: int, region: str) -> Dict[str, Any]:
    key = build_price_cache_key(appid, region)
    row = price_cache.get(key)
    if isinstance(row, dict):
        return row
    legacy = price_cache.get(str(appid))
    if isinstance(legacy, dict):
        return legacy
    return {}


def check_prices(data: Dict[str, Any], region: str) -> CheckResult:
    wishlist = data.get("wishlist", [])
    price_cache = data.get("price_cache", {})
    if not isinstance(wishlist, list) or not wishlist:
        return CheckResult("A", "Price correctness", "SKIP", "Wishlist is empty.")

    appids: List[int] = []
    for row in wishlist:
        try:
            appid = int((row or {}).get("appid", 0))
        except (TypeError, ValueError):
            continue
        if appid > 0:
            appids.append(appid)

    if not appids:
        return CheckResult("A", "Price correctness", "SKIP", "No valid appids in wishlist.")

    provider = SteamPriceProvider()
    live = provider.getCurrentPrices(appids, region).get("prices", {})

    mismatches: List[str] = []
    compared = 0
    for appid in appids:
        cache = cache_for_region(price_cache, appid, region)
        current = cache.get("current") if isinstance(cache.get("current"), dict) else {}
        cached_price = to_float(current.get("price"))
        cached_currency = str(current.get("currency") or "").upper().strip()
        if cached_price is None or not cached_currency:
            continue

        live_row = live.get(appid) if isinstance(live, dict) else None
        if not isinstance(live_row, dict):
            continue
        live_price = to_float(live_row.get("price"))
        live_currency = str(live_row.get("currency") or "").upper().strip()
        if live_price is None or not live_currency:
            continue

        compared += 1
        if abs(cached_price - live_price) > 0.01 or cached_currency != live_currency:
            mismatches.append(
                f"appid {appid}: cache={cached_price} {cached_currency}, live={live_price} {live_currency}"
            )

    if compared == 0:
        return CheckResult("A", "Price correctness", "SKIP", "No comparable price rows (cache/live).")
    if mismatches:
        return CheckResult("A", "Price correctness", "FAIL", "; ".join(mismatches[:8]))
    return CheckResult("A", "Price correctness", "PASS", f"Compared {compared} games.")


def check_discount_math(data: Dict[str, Any], region: str) -> CheckResult:
    wishlist = data.get("wishlist", [])
    price_cache = data.get("price_cache", {})
    if not isinstance(wishlist, list) or not wishlist:
        return CheckResult("B", "Discount correctness", "SKIP", "Wishlist is empty.")

    errors: List[str] = []
    checked = 0
    for row in wishlist:
        try:
            appid = int((row or {}).get("appid", 0))
        except (TypeError, ValueError):
            continue
        if appid <= 0:
            continue
        cache = cache_for_region(price_cache, appid, region)
        current = cache.get("current") if isinstance(cache.get("current"), dict) else {}
        price = to_float(current.get("price"))
        initial = to_float(current.get("initial_price"))
        if price is None or initial is None:
            continue

        checked += 1
        expected = computed_discount(initial, price)
        try:
            stored = int(current.get("discount_percent", 0))
        except (TypeError, ValueError):
            stored = 0
        stored = max(0, min(stored, 99))

        if abs(expected - stored) > 1:
            errors.append(f"appid {appid}: expected {expected}%, stored {stored}%")

        inconsistent = current.get("price_data_consistent") is False
        if inconsistent and stored > 0:
            errors.append(f"appid {appid}: inconsistent data but discount badge still > 0")

    if checked == 0:
        return CheckResult("B", "Discount correctness", "SKIP", "No priced rows in cache.")
    if errors:
        return CheckResult("B", "Discount correctness", "FAIL", "; ".join(errors[:8]))
    return CheckResult("B", "Discount correctness", "PASS", f"Checked {checked} games.")


def check_history_low(data: Dict[str, Any], region: str) -> CheckResult:
    wishlist = data.get("wishlist", [])
    price_cache = data.get("price_cache", {})
    if not isinstance(wishlist, list) or not wishlist:
        return CheckResult("C", "Historical Low consistency", "SKIP", "Wishlist is empty.")

    issues: List[str] = []
    checked = 0
    for row in wishlist:
        try:
            appid = int((row or {}).get("appid", 0))
        except (TypeError, ValueError):
            continue
        if appid <= 0:
            continue
        cache = cache_for_region(price_cache, appid, region)
        history = cache.get("history_6m") if isinstance(cache.get("history_6m"), list) else []
        low = cache.get("all_time_low") if isinstance(cache.get("all_time_low"), dict) else None
        verified = bool(cache.get("all_time_low_verified"))

        if not history:
            if low:
                issues.append(f"appid {appid}: all_time_low exists but history_6m is empty")
            continue
        if len(history) < 2:
            continue

        prices = [to_float(point.get("price")) for point in history if isinstance(point, dict)]
        prices = [value for value in prices if value is not None]
        if len(prices) < 2:
            continue

        checked += 1
        min_price = min(prices)
        low_price = to_float((low or {}).get("price"))

        if not verified:
            if low is not None:
                issues.append(f"appid {appid}: low shown while verification flag is false")
            continue
        if low_price is None:
            issues.append(f"appid {appid}: verified low flag set but all_time_low missing")
            continue
        if abs(min_price - low_price) > 0.01:
            issues.append(f"appid {appid}: all_time_low={low_price}, history_min={min_price}")

    if checked == 0:
        return CheckResult("C", "Historical Low consistency", "SKIP", "No verified 6m history in cache.")
    if issues:
        return CheckResult("C", "Historical Low consistency", "FAIL", "; ".join(issues[:8]))
    return CheckResult("C", "Historical Low consistency", "PASS", f"Checked {checked} history rows.")


def check_chart_points(data: Dict[str, Any], region: str) -> CheckResult:
    wishlist = data.get("wishlist", [])
    price_cache = data.get("price_cache", {})
    if not isinstance(wishlist, list) or not wishlist:
        return CheckResult("D", "Chart points (6 months)", "SKIP", "Wishlist is empty.")

    cutoff = datetime.now(timezone.utc) - timedelta(days=180)
    issues: List[str] = []
    checked = 0
    for row in wishlist:
        try:
            appid = int((row or {}).get("appid", 0))
        except (TypeError, ValueError):
            continue
        if appid <= 0:
            continue
        cache = cache_for_region(price_cache, appid, region)
        history = cache.get("history_6m") if isinstance(cache.get("history_6m"), list) else []
        if len(history) < 2:
            continue

        checked += 1
        parsed_ts: List[datetime] = []
        prices: List[float] = []
        had_sale = False
        for point in history:
            if not isinstance(point, dict):
                continue
            ts = parse_ts(point.get("timestamp"))
            price = to_float(point.get("price"))
            if ts is None or price is None:
                continue
            parsed_ts.append(ts)
            prices.append(price)
            if bool(point.get("on_sale")):
                had_sale = True

        if len(parsed_ts) < 2:
            issues.append(f"appid {appid}: less than 2 valid points")
            continue
        if parsed_ts != sorted(parsed_ts):
            issues.append(f"appid {appid}: timestamps are not chronological")
        if any(ts < cutoff for ts in parsed_ts):
            issues.append(f"appid {appid}: points outside 6-month rolling window")
        if had_sale and abs(max(prices) - min(prices)) < 0.01:
            issues.append(f"appid {appid}: sale points present but curve is flat")

    if checked == 0:
        return CheckResult("D", "Chart points (6 months)", "SKIP", "No charts with >=2 points.")
    if issues:
        return CheckResult("D", "Chart points (6 months)", "FAIL", "; ".join(issues[:8]))
    return CheckResult("D", "Chart points (6 months)", "PASS", f"Checked {checked} chart histories.")


def check_cover_art(data: Dict[str, Any], region: str) -> CheckResult:
    wishlist = data.get("wishlist", [])
    price_cache = data.get("price_cache", {})
    if not isinstance(wishlist, list) or not wishlist:
        return CheckResult("E", "Cover art", "SKIP", "Wishlist is empty.")

    missing: List[str] = []
    checked = 0
    for row in wishlist:
        try:
            appid = int((row or {}).get("appid", 0))
        except (TypeError, ValueError):
            continue
        if appid <= 0:
            continue
        cache = cache_for_region(price_cache, appid, region)
        current = cache.get("current") if isinstance(cache.get("current"), dict) else {}
        is_released = bool(current.get("is_released", cache.get("is_released", True)))
        if not is_released:
            continue
        checked += 1
        capsule = str(current.get("capsule_url") or cache.get("capsule_url") or "").strip()
        if not capsule:
            missing.append(f"appid {appid}")

    if checked == 0:
        return CheckResult("E", "Cover art", "SKIP", "No released games in cache.")
    if missing:
        return CheckResult("E", "Cover art", "FAIL", ", ".join(missing[:12]))
    return CheckResult("E", "Cover art", "PASS", f"Checked {checked} released games.")


def check_release_status(data: Dict[str, Any], region: str) -> CheckResult:
    wishlist = data.get("wishlist", [])
    price_cache = data.get("price_cache", {})
    if not isinstance(wishlist, list) or not wishlist:
        return CheckResult("F", "Release status", "SKIP", "Wishlist is empty.")

    unreleased = 0
    missing_date = 0
    for row in wishlist:
        try:
            appid = int((row or {}).get("appid", 0))
        except (TypeError, ValueError):
            continue
        if appid <= 0:
            continue
        cache = cache_for_region(price_cache, appid, region)
        current = cache.get("current") if isinstance(cache.get("current"), dict) else {}
        is_released = bool(current.get("is_released", cache.get("is_released", True)))
        if is_released:
            continue
        unreleased += 1
        release_date = str(current.get("release_date") or cache.get("release_date") or "").strip()
        if not release_date:
            missing_date += 1

    if unreleased == 0:
        return CheckResult("F", "Release status", "SKIP", "No unreleased games in cache.")
    if missing_date > 0:
        return CheckResult(
            "F",
            "Release status",
            "PASS",
            f"{unreleased} unreleased, {missing_date} without date (UI must show TBA).",
        )
    return CheckResult("F", "Release status", "PASS", f"All {unreleased} unreleased games have release_date.")


def check_sales_events() -> CheckResult:
    provider = SteamSalesProvider()
    data = provider.get_sales_events()
    events = data.get("events", []) if isinstance(data, dict) else []
    warning = str((data or {}).get("warning") or "")
    source = str((data or {}).get("source") or "")

    if not isinstance(events, list) or not events:
        return CheckResult("G", "Sales events + links", "FAIL", "Sales list is empty.")

    bad_urls: List[str] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        url = str(ev.get("url") or "").strip()
        if not url.startswith("https://store.steampowered.com/"):
            bad_urls.append(url or "<empty>")
            continue
        path = url.replace("https://store.steampowered.com", "")
        if path in {"", "/"}:
            bad_urls.append(url)
            continue
        if not any(token in path for token in ("/sale/", "/category/", "/developer/", "/publisher/", "/franchise/")):
            bad_urls.append(url)

    if bad_urls:
        return CheckResult("G", "Sales events + links", "FAIL", "Invalid sale URLs: " + "; ".join(bad_urls[:8]))

    note = f"{len(events)} events from {source or 'unknown source'}."
    if warning:
        note += f" Warning: {warning}"
    return CheckResult("G", "Sales events + links", "PASS", note)


def print_results(results: List[CheckResult]) -> int:
    print("Potato Deals QA checklist")
    print("=" * 72)
    for result in results:
        print(f"[{result.status}] {result.code}) {result.title}")
        print(f"  {result.details}")
    print("=" * 72)
    failed = [item for item in results if item.status == "FAIL"]
    skipped = [item for item in results if item.status == "SKIP"]
    print(f"FAIL: {len(failed)} | SKIP: {len(skipped)} | TOTAL: {len(results)}")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Potato Deals pre-release QA checklist.")
    parser.add_argument("--region", default="us", help="Steam store region (default: us).")
    parser.add_argument(
        "--data-file",
        default=os.path.join(os.getcwd(), "potato_data.json"),
        help="Path to potato_data.json (default: ./potato_data.json).",
    )
    args = parser.parse_args()

    data = load_data(args.data_file)
    region = str(args.region or data.get("settings", {}).get("region", "us")).lower().strip()[:2] or "us"

    results = [
        check_prices(data, region),
        check_discount_math(data, region),
        check_history_low(data, region),
        check_chart_points(data, region),
        check_cover_art(data, region),
        check_release_status(data, region),
        check_sales_events(),
    ]
    return print_results(results)


if __name__ == "__main__":
    raise SystemExit(main())
