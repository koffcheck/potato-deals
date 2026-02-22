#!/usr/bin/env python3
"""
Validation script: tests Steam API, covers, sales, and price history sources.
Run: python3 scripts/validate.py
"""
import json
import sys
import urllib.request
import urllib.error
import re
import time
from typing import Any, Dict, List, Optional

TEST_APPIDS = {
    "Resident Evil": 418370,       # RE7 — known discount history
    "Return of the Obra Dinn": 653530,
    "Red Dead Redemption 2": 1174180,
    "Escape from Tarkov": 0,       # Not on Steam store — special case
    "The Cube": 2954430,           # "The Cube" on Steam
}

# Regions to validate
TEST_REGIONS = ["us", "kz", "ru", "uz"]

def fetch_json(url: str, timeout: int = 10) -> Optional[Dict]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  ❌ FETCH FAILED: {url} → {e}")
        return None

def fetch_status(url: str, timeout: int = 8) -> int:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0

# ─── A. Validate Steam Price API ───────────────────────────────────────────

def validate_prices():
    print("\n" + "=" * 70)
    print("A. STEAM PRICE API VALIDATION")
    print("=" * 70)

    for region in TEST_REGIONS:
        # Steam API uses country codes (uppercase 2-letter)
        cc = region.upper()
        if cc == "EU":
            cc = "DE"  # Use Germany for EU region
        print(f"\n── Region: {region} (cc={cc}) ──")

        for name, appid in TEST_APPIDS.items():
            if appid == 0:
                print(f"  {name}: SKIPPED (not on Steam)")
                continue

            url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc={cc}&filters=price_overview"
            data = fetch_json(url)
            time.sleep(0.5)  # Respect rate limits

            if not data:
                continue

            app_data = data.get(str(appid), {})
            if not app_data.get("success"):
                print(f"  {name} ({appid}): ⚠️ API returned success=false")
                continue

            price_data = app_data.get("data", {}).get("price_overview")
            if not price_data:
                print(f"  {name} ({appid}): FREE or no price data")
                continue

            currency = price_data.get("currency", "?")
            final = price_data.get("final", 0) / 100
            initial = price_data.get("initial", 0) / 100
            discount = price_data.get("discount_percent", 0)

            print(f"  {name} ({appid}): {final:.2f} {currency}"
                  f" (initial: {initial:.2f}, discount: -{discount}%)")

# ─── B. Validate store_country_from_region mapping ─────────────────────────

def validate_region_mapping():
    print("\n" + "=" * 70)
    print("B. REGION → CC MAPPING VALIDATION")
    print("=" * 70)

    sys.path.insert(0, "/Users/antonsannikov/Downloads/analys/review_310/potato-deals")
    try:
        from backend.providers import store_country_from_region
        for region in TEST_REGIONS + ["eu", "gb", "tr", "ar", "ua", "br", "cn", "in"]:
            cc = store_country_from_region(region)
            print(f"  region='{region}' → cc='{cc}'")
    except Exception as e:
        print(f"  ❌ IMPORT ERROR: {e}")

# ─── C. Validate Cover Art URLs ───────────────────────────────────────────

def validate_covers():
    print("\n" + "=" * 70)
    print("C. COVER ART URL VALIDATION")
    print("=" * 70)

    cdn_patterns = [
        "https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{appid}/library_600x900_2x.jpg",
        "https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{appid}/capsule_616x353.jpg",
        "https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{appid}/header.jpg",
        "https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg",
        "https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/capsule_231x87.jpg",
        "https://steamcdn-a.akamaihd.net/steam/apps/{appid}/header.jpg",
    ]

    for name, appid in TEST_APPIDS.items():
        if appid == 0:
            print(f"\n  {name}: SKIPPED (no appid)")
            continue
        print(f"\n  {name} ({appid}):")
        found = False
        for pattern in cdn_patterns:
            url = pattern.format(appid=appid)
            status = fetch_status(url)
            label = "✅" if status == 200 else "❌"
            suffix = url.split("/")[-1]
            print(f"    {label} {suffix}: HTTP {status}")
            if status == 200:
                found = True
        if not found:
            print(f"    ⚠️ NO WORKING COVER URL FOUND!")

# ─── D. Validate Sales Metadata ───────────────────────────────────────────

def validate_sales():
    print("\n" + "=" * 70)
    print("D. SALES METADATA VALIDATION")
    print("=" * 70)

    sys.path.insert(0, "/Users/antonsannikov/Downloads/analys/review_310/potato-deals")
    try:
        from backend.providers import SteamSalesProvider
        provider = SteamSalesProvider()
        result = provider.get_sales_events()
        events = result.get("events", [])
        print(f"\n  Total events fetched: {len(events)}")
        print(f"  Source: {result.get('source', 'unknown')}")
        if result.get("warning"):
            print(f"  ⚠️ Warning: {result['warning']}")

        for i, ev in enumerate(events[:15]):
            name = ev.get("name", "???")
            url = ev.get("url", "")
            start_ts = ev.get("start_ts", 0)
            end_ts = ev.get("end_ts", 0)
            status = ev.get("status", "?")
            url_valid = ev.get("url_valid", None)

            # Check if the URL is actually reachable
            url_status = fetch_status(url) if url else 0

            start_str = time.strftime("%Y-%m-%d", time.gmtime(start_ts)) if start_ts else "?"
            end_str = time.strftime("%Y-%m-%d", time.gmtime(end_ts)) if end_ts else "?"

            print(f"\n  [{i+1}] {name}")
            print(f"      URL: {url}")
            print(f"      URL reachable: HTTP {url_status}")
            print(f"      Dates: {start_str} → {end_str}")
            print(f"      Status: {status}, url_valid: {url_valid}")

    except Exception as e:
        import traceback
        print(f"  ❌ SALES VALIDATION ERROR: {e}")
        traceback.print_exc()

# ─── E. Validate ITAD Provider (price history) ────────────────────────────

def validate_itad():
    print("\n" + "=" * 70)
    print("E. ITAD PRICE HISTORY VALIDATION")
    print("=" * 70)

    sys.path.insert(0, "/Users/antonsannikov/Downloads/analys/review_310/potato-deals")
    try:
        from backend.itad_provider import ITADHistoryProvider
        provider = ITADHistoryProvider()
        if not provider.is_configured():
            print("  ⚠️ ITAD API key NOT configured — no real price history available")
            print("  → Price charts and historical low depend on this.")
            print("  → Without it, only synthetic/placeholder data is shown.")
        else:
            print("  ✅ ITAD API key is configured")
            # Test one game
            test_appid = 418370  # RE7
            history = provider.fetch_price_history(test_appid, months=6, country="US")
            print(f"  History points for RE7 (appid {test_appid}): {len(history)}")
            if history:
                for h in history[:3]:
                    print(f"    {h.get('timestamp')}: ${h.get('price')}")
    except Exception as e:
        print(f"  ❌ ITAD ERROR: {e}")

# ─── F. Validate conversion logic ─────────────────────────────────────────

def validate_conversion():
    print("\n" + "=" * 70)
    print("F. CURRENCY CONVERSION VALIDATION")
    print("=" * 70)

    # Fetch real exchange rates
    url = "https://open.er-api.com/v6/latest/USD"
    data = fetch_json(url)
    if not data or data.get("result") != "success":
        print("  ❌ Cannot fetch exchange rates")
        return

    rates = data.get("rates", {})
    test_pairs = [("USD", "KZT"), ("USD", "RUB"), ("USD", "EUR"), ("KZT", "USD")]
    for src, tgt in test_pairs:
        if src in rates and tgt in rates:
            rate = rates[tgt] / rates[src]
            example = 9.99 * rate if src == "USD" else 9.99 / rates[src] * rates[tgt]
            print(f"  {src} → {tgt}: rate={rate:.4f}, 9.99 {src} = {example:.2f} {tgt}")

# ─── Run all ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Potato Deals v3.1.2 — Data Validation Report")
    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")

    validate_prices()
    validate_region_mapping()
    validate_covers()
    validate_sales()
    validate_itad()
    validate_conversion()

    print("\n" + "=" * 70)
    print("VALIDATION COMPLETE")
    print("=" * 70)
