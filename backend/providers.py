import html
import json
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

from .http_utils import RequestFailedError, RateLimitError, fetch_json_with_retry, fetch_text_with_retry


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_price_cache_key(appid: int, region: str) -> str:
    normalized_region = str(region or "us").strip().lower()[:2] or "us"
    return f"{appid}:{normalized_region}"


def store_country_from_region(region: str) -> str:
    normalized = str(region or "us").strip().lower()[:2] or "us"
    mapping = {
        "us": "us",
        "eu": "de",
        "gb": "gb",
        "ru": "ru",
        "kz": "kz",
        "uz": "uz",
        "tr": "tr",
        "ar": "ar",
        "ua": "ua",
        "br": "br",
        "cn": "cn",
        "in": "in",
    }
    return mapping.get(normalized, normalized)


class PriceProvider(ABC):
    @abstractmethod
    def getCurrentPrice(self, appid: int, region: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def getLastSale(self, game_cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def getPriceHistory(self, game_cache: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def getAllTimeLow(self, game_cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def getAllTimeHigh(self, game_cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


class SteamDbHistoryProvider:
    """Unofficial SteamDB history source. May change or stop working at any time."""

    def __init__(self, request_timeout: int = 6) -> None:
        self.request_timeout = request_timeout
        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PotatoDeals/3.1"
        )
        self._cache: Dict[str, List[Dict[str, Any]]] = {}

    def _fetch_json(self, url: str) -> Any:
        return fetch_json_with_retry(
            url,
            timeout=self.request_timeout,
            user_agent=self.user_agent,
            max_retries=1,
        )

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            parsed = float(str(value).strip())
        except (TypeError, ValueError):
            return None
        if not (parsed >= 0):
            return None
        # Some APIs can return cents instead of major currency units.
        if parsed > 100000:
            parsed = parsed / 100.0
        return round(parsed, 2)

    def _to_epoch(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            numeric = float(value)
            if numeric <= 0:
                return None
            if numeric > 10_000_000_000:
                return int(round(numeric / 1000.0))
            return int(round(numeric))
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                numeric = float(text)
                if numeric > 10_000_000_000:
                    return int(round(numeric / 1000.0))
                if numeric > 0:
                    return int(round(numeric))
            except ValueError:
                pass
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp())
            except ValueError:
                return None
        return None

    def _walk_points(self, node: Any, out: List[Dict[str, Any]], depth: int = 0) -> None:
        if depth > 8 or node is None:
            return

        if isinstance(node, list):
            if len(node) >= 2 and not isinstance(node[0], (dict, list)) and not isinstance(node[1], (dict, list)):
                ts = self._to_epoch(node[0])
                price = self._safe_float(node[1])
                if ts and price is not None:
                    out.append({"ts": ts, "price": price})
            for child in node:
                self._walk_points(child, out, depth + 1)
            return

        if isinstance(node, dict):
            ts = self._to_epoch(
                node.get("timestamp")
                or node.get("time")
                or node.get("date")
                or node.get("x")
                or node.get("created_at")
            )
            price = self._safe_float(
                node.get("price")
                or node.get("final")
                or node.get("sale_price")
                or node.get("value")
                or node.get("y")
                or node.get("current")
            )
            if ts and price is not None:
                out.append({"ts": ts, "price": price})
            for value in node.values():
                if isinstance(value, (dict, list)):
                    self._walk_points(value, out, depth + 1)

    def _normalize_points(self, points: List[Dict[str, Any]], currency: str) -> List[Dict[str, Any]]:
        by_ts: Dict[int, float] = {}
        for point in points:
            ts = int(point.get("ts", 0) or 0)
            price = self._safe_float(point.get("price"))
            if ts <= 0 or price is None:
                continue
            by_ts[ts] = price

        if not by_ts:
            return []

        sorted_items = sorted(by_ts.items(), key=lambda item: item[0])
        rows: List[Dict[str, Any]] = []
        running_regular: Optional[float] = None
        for ts, price in sorted_items:
            if running_regular is None:
                running_regular = price
            else:
                running_regular = max(running_regular, price)

            regular = running_regular if running_regular is not None else price
            discount = 0
            on_sale = False
            if regular > 0 and price < (regular - 0.01):
                discount = int(round(((regular - price) / regular) * 100))
                discount = max(0, min(discount, 99))
                on_sale = discount > 0

            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                    "price": round(price, 2),
                    "initial_price": round(regular, 2),
                    "currency": currency,
                    "discount_percent": discount,
                    "on_sale": on_sale,
                    "source": "steamdb_unofficial",
                }
            )
        return rows

    def get_price_history(self, appid: int, region: str, currency: str) -> List[Dict[str, Any]]:
        normalized_region = store_country_from_region(region)
        normalized_currency = str(currency or "USD").strip().upper()[:6] or "USD"
        cache_key = f"{appid}:{normalized_region}:{normalized_currency}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        urls = [
            f"https://steamdb.info/api/GetPriceHistory/?appid={appid}&cc={normalized_region}",
            f"https://steamdb.info/api/GetPriceHistory/?appid={appid}",
        ]

        for url in urls:
            try:
                payload = self._fetch_json(url)
            except (RateLimitError, RequestFailedError):
                continue

            raw_points: List[Dict[str, Any]] = []
            self._walk_points(payload, raw_points)
            normalized = self._normalize_points(raw_points, normalized_currency)
            if len(normalized) >= 2:
                self._cache[cache_key] = normalized
                return normalized

        self._cache[cache_key] = []
        return []


class SteamSalesProvider:
    """Fetches live Steam sale events from official Steam store pages."""

    COLLECTION_URL = "https://store.steampowered.com/news/collection/sales"
    HOMEPAGE_URL = "https://store.steampowered.com/sale/"

    FALLBACK_EVENTS = [
        {
            "name": "Steam Spring Sale",
            "description": "Fallback schedule (live sales feed unavailable).",
            "url": "https://store.steampowered.com/sale/spring",
            "start_ts": 0,
            "end_ts": 0,
            "status": "unknown",
            "source": "fallback_calendar",
        },
        {
            "name": "Steam Summer Sale",
            "description": "Fallback schedule (live sales feed unavailable).",
            "url": "https://store.steampowered.com/sale/summer",
            "start_ts": 0,
            "end_ts": 0,
            "status": "unknown",
            "source": "fallback_calendar",
        },
        {
            "name": "Steam Autumn Sale",
            "description": "Fallback schedule (live sales feed unavailable).",
            "url": "https://store.steampowered.com/sale/autumn",
            "start_ts": 0,
            "end_ts": 0,
            "status": "unknown",
            "source": "fallback_calendar",
        },
        {
            "name": "Steam Winter Sale",
            "description": "Fallback schedule (live sales feed unavailable).",
            "url": "https://store.steampowered.com/sale/winter",
            "start_ts": 0,
            "end_ts": 0,
            "status": "unknown",
            "source": "fallback_calendar",
        },
    ]

    def __init__(self, request_timeout: int = 8) -> None:
        self.request_timeout = request_timeout
        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PotatoDeals/3.1"
        )
        self._cache_until = 0.0
        self._cache_result: Dict[str, Any] = {}

    def _fetch_text(self, url: str) -> str:
        return fetch_text_with_retry(
            url,
            timeout=self.request_timeout,
            user_agent=self.user_agent,
            max_retries=1,
        )

    def _extract_app_config_attr(self, html_text: str, attribute: str) -> Optional[str]:
        pattern = re.compile(
            rf'id="application_config"[^>]*\b{re.escape(attribute)}="([^"]+)"',
            re.IGNORECASE | re.DOTALL,
        )
        match = pattern.search(html_text)
        if not match:
            return None
        return html.unescape(match.group(1))

    def _as_dict(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _clean_url(self, raw_url: Any) -> Optional[str]:
        text = str(raw_url or "").strip()
        if not text:
            return None
        text = html.unescape(text).replace("\\/", "/")
        text = text.strip().rstrip(").,;\"'")
        if text.startswith("//"):
            text = "https:" + text
        if text.startswith("/"):
            text = "https://store.steampowered.com" + text

        parsed = urlparse(text)
        if parsed.scheme != "https" or parsed.netloc != "store.steampowered.com":
            return None

        path = parsed.path or "/"
        if path in {"", "/"}:
            return None

        landing_tokens = ("/sale/", "/category/", "/developer/", "/publisher/", "/franchise/")
        if not any(token in path for token in landing_tokens):
            return None

        normalized = urlunparse(("https", "store.steampowered.com", path, "", "", ""))
        return normalized.rstrip("/")

    def _url_candidates_from_body(self, body: str) -> List[str]:
        if not body:
            return []
        # Accept direct sale/category/developer/publisher links from event announcements.
        pattern = re.compile(r"https://store\.steampowered\.com/[^\s\]\"'<>)]+", re.IGNORECASE)
        return [match.group(0) for match in pattern.finditer(body)]

    def _event_status(self, now_ts: int, start_ts: int, end_ts: int) -> str:
        if start_ts > 0 and now_ts < start_ts:
            return "upcoming"
        if start_ts > 0 and end_ts > 0 and start_ts <= now_ts <= end_ts:
            return "active"
        if end_ts > 0 and now_ts > end_ts:
            return "ended"
        return "active"

    def _pick_description(self, data: Dict[str, Any]) -> str:
        for key in ("localized_subtitle", "localized_summary"):
            values = data.get(key)
            if isinstance(values, list):
                for value in values:
                    text = str(value or "").strip()
                    if text:
                        return text
        return ""

    def _resolve_event_url(self, event: Dict[str, Any], jsondata: Dict[str, Any]) -> Optional[str]:
        body = str((event.get("announcement_body") or {}).get("body") or "")
        candidates: List[str] = []
        candidates.extend(self._url_candidates_from_body(body))

        browse_more = str(jsondata.get("sale_browsemore_url") or "").strip()
        if browse_more:
            candidates.append(browse_more)

        vanity = str(jsondata.get("sale_vanity_id") or "").strip()
        if vanity:
            candidates.append(f"https://store.steampowered.com/sale/{vanity}")

        for candidate in candidates:
            cleaned = self._clean_url(candidate)
            if cleaned:
                return cleaned
        return None

    def _parse_collection_events(self, html_text: str) -> List[Dict[str, Any]]:
        raw_initial = self._extract_app_config_attr(html_text, "data-initialEvents")
        if not raw_initial:
            return []

        try:
            initial = json.loads(raw_initial)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []

        rows = initial.get("events")
        if not isinstance(rows, list):
            return []

        now_ts = int(time.time())
        events: List[Dict[str, Any]] = []
        seen_urls = set()

        for row in rows:
            if not isinstance(row, dict):
                continue

            jsondata = self._as_dict(row.get("jsondata"))
            if not jsondata.get("bSaleEnabled", False):
                # Keep only sale-like partner events.
                if int(row.get("event_type", 0) or 0) != 20:
                    continue

            url = self._resolve_event_url(row, jsondata)
            if not url or url in seen_urls:
                continue

            try:
                start_ts = int(row.get("rtime32_start_time", 0) or 0)
            except (TypeError, ValueError):
                start_ts = 0
            try:
                end_ts = int(row.get("rtime32_end_time", 0) or 0)
            except (TypeError, ValueError):
                end_ts = 0

            # Skip events that clearly ended long ago.
            if end_ts > 0 and end_ts < (now_ts - 24 * 3600):
                continue

            name = str((row.get("announcement_body") or {}).get("headline") or row.get("event_name") or "").strip()
            if not name:
                name = "Steam Sale Event"

            description = self._pick_description(jsondata)
            status = self._event_status(now_ts, start_ts, end_ts)

            events.append(
                {
                    "name": name,
                    "description": description,
                    "url": url,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "status": status,
                    "major": bool("next fest" in name.lower() or "steam " in name.lower()),
                    "source": "steam_sales_collection",
                }
            )
            seen_urls.add(url)

        return events

    def _parse_homepage_sales(self, html_text: str, existing_urls: set) -> List[Dict[str, Any]]:
        now_ts = int(time.time())
        events: List[Dict[str, Any]] = []
        # Collect visible sale/category links with optional human title.
        anchor_pattern = re.compile(
            r'<a[^>]+href="([^"]+)"[^>]*(?:aria-label="([^"]+)")?[^>]*>',
            re.IGNORECASE,
        )
        for match in anchor_pattern.finditer(html_text):
            raw_url = match.group(1)
            cleaned = self._clean_url(raw_url)
            if not cleaned or cleaned in existing_urls:
                continue

            title = html.unescape(match.group(2) or "").strip()
            if not title:
                # Derive human-readable title from URL path slug
                path = urlparse(cleaned).path.rstrip("/")
                slug = path.rsplit("/", 1)[-1] if "/" in path else path
                if slug:
                    # Split camelCase/PascalCase: "WargamingPubSale2026" → "Wargaming Pub Sale 2026"
                    spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', slug)
                    spaced = re.sub(r'([A-Za-z])(\d)', r'\1 \2', spaced)
                    spaced = re.sub(r'(\d)([A-Za-z])', r'\1 \2', spaced)
                    spaced = spaced.replace("-", " ").replace("_", " ").strip()
                    title = spaced.title()
                    # For category pages, prefix with "Steam" for clarity
                    if "/category/" in cleaned:
                        title = f"Steam {title} Sale"
                else:
                    title = "Steam Sale Event"

            events.append(
                {
                    "name": title or "Steam Sale Event",
                    "description": "",
                    "url": cleaned,
                    "start_ts": 0,
                    "end_ts": 0,
                    "status": "active",
                    "major": bool("steam" in title.lower()),
                    "source": "steam_sale_homepage",
                }
            )
            existing_urls.add(cleaned)
        return events

    def _fallback_events(self) -> List[Dict[str, Any]]:
        return [dict(item) for item in self.FALLBACK_EVENTS]

    def get_sales_events(self) -> Dict[str, Any]:
        now = time.time()
        now_ts = int(now)
        if self._cache_result and now < self._cache_until:
            return dict(self._cache_result)

        events: List[Dict[str, Any]] = []
        warning = ""
        source = "steam_official_collection"

        try:
            collection_html = self._fetch_text(self.COLLECTION_URL)
            events = self._parse_collection_events(collection_html)
        except (RateLimitError, RequestFailedError) as err:
            warning = f"Live Steam sales feed unavailable ({err}). Showing fallback schedule."
            source = "fallback_calendar"

        existing_urls = {str(event.get("url") or "") for event in events}
        try:
            sale_home_html = self._fetch_text(self.HOMEPAGE_URL)
            events.extend(self._parse_homepage_sales(sale_home_html, existing_urls))
        except (RateLimitError, RequestFailedError):
            pass

        if not events:
            events = self._fallback_events()
            if not warning:
                warning = "Live Steam sales feed unavailable. Showing fallback schedule."
            source = "fallback_calendar"

        order = {"active": 0, "upcoming": 1, "unknown": 2, "ended": 3}
        events.sort(
            key=lambda item: (
                order.get(str(item.get("status") or "unknown"), 2),
                int(item.get("start_ts", 0) or 0),
                str(item.get("name") or ""),
            )
        )

        # All events that have an explicit URL from the Steam API are treated as valid.
        # The previous HEAD-request loop (checking every upcoming event URL) added up to
        # 5×3s = 15s of synchronous blocking per call, which caused visible UI hangs.
        # The frontend already shows a "Page not available yet" label only when
        # url_valid is False, so marking everything True is the safe fallback.
        for event in events:
            url = event.get("url", "")
            event["url_valid"] = bool(url and url != "https://store.steampowered.com/")

        result = {
            "events": events,
            "warning": warning,
            "source": source,
            "fetched_at": utc_now_iso(),
        }
        self._cache_result = result
        self._cache_until = time.time() + 15 * 60
        return dict(result)


class SteamPriceProvider(PriceProvider):
    def __init__(self, request_timeout: int = 10) -> None:
        self.request_timeout = request_timeout
        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PotatoDeals/2.0"
        )
        self.cheapshark_provider = CheapSharkPriceProvider(request_timeout=3)
        self.steamdb_history_provider = SteamDbHistoryProvider(request_timeout=4)

    def _fetch_json(self, url: str) -> Dict[str, Any]:
        return fetch_json_with_retry(
            url,
            timeout=self.request_timeout,
            user_agent=self.user_agent,
            max_retries=1,
        )

    def _safe_currency(self, value: Any, fallback: str = "USD") -> str:
        text = str(value or "").strip().upper()
        if not text or len(text) > 6:
            return fallback
        return text

    def _computed_discount_percent(self, initial_value: float, final_value: float) -> int:
        if initial_value <= 0 or final_value >= (initial_value - 0.01):
            return 0
        discount = int(round(((initial_value - final_value) * 100.0) / initial_value))
        return max(0, min(discount, 99))

    def _resolve_discount_math(
        self,
        initial_value: float,
        final_value: float,
        provider_discount: Any,
    ) -> Dict[str, Any]:
        computed = self._computed_discount_percent(initial_value, final_value)
        consistent = True
        try:
            reported = int(provider_discount)
        except (TypeError, ValueError):
            reported = None
        if reported is not None:
            reported = max(0, min(reported, 99))
            # Steam can differ by 1% because of currency rounding.
            if abs(reported - computed) > 1:
                consistent = False
        return {
            "discount_percent": computed,
            "consistent": consistent,
            "error": None if consistent else "Price data inconsistent",
        }

    def _extract_package_ids(self, data: Dict[str, Any]) -> List[int]:
        package_ids: List[int] = []

        for package_id in data.get("packages", []) if isinstance(data.get("packages"), list) else []:
            try:
                parsed = int(package_id)
            except (TypeError, ValueError):
                continue
            if parsed > 0 and parsed not in package_ids:
                package_ids.append(parsed)

        package_groups = data.get("package_groups")
        if isinstance(package_groups, list):
            for group in package_groups:
                if not isinstance(group, dict):
                    continue
                for sub in group.get("subs", []) if isinstance(group.get("subs"), list) else []:
                    if not isinstance(sub, dict):
                        continue
                    try:
                        parsed = int(sub.get("packageid", 0))
                    except (TypeError, ValueError):
                        continue
                    if parsed > 0 and parsed not in package_ids:
                        package_ids.append(parsed)
        return package_ids

    def _extract_price_from_package_payload(self, package_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        item = payload.get(str(package_id), {}) if payload else {}
        if not item.get("success"):
            return None

        data = item.get("data", {})
        if not isinstance(data, dict):
            return None
        if data.get("is_free_license"):
            return {
                "price": 0.0,
                "initial_price": 0.0,
                "currency": "USD",
                "discount_percent": 0,
                "on_sale": False,
                "price_data_consistent": True,
                "price_data_error": None,
            }

        price_info = data.get("price", {})
        if not isinstance(price_info, dict):
            return None

        final_raw = price_info.get("final")
        if final_raw is None:
            return None
        try:
            final_value = round(float(final_raw) / 100.0, 2)
        except (TypeError, ValueError):
            return None

        initial_raw = price_info.get("initial", final_raw)
        try:
            initial_value = round(float(initial_raw) / 100.0, 2)
        except (TypeError, ValueError):
            initial_value = final_value
        if initial_value < final_value:
            initial_value = final_value

        discount_math = self._resolve_discount_math(
            initial_value=initial_value,
            final_value=final_value,
            provider_discount=price_info.get("discount_percent", 0),
        )
        discount_percent = int(discount_math["discount_percent"])
        consistent = bool(discount_math["consistent"])

        return {
            "price": final_value,
            "initial_price": initial_value,
            "currency": self._safe_currency(price_info.get("currency"), fallback="USD"),
            "discount_percent": discount_percent,
            "on_sale": discount_percent > 0 and consistent,
            "price_data_consistent": consistent,
            "price_data_error": discount_math["error"],
        }

    def _fetch_price_from_packages(self, package_ids: List[int], region: str) -> Optional[Dict[str, Any]]:
        if not package_ids:
            return None
        store_country = store_country_from_region(region)
        for package_id in package_ids[:3]:
            package_url = (
                f"https://store.steampowered.com/api/packagedetails"
                f"?packageids={package_id}&cc={store_country}&l=en"
            )
            try:
                package_payload = self._fetch_json(package_url)
            except (RateLimitError, RequestFailedError):
                continue
            parsed = self._extract_price_from_package_payload(package_id, package_payload)
            if parsed:
                return parsed
        return None

    def _extract_price(self, appid: int, payload: Dict[str, Any], region: str) -> Optional[Dict[str, Any]]:
        item = payload.get(str(appid), {}) if payload else {}
        if not item.get("success"):
            return None

        data = item.get("data", {})
        if not isinstance(data, dict):
            return None

        release_raw = data.get("release_date") or {}
        release_date = ""
        is_released = True
        if isinstance(release_raw, dict):
            release_date = str(release_raw.get("date") or "").strip()
            is_released = not bool(release_raw.get("coming_soon"))
        elif isinstance(release_raw, str):
            release_date = release_raw.strip()

        title = str(data.get("name") or "").strip() or None
        capsule_url = (
            str(data.get("capsule_imagev5") or "").strip()
            or str(data.get("header_image") or "").strip()
            or str(data.get("capsule_image") or "").strip()
            or None
        )
        store_url = f"https://store.steampowered.com/app/{appid}/"

        metacritic_score = None
        metacritic_raw = data.get("metacritic") or {}
        if isinstance(metacritic_raw, dict):
            try:
                metacritic_score = int(metacritic_raw.get("score", 0)) or None
            except (TypeError, ValueError):
                metacritic_score = None

        if data.get("is_free"):
            return {
                "price": 0.0,
                "initial_price": 0.0,
                "currency": "USD",
                "discount_percent": 0,
                "on_sale": False,
                "price_data_consistent": True,
                "price_data_error": None,
                "title": title,
                "metacritic_score": metacritic_score,
                "release_date": release_date or None,
                "is_released": is_released,
                "capsule_url": capsule_url,
                "store_url": store_url,
                "retrieved_at": utc_now_iso(),
            }

        price_overview = data.get("price_overview")
        provider_discount: Any = 0
        price_consistent = True
        price_error: Optional[str] = None
        if price_overview:
            try:
                final_value = float(price_overview.get("final", 0)) / 100.0
            except (TypeError, ValueError):
                final_value = 0.0
            try:
                initial_value = float(price_overview.get("initial", price_overview.get("final", 0))) / 100.0
            except (TypeError, ValueError):
                initial_value = final_value
            provider_discount = price_overview.get("discount_percent", 0)
            currency = self._safe_currency(price_overview.get("currency"), fallback="USD")
        else:
            package_price = self._fetch_price_from_packages(self._extract_package_ids(data), region)
            if not package_price:
                return {
                    "price": None,
                    "initial_price": None,
                    "currency": None,
                    "discount_percent": 0,
                    "on_sale": False,
                    "price_data_consistent": True,
                    "price_data_error": None,
                    "title": title,
                    "metacritic_score": metacritic_score,
                    "release_date": release_date or None,
                    "is_released": is_released,
                    "capsule_url": capsule_url,
                    "store_url": store_url,
                    "retrieved_at": utc_now_iso(),
                }
            try:
                final_value = float(package_price.get("price", 0))
            except (TypeError, ValueError):
                final_value = 0.0
            try:
                initial_value = float(package_price.get("initial_price", final_value))
            except (TypeError, ValueError):
                initial_value = final_value
            provider_discount = package_price.get("discount_percent", 0)
            price_consistent = bool(package_price.get("price_data_consistent", True))
            price_error = package_price.get("price_data_error")
            currency = self._safe_currency(package_price.get("currency"), fallback="USD")
        if initial_value < final_value:
            initial_value = final_value

        discount_math = self._resolve_discount_math(
            initial_value=initial_value,
            final_value=final_value,
            provider_discount=provider_discount,
        )
        discount_percent = int(discount_math["discount_percent"])
        price_consistent = price_consistent and bool(discount_math["consistent"])
        if price_error is None:
            price_error = discount_math["error"]

        return {
            "price": round(final_value, 2),
            "initial_price": round(initial_value, 2),
            "currency": currency,
            "discount_percent": discount_percent,
            "on_sale": discount_percent > 0 and price_consistent,
            "price_data_consistent": price_consistent,
            "price_data_error": price_error,
            "title": title,
            "metacritic_score": metacritic_score,
            "release_date": release_date or None,
            "is_released": is_released,
            "capsule_url": capsule_url,
            "store_url": store_url,
            "retrieved_at": utc_now_iso(),
        }

    def _extract_title(self, appid: int, payload: Dict[str, Any]) -> Optional[str]:
        item = payload.get(str(appid), {}) if payload else {}
        if not item.get("success"):
            return None
        data = item.get("data", {})
        title = str(data.get("name") or "").strip()
        return title or None

    def _extract_basic_title(self, appid: int, payload: Dict[str, Any]) -> Optional[str]:
        item = payload.get(str(appid), {}) if payload else {}
        if not item.get("success"):
            return None
        data = item.get("data", {})
        if not isinstance(data, dict):
            return None
        title = str(data.get("name") or "").strip()
        return title or None

    def _record_error(self, errors: Dict[str, int], code: str) -> None:
        key = str(code or "unknown_error").strip() or "unknown_error"
        errors[key] = int(errors.get(key, 0)) + 1

    def _format_error_detail(self, errors: Dict[str, int]) -> str:
        if not errors:
            return ""
        ordered = sorted(errors.items(), key=lambda item: (-item[1], item[0]))
        return ";".join(f"{code}:{count}" for code, count in ordered[:6])

    def getCurrentPrices(self, appids: List[int], region: str) -> Dict[str, Any]:
        if not appids:
            return {"prices": {}, "titles": {}, "rate_limited": False, "rate_limited_ids": [], "error_detail": ""}

        results: Dict[int, Optional[Dict[str, Any]]] = {}
        titles: Dict[int, str] = {}
        rate_limited_ids = set()
        rate_limited = False
        error_counts: Dict[str, int] = {}
        for index in range(0, len(appids), 1):
            chunk = appids[index : index + 1]
            appids_query = ",".join(str(appid) for appid in chunk)
            store_country = store_country_from_region(region)
            url = f"https://store.steampowered.com/api/appdetails?appids={appids_query}&cc={store_country}&l=en"
            try:
                payload = self._fetch_json(url)
            except RateLimitError:
                rate_limited = True
                rate_limited_ids.update(chunk)
                self._record_error(error_counts, "rate_limited")
                for appid in chunk:
                    results[appid] = None
                continue
            except RequestFailedError as err:
                self._record_error(error_counts, str(err))
                for appid in chunk:
                    results[appid] = None
                    basic_url = (
                        f"https://store.steampowered.com/api/appdetails"
                        f"?appids={appid}&cc={store_country}&l=en&filters=basic"
                    )
                    try:
                        basic_payload = self._fetch_json(basic_url)
                        basic_title = self._extract_basic_title(appid, basic_payload)
                        if basic_title:
                            titles[appid] = basic_title
                    except RateLimitError:
                        rate_limited = True
                        rate_limited_ids.add(appid)
                        self._record_error(error_counts, "rate_limited")
                    except RequestFailedError as fallback_err:
                        self._record_error(error_counts, str(fallback_err))
                continue

            if not isinstance(payload, dict):
                self._record_error(error_counts, "invalid_appdetails_payload")
                for appid in chunk:
                    results[appid] = None
                    basic_url = (
                        f"https://store.steampowered.com/api/appdetails"
                        f"?appids={appid}&cc={store_country}&l=en&filters=basic"
                    )
                    try:
                        basic_payload = self._fetch_json(basic_url)
                        basic_title = self._extract_basic_title(appid, basic_payload)
                        if basic_title:
                            titles[appid] = basic_title
                    except RateLimitError:
                        rate_limited = True
                        rate_limited_ids.add(appid)
                        self._record_error(error_counts, "rate_limited")
                    except RequestFailedError as fallback_err:
                        self._record_error(error_counts, str(fallback_err))
                continue

            for appid in chunk:
                results[appid] = self._extract_price(appid, payload, region)
                title = self._extract_title(appid, payload)
                if title:
                    titles[appid] = title
                if results[appid] is not None:
                    continue
                # Fallback for edge cases where batch response misses an app.
                single_url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc={store_country}&l=en"
                try:
                    single_payload = self._fetch_json(single_url)
                    results[appid] = self._extract_price(appid, single_payload, region)
                    fallback_title = self._extract_title(appid, single_payload)
                    if fallback_title:
                        titles[appid] = fallback_title
                except RateLimitError:
                    rate_limited = True
                    rate_limited_ids.add(appid)
                    self._record_error(error_counts, "rate_limited")
                    results[appid] = None
                except RequestFailedError as err:
                    self._record_error(error_counts, str(err))
                    results[appid] = None
                    basic_url = (
                        f"https://store.steampowered.com/api/appdetails"
                        f"?appids={appid}&cc={store_country}&l=en&filters=basic"
                    )
                    try:
                        basic_payload = self._fetch_json(basic_url)
                        basic_title = self._extract_basic_title(appid, basic_payload)
                        if basic_title:
                            titles[appid] = basic_title
                    except RateLimitError:
                        rate_limited = True
                        rate_limited_ids.add(appid)
                        self._record_error(error_counts, "rate_limited")
                    except RequestFailedError as fallback_err:
                        self._record_error(error_counts, str(fallback_err))
                if not titles.get(appid):
                    basic_url = (
                        f"https://store.steampowered.com/api/appdetails"
                        f"?appids={appid}&cc={store_country}&l=en&filters=basic"
                    )
                    try:
                        basic_payload = self._fetch_json(basic_url)
                        basic_title = self._extract_basic_title(appid, basic_payload)
                        if basic_title:
                            titles[appid] = basic_title
                    except RateLimitError:
                        rate_limited = True
                        rate_limited_ids.add(appid)
                        self._record_error(error_counts, "rate_limited")
                    except RequestFailedError as fallback_err:
                        self._record_error(error_counts, str(fallback_err))

            time.sleep(0.2)

        return {
            "prices": results,
            "titles": titles,
            "rate_limited": rate_limited,
            "rate_limited_ids": sorted(rate_limited_ids),
            "error_detail": self._format_error_detail(error_counts),
        }

    def getCurrentPrice(self, appid: int, region: str) -> Optional[Dict[str, Any]]:
        result = self.getCurrentPrices([appid], region)
        prices = result.get("prices", {})
        return prices.get(appid)

    def getPriceHistory(self, game_cache: Dict[str, Any]) -> List[Dict[str, Any]]:
        history = game_cache.get("history", [])
        if not isinstance(history, list):
            return []
        return history

    def _normalize_history_point(self, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None
        price_raw = raw.get("price")
        if price_raw is None:
            return None
        try:
            price = round(float(price_raw), 2)
        except (TypeError, ValueError):
            return None
        if price < 0:
            return None

        initial_raw = raw.get("initial_price", price)
        try:
            initial_price = round(float(initial_raw), 2)
        except (TypeError, ValueError):
            initial_price = price
        if initial_price < price:
            initial_price = price

        ts = str(raw.get("timestamp") or "").strip()
        if not ts:
            return None

        currency = self._safe_currency(raw.get("currency"), fallback="USD")
        try:
            discount = int(raw.get("discount_percent", 0))
        except (TypeError, ValueError):
            discount = 0
        discount = max(0, min(discount, 99))
        on_sale = bool(raw.get("on_sale")) or discount > 0 or (initial_price > price + 0.01)

        normalized = {
            "timestamp": ts,
            "price": price,
            "initial_price": initial_price,
            "currency": currency,
            "discount_percent": discount,
            "on_sale": on_sale,
        }
        source = str(raw.get("source") or "").strip()
        if source:
            normalized["source"] = source
        return normalized

    def _merge_history(
        self,
        current_history: List[Dict[str, Any]],
        external_history: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        by_timestamp: Dict[str, Dict[str, Any]] = {}

        for point in current_history:
            normalized = self._normalize_history_point(point)
            if normalized:
                by_timestamp[normalized["timestamp"]] = normalized

        for point in external_history:
            normalized = self._normalize_history_point(point)
            if not normalized:
                continue
            ts = normalized["timestamp"]
            existing = by_timestamp.get(ts)
            if existing is None:
                by_timestamp[ts] = normalized
                continue
            existing_source = str(existing.get("source") or "")
            incoming_source = str(normalized.get("source") or "")
            # Prefer ITAD/SteamDB points over local snapshots when timestamps collide.
            if (incoming_source == "itad") or (
                incoming_source == "steamdb_unofficial" and existing_source not in {"itad", "steamdb_unofficial"}
            ):
                by_timestamp[ts] = normalized

        def _parse_ts(value: str) -> datetime:
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                return datetime.min.replace(tzinfo=timezone.utc)

        merged = sorted(by_timestamp.values(), key=lambda item: _parse_ts(item["timestamp"]))
        if len(merged) > 400:
            merged = merged[-400:]
        return merged

    def _history_last_six_months(self, history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=180)
        result: List[Dict[str, Any]] = []
        for point in history:
            ts_raw = str(point.get("timestamp") or "").strip()
            if not ts_raw:
                continue
            try:
                parsed = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if parsed >= cutoff:
                result.append(point)
        return result

    def _derive_history_metrics(self, game_cache: Dict[str, Any]) -> None:
        history = self.getPriceHistory(game_cache)
        verified = self._history_last_six_months(history)

        if len(verified) < 2:
            game_cache["history_6m"] = []
            game_cache["last_sale"] = None
        else:
            game_cache["history_6m"] = verified
            last_sale = None
            for point in reversed(verified):
                if point.get("on_sale"):
                    last_sale = point
                    break
            game_cache["last_sale"] = last_sale

        if not history:
            game_cache["all_time_low"] = None
            game_cache["all_time_high"] = None
            game_cache["all_time_low_verified"] = False
            return

        best_low = min(history, key=lambda item: item.get("price", float("inf")))
        best_high = max(history, key=lambda item: item.get("initial_price", item.get("price", 0)))

        game_cache["all_time_low"] = {
            "price": best_low.get("price"),
            "currency": best_low.get("currency"),
            "timestamp": best_low.get("timestamp"),
        }
        game_cache["all_time_high"] = {
            "price": best_high.get("initial_price", best_high.get("price")),
            "currency": best_high.get("currency"),
            "timestamp": best_high.get("timestamp"),
        }
        # Only mark as verified if we have non-synthetic history
        has_real = any(
            str(p.get("source", "")) not in ("synthetic_baseline", "current_snapshot")
            for p in history
        )
        game_cache["all_time_low_verified"] = has_real

    def getLastSale(self, game_cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        history = game_cache.get("history_6m", [])
        if not isinstance(history, list) or not history:
            history = self.getPriceHistory(game_cache)
        for item in reversed(history):
            if item.get("on_sale"):
                return item
        return None

    def getAllTimeLow(self, game_cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        history = self.getPriceHistory(game_cache)
        if not history:
            return None
        best = min(history, key=lambda x: x.get("price", float("inf")))
        return {
            "price": best.get("price"),
            "currency": best.get("currency"),
            "timestamp": best.get("timestamp"),
        }

    def getAllTimeHigh(self, game_cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        history = self.getPriceHistory(game_cache)
        if not history:
            return None
        best = max(history, key=lambda x: x.get("initial_price", x.get("price", 0)))
        return {
            "price": best.get("initial_price", best.get("price")),
            "currency": best.get("currency"),
            "timestamp": best.get("timestamp"),
        }

    def _generate_fallback_history(self, game_cache: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate minimal chart data from current price when real history is unavailable."""
        current = game_cache.get("current") or {}
        price_raw = current.get("price")
        if price_raw is None:
            return []
        try:
            price = round(float(price_raw), 2)
        except (TypeError, ValueError):
            return []
        initial_raw = current.get("initial_price", price_raw)
        try:
            initial = round(float(initial_raw), 2)
        except (TypeError, ValueError):
            initial = price
        if initial < price:
            initial = price
        currency = self._safe_currency(current.get("currency"), fallback="USD")
        now = datetime.now(timezone.utc)
        points: List[Dict[str, Any]] = []
        # Add initial "regular price" point 6 months ago
        if initial > price + 0.01:
            points.append({
                "timestamp": (now - timedelta(days=180)).isoformat(),
                "price": initial,
                "initial_price": initial,
                "currency": currency,
                "discount_percent": 0,
                "on_sale": False,
                "source": "synthetic_baseline",
            })
        # Mid-point at 3 months ago with initial price
        points.append({
            "timestamp": (now - timedelta(days=90)).isoformat(),
            "price": initial,
            "initial_price": initial,
            "currency": currency,
            "discount_percent": 0,
            "on_sale": False,
            "source": "synthetic_baseline",
        })
        # Current price point
        discount = int(current.get("discount_percent", 0))
        points.append({
            "timestamp": now.isoformat(),
            "price": price,
            "initial_price": initial,
            "currency": currency,
            "discount_percent": discount,
            "on_sale": bool(current.get("on_sale")),
            "source": "current_snapshot",
        })
        return points

    def update_prices(self, appids: List[int], region: str, cache: Dict[str, Any]) -> Dict[str, Any]:
        updated = 0
        failed = 0
        batch = self.getCurrentPrices(appids, region)
        batch_prices = batch.get("prices", {})
        titles = batch.get("titles", {})
        rate_limited = bool(batch.get("rate_limited", False))
        rate_limited_ids = set(batch.get("rate_limited_ids", []))
        error_detail = str(batch.get("error_detail", "") or "").strip()
        fallback_used = 0
        steamdb_history_backfilled = 0

        # CheapShark is used only as a metadata fallback (e.g., metacritic).
        cs_game_details: Dict[int, Dict[str, Any]] = {}
        try:
            cs_game_details = self.cheapshark_provider.fetch_game_details_batch(appids)
        except Exception:
            pass  # Non-critical; proceed without enrichment

        for appid in appids:
            key = build_price_cache_key(appid, region)
            current = batch_prices.get(appid)
            if current is None:
                failed += 1
                game_cache = cache.setdefault(key, {})
                if appid in rate_limited_ids:
                    game_cache["last_error"] = "price_rate_limited"
                else:
                    game_cache["last_error"] = "missing_price_data"
                continue

            game_cache = cache.setdefault(key, {})
            # Preserve user preferences from older cache layouts.
            legacy_cache = cache.get(str(appid))
            if isinstance(legacy_cache, dict):
                if "notify_enabled" in legacy_cache and "notify_enabled" not in game_cache:
                    game_cache["notify_enabled"] = legacy_cache.get("notify_enabled")
                if "pinned" in legacy_cache and "pinned" not in game_cache:
                    game_cache["pinned"] = legacy_cache.get("pinned")

            raw_history = game_cache.get("history")
            if isinstance(raw_history, list):
                history = raw_history
            else:
                history = []
                game_cache["history"] = history

            game_cache["current"] = current

            if current.get("release_date"):
                game_cache["release_date"] = current.get("release_date")
            game_cache["is_released"] = bool(current.get("is_released", True))
            if current.get("capsule_url"):
                game_cache["capsule_url"] = current.get("capsule_url")
            if current.get("store_url"):
                game_cache["store_url"] = current.get("store_url")

            cs_details = cs_game_details.get(appid) or {}
            cs_metacritic = cs_details.get("metacritic_score")
            metacritic = current.get("metacritic_score")
            if not metacritic and cs_metacritic:
                metacritic = cs_metacritic
            if metacritic:
                game_cache["metacritic_score"] = metacritic

            current_price = current.get("price")
            if current_price is not None:
                try:
                    parsed_current = round(float(current_price), 2)
                except (TypeError, ValueError):
                    parsed_current = None
                try:
                    parsed_initial = round(float(current.get("initial_price", current_price)), 2)
                except (TypeError, ValueError):
                    parsed_initial = parsed_current
                if parsed_current is None:
                    parsed_current = None
                if parsed_current is not None and parsed_initial is not None and parsed_initial < parsed_current:
                    parsed_initial = parsed_current
                if parsed_current is None:
                    continue

                snapshot = {
                    "timestamp": current.get("retrieved_at", utc_now_iso()),
                    "price": parsed_current,
                    "initial_price": parsed_initial if parsed_initial is not None else parsed_current,
                    "currency": self._safe_currency(current.get("currency"), fallback="USD"),
                    "discount_percent": int(current.get("discount_percent", 0)),
                    "on_sale": bool(current.get("on_sale")),
                    "source": "local_tracking",
                }
                last_snapshot = self._normalize_history_point(history[-1]) if history else None
                is_new = not last_snapshot or any(
                    last_snapshot.get(field) != snapshot.get(field)
                    for field in ("price", "initial_price", "currency", "discount_percent", "on_sale")
                )
                if is_new:
                    history.append(snapshot)
                if len(history) > 220:
                    del history[0 : len(history) - 220]

            merged_history = self._merge_history(history, [])
            history_6m = self._history_last_six_months(merged_history)

            if len(history_6m) < 2:
                steamdb_history = self.steamdb_history_provider.get_price_history(
                    appid=appid,
                    region=region,
                    currency=str(current.get("currency") or "USD"),
                )
                if steamdb_history:
                    merged_candidate = self._merge_history(merged_history, steamdb_history)
                    if len(merged_candidate) > len(merged_history):
                        merged_history = merged_candidate
                        steamdb_history_backfilled += 1
                        game_cache["history_source"] = "steamdb_unofficial"

            game_cache["history"] = merged_history
            self._derive_history_metrics(game_cache)

            # If still not enough history for chart, generate synthetic fallback
            if len(game_cache.get("history_6m", [])) < 2:
                fallback = self._generate_fallback_history(game_cache)
                if fallback:
                    merged_history = self._merge_history(merged_history, fallback)
                    game_cache["history"] = merged_history
                    self._derive_history_metrics(game_cache)
                    if not game_cache.get("history_source") or game_cache["history_source"] == "none":
                        game_cache["history_source"] = "synthetic_fallback"

            if not game_cache.get("history_source"):
                if len(game_cache.get("history_6m", [])) >= 2:
                    game_cache["history_source"] = "local_tracking"
                else:
                    game_cache["history_source"] = "none"

            game_cache["last_error"] = None
            game_cache.setdefault("notify_enabled", False)
            game_cache.setdefault("pinned", False)
            updated += 1

        return {
            "updated": updated,
            "failed": failed,
            "rate_limited": rate_limited,
            "titles": titles,
            "error_detail": error_detail,
            "fallback_used": fallback_used,
            "steamdb_history_backfilled": steamdb_history_backfilled,
        }


class CheapSharkPriceProvider:
    def __init__(self, request_timeout: int = 3) -> None:
        self.request_timeout = request_timeout
        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PotatoDeals/2.0"
        )

    def _fetch_json(self, url: str) -> Any:
        return fetch_json_with_retry(
            url,
            timeout=self.request_timeout,
            user_agent=self.user_agent,
            max_retries=0,
        )

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def _record_error(self, errors: Dict[str, int], code: str) -> None:
        key = str(code or "unknown_error").strip() or "unknown_error"
        errors[key] = int(errors.get(key, 0)) + 1

    def _format_error_detail(self, errors: Dict[str, int]) -> str:
        if not errors:
            return ""
        ordered = sorted(errors.items(), key=lambda item: (-item[1], item[0]))
        return ";".join(f"{code}:{count}" for code, count in ordered[:6])

    def _pick_best_deal(self, deals: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        valid_deals = [deal for deal in deals if self._safe_float(deal.get("salePrice")) is not None]
        if not valid_deals:
            return None
        steam_deals = [deal for deal in valid_deals if str(deal.get("storeID", "")).strip() == "1"]
        pool = steam_deals if steam_deals else valid_deals
        return min(pool, key=lambda item: self._safe_float(item.get("salePrice")) or 10_000_000.0)

    def _build_price(self, deal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        sale = self._safe_float(deal.get("salePrice"))
        if sale is None:
            return None
        normal = self._safe_float(deal.get("normalPrice"))
        if normal is None or normal < sale:
            normal = sale

        savings = self._safe_float(deal.get("savings"))
        if savings is not None:
            discount = int(round(max(0.0, min(savings, 99.0))))
        else:
            discount = int(round(((normal - sale) / normal) * 100)) if normal > 0 else 0
        discount = max(0, min(discount, 99))

        title = str(deal.get("title") or "").strip()

        # Extract Metacritic score from CheapShark deal
        metacritic_score = self._safe_int(deal.get("metacriticScore"))
        if metacritic_score is not None and metacritic_score <= 0:
            metacritic_score = None

        return {
            "price": round(sale, 2),
            "initial_price": round(normal, 2),
            "currency": "USD",
            "discount_percent": discount,
            "on_sale": discount > 0,
            "title": title or None,
            "metacritic_score": metacritic_score,
            "retrieved_at": utc_now_iso(),
        }

    def _timestamp_from_unix(self, value: Any) -> str:
        parsed = self._safe_int(value)
        if parsed is None or parsed <= 0:
            return utc_now_iso()
        try:
            return datetime.fromtimestamp(parsed, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return utc_now_iso()

    def _fetch_game_details(self, game_id: str, cache: Dict[str, Optional[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
        """Fetch CheapShark game details: ATL, metacritic, retailPrice."""
        key = str(game_id or "").strip()
        if not key:
            return None
        if key in cache:
            return cache[key]

        try:
            payload = self._fetch_json(f"https://www.cheapshark.com/api/1.0/games?id={key}")
        except (RateLimitError, RequestFailedError):
            cache[key] = None
            return None

        if not isinstance(payload, dict):
            cache[key] = None
            return None

        result: Dict[str, Any] = {}

        # cheapestPriceEver
        cheapest = payload.get("cheapestPriceEver", {})
        if isinstance(cheapest, dict):
            price = self._safe_float(cheapest.get("price"))
            if price is not None:
                result["cheapest_price_ever"] = {
                    "price": round(price, 2),
                    "currency": "USD",
                    "timestamp": self._timestamp_from_unix(cheapest.get("date")),
                }

        # Extract retailPrice and metacritic from deals
        deals = payload.get("deals", [])
        if isinstance(deals, list) and deals:
            # Get retail price from first deal
            first_deal = deals[0] if isinstance(deals[0], dict) else {}
            retail = self._safe_float(first_deal.get("retailPrice"))
            if retail is not None:
                result["retail_price"] = round(retail, 2)

        cache[key] = result if result else None
        return result if result else None

    def _resolve_game_id(self, appid: int, cache: Dict[int, Optional[str]]) -> Optional[str]:
        """Look up CheapShark gameID for a Steam appid."""
        if appid in cache:
            return cache[appid]
        try:
            payload = self._fetch_json(f"https://www.cheapshark.com/api/1.0/games?steamAppID={appid}")
        except (RateLimitError, RequestFailedError):
            cache[appid] = None
            return None
        if isinstance(payload, list) and payload:
            game_id = str(payload[0].get("gameID", "")).strip()
            cache[appid] = game_id or None
            return game_id or None
        cache[appid] = None
        return None

    def fetch_game_details_batch(self, appids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Fetch CheapShark game details (ATL + metacritic) for a batch of appids."""
        results: Dict[int, Dict[str, Any]] = {}
        game_id_cache: Dict[int, Optional[str]] = {}
        details_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        start_ts = time.monotonic()
        max_budget = 15.0

        # First, get deals to find gameIDs and metacritic
        for appid in appids:
            if time.monotonic() - start_ts > max_budget:
                break
            url = f"https://www.cheapshark.com/api/1.0/deals?steamAppID={appid}&pageSize=5"
            try:
                payload = self._fetch_json(url)
            except (RateLimitError, RequestFailedError):
                continue
            deals = payload if isinstance(payload, list) else []
            if not deals:
                continue
            first_deal = deals[0] if isinstance(deals[0], dict) else {}
            game_id = str(first_deal.get("gameID", "")).strip()
            if game_id:
                game_id_cache[appid] = game_id

            # Extract metacritic from deals
            mc = self._safe_int(first_deal.get("metacriticScore"))
            if mc and mc > 0:
                results.setdefault(appid, {})["metacritic_score"] = mc

            time.sleep(0.03)

        # Then fetch game details (ATL) for each unique gameID
        seen_game_ids: Dict[str, Dict[str, Any]] = {}
        for appid in appids:
            if time.monotonic() - start_ts > max_budget:
                break
            game_id = game_id_cache.get(appid)
            if not game_id:
                continue
            if game_id in seen_game_ids:
                # Reuse cached result
                results.setdefault(appid, {}).update(seen_game_ids[game_id])
                continue

            details = self._fetch_game_details(game_id, details_cache)
            if details:
                seen_game_ids[game_id] = details
                results.setdefault(appid, {}).update(details)
            time.sleep(0.03)

        return results

    def getCurrentPrices(self, appids: List[int]) -> Dict[str, Any]:
        if not appids:
            return {
                "prices": {},
                "titles": {},
                "all_time_low_hints": {},
                "rate_limited": False,
                "rate_limited_ids": [],
                "error_detail": "",
            }

        start_ts = time.monotonic()
        max_budget_seconds = 22.0
        prices: Dict[int, Optional[Dict[str, Any]]] = {}
        titles: Dict[int, str] = {}
        low_hints: Dict[int, Optional[Dict[str, Any]]] = {}
        error_counts: Dict[str, int] = {}
        rate_limited = False
        rate_limited_ids = set()
        details_cache: Dict[str, Optional[Dict[str, Any]]] = {}

        for appid in appids:
            if time.monotonic() - start_ts > max_budget_seconds:
                self._record_error(error_counts, "fallback_time_budget_exceeded")
                prices[appid] = None
                continue

            url = f"https://www.cheapshark.com/api/1.0/deals?steamAppID={appid}&pageSize=12"
            try:
                payload = self._fetch_json(url)
            except RateLimitError:
                rate_limited = True
                rate_limited_ids.add(appid)
                self._record_error(error_counts, "rate_limited")
                prices[appid] = None
                continue
            except RequestFailedError as err:
                self._record_error(error_counts, str(err))
                prices[appid] = None
                continue

            deals = payload if isinstance(payload, list) else []
            if not deals:
                prices[appid] = None
                continue

            best = self._pick_best_deal([deal for deal in deals if isinstance(deal, dict)])
            if not best:
                prices[appid] = None
                continue

            built = self._build_price(best)
            prices[appid] = built
            if built and built.get("title"):
                titles[appid] = str(built["title"])

            game_id = str(best.get("gameID", "")).strip()
            if game_id:
                details = self._fetch_game_details(game_id, details_cache)
                if details and details.get("cheapest_price_ever"):
                    low_hints[appid] = details["cheapest_price_ever"]

            time.sleep(0.05)

        return {
            "prices": prices,
            "titles": titles,
            "all_time_low_hints": low_hints,
            "rate_limited": rate_limited,
            "rate_limited_ids": sorted(rate_limited_ids),
            "error_detail": self._format_error_detail(error_counts),
        }


class ExchangeRateProvider:
    def __init__(self, request_timeout: int = 10) -> None:
        self.request_timeout = request_timeout
        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PotatoDeals/2.0"
        )

    def _fetch_json(self, url: str) -> Dict[str, Any]:
        return fetch_json_with_retry(
            url,
            timeout=self.request_timeout,
            user_agent=self.user_agent,
            max_retries=1,
        )

    def update_rates(
        self,
        existing_rates: Dict[str, Any],
        force: bool = False,
        max_age_hours: int = 6,
    ) -> Dict[str, Any]:
        updated_at = existing_rates.get("updated_at")
        if updated_at and not force:
            try:
                updated_dt = datetime.fromisoformat(updated_at)
                if datetime.now(timezone.utc) - updated_dt < timedelta(hours=max_age_hours):
                    return {
                        "cache": existing_rates,
                        "from_cache": True,
                        "error": None,
                    }
            except ValueError:
                pass

        url = "https://open.er-api.com/v6/latest/USD"
        try:
            payload = self._fetch_json(url)
            if payload.get("result") != "success":
                raise RuntimeError("failed_exchange_rate_response")
            rates = payload.get("rates", {})
            rates["USD"] = 1.0
            cache = {
                "base": "USD",
                "rates": rates,
                "updated_at": utc_now_iso(),
            }
            return {
                "cache": cache,
                "from_cache": False,
                "error": None,
            }
        except RateLimitError:
            return {
                "cache": existing_rates,
                "from_cache": bool(existing_rates.get("rates")),
                "error": "exchange_rates_rate_limited",
            }
        except Exception:
            return {
                "cache": existing_rates,
                "from_cache": bool(existing_rates.get("rates")),
                "error": "exchange_rates_unavailable",
            }
