import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, cast

from .itad_provider import ITADHistoryProvider
from .providers import (
    ExchangeRateProvider,
    SteamPriceProvider,
    SteamSalesProvider,
    build_price_cache_key,
    store_country_from_region,
)
from .steam_local import detect_steam_id, detect_steam_region
from .store import DataStore, utc_now_iso
from .wishlist import WishlistProvider

logger = logging.getLogger("potato_deals.service")


class PotatoDealsService:
    def __init__(self, base_dir: str) -> None:
        self.store = DataStore(base_dir)
        self.price_provider = SteamPriceProvider()
        self.sales_provider = SteamSalesProvider()
        self.wishlist_provider = WishlistProvider()
        self.exchange_provider = ExchangeRateProvider()
        self.itad_provider = ITADHistoryProvider()

    def _load(self) -> Dict[str, Any]:
        data = self.store.load()
        changed = self._sanitize_state(data)
        if changed:
            data = self.store.save(data)
        return data

    def _save(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self.store.save(data)

    def _sanitize_state(self, data: Dict[str, Any]) -> bool:
        changed = False

        raw_wishlist = data.get("wishlist")
        if not isinstance(raw_wishlist, list):
            data["wishlist"] = []
            raw_wishlist = []
            changed = True

        normalized_wishlist: List[Dict[str, Any]] = []
        seen_ids = set()
        for fallback_order, raw_game in enumerate(raw_wishlist, start=1):
            if not isinstance(raw_game, dict):
                changed = True
                continue
            try:
                appid = int(raw_game.get("appid", 0))
            except (TypeError, ValueError):
                changed = True
                continue
            if appid <= 0 or appid in seen_ids:
                changed = True
                continue
            seen_ids.add(appid)
            title = str(raw_game.get("title") or "").strip() or f"App {appid}"
            try:
                order = int(raw_game.get("order", fallback_order))
            except (TypeError, ValueError):
                order = fallback_order
                changed = True
            if order <= 0:
                order = fallback_order
                changed = True
            normalized_wishlist.append(
                {
                    "appid": appid,
                    "title": title,
                    "order": order,
                }
            )

        normalized_wishlist.sort(key=lambda item: item.get("order", 0))
        for index, game in enumerate(normalized_wishlist, start=1):
            if game.get("order") != index:
                game["order"] = index
                changed = True

        if normalized_wishlist != raw_wishlist:
            data["wishlist"] = normalized_wishlist
            changed = True

        valid_ids = {str(game["appid"]) for game in normalized_wishlist}

        raw_cache_any = data.get("price_cache")
        if not isinstance(raw_cache_any, dict):
            raw_cache: Dict[str, Any] = {}
            data["price_cache"] = raw_cache
            changed = True
        else:
            raw_cache = cast(Dict[str, Any], raw_cache_any)

        clean_cache: Dict[str, Any] = {}
        for key, value in raw_cache.items():
            appid = self._extract_appid_from_cache_key(str(key))
            if appid is None or str(appid) not in valid_ids or not isinstance(value, dict):
                changed = True
                continue

            normalized_cache: Dict[str, Any] = dict(value)
            history = normalized_cache.get("history", [])
            if not isinstance(history, list):
                normalized_cache["history"] = []
                changed = True

            for field in ("current", "last_sale", "all_time_low", "all_time_high"):
                if field in normalized_cache and normalized_cache[field] is not None and not isinstance(normalized_cache[field], dict):
                    normalized_cache[field] = None
                    changed = True

            if "notify_enabled" in normalized_cache and not isinstance(normalized_cache["notify_enabled"], bool):
                normalized_cache["notify_enabled"] = bool(normalized_cache["notify_enabled"])
                changed = True

            if "pinned" in normalized_cache and not isinstance(normalized_cache["pinned"], bool):
                normalized_cache["pinned"] = bool(normalized_cache["pinned"])
                changed = True

            clean_cache[str(key)] = normalized_cache

        if clean_cache != raw_cache:
            data["price_cache"] = clean_cache
            changed = True

        raw_alerts_any = data.get("alerts")
        if not isinstance(raw_alerts_any, dict):
            raw_alerts: Dict[str, Any] = {}
            data["alerts"] = raw_alerts
            changed = True
        else:
            raw_alerts = cast(Dict[str, Any], raw_alerts_any)

        clean_alerts: Dict[str, Any] = {}
        for key, value in raw_alerts.items():
            try:
                appid = int(str(key).strip())
            except (TypeError, ValueError):
                changed = True
                continue
            if appid <= 0 or str(appid) not in valid_ids or not isinstance(value, dict):
                changed = True
                continue
            clean_alerts[str(appid)] = value

        if clean_alerts != raw_alerts:
            data["alerts"] = clean_alerts
            changed = True

        return changed

    def _convert_amount(
        self,
        amount: Optional[float],
        source_currency: Optional[str],
        target_currency: str,
        rates_cache: Dict[str, Any],
    ) -> Optional[float]:
        if amount is None or source_currency is None:
            return None
        source = source_currency.upper()
        target = target_currency.upper()
        if source == target:
            return round(amount, 2)

        rates = rates_cache.get("rates", {})
        if source not in rates or target not in rates:
            return None

        source_rate = rates[source]
        target_rate = rates[target]
        if source_rate == 0:
            return None

        usd_amount = amount / source_rate
        return round(usd_amount * target_rate, 2)

    def _safe_iso_to_datetime(self, raw: Optional[str]) -> Optional[datetime]:
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return None

    def _extract_region_from_cache_key(self, key: str, fallback: str) -> str:
        raw = str(key).strip()
        if ":" not in raw:
            return fallback
        _, region_raw = raw.split(":", 1)
        region = region_raw.strip().lower()[:2]
        return region or fallback

    def _current_region_quote(self, cache_ref: Dict[str, Any], region: str) -> Optional[Dict[str, Any]]:
        current_any = cache_ref.get("current")
        current = cast(Dict[str, Any], current_any) if isinstance(current_any, dict) else {}
        price = current.get("price")
        currency = current.get("currency")
        if price is None or currency is None:
            return None

        all_time_low_any = cache_ref.get("all_time_low")
        all_time_low = cast(Dict[str, Any], all_time_low_any) if isinstance(all_time_low_any, dict) else {}
        low_price = all_time_low.get("price") if isinstance(all_time_low, dict) else None
        is_historic_low = False
        if low_price is not None and price is not None:
            try:
                is_historic_low = float(price) <= float(low_price)
            except (TypeError, ValueError):
                is_historic_low = False

        if price is None:
            return None
        try:
            price_value = round(float(price), 2)
        except (TypeError, ValueError):
            return None

        return {
            "region": region,
            "price": price_value,
            "currency": str(currency).upper(),
            "discount_percent": int(current.get("discount_percent", 0)),
            "on_sale": bool(current.get("on_sale")),
            "is_historic_low_for_region": is_historic_low,
        }

    def _build_regional_prices(
        self,
        region: str,
        cache_ref: Dict[str, Any],
        rates_cache: Dict[str, Any],
        target_currency: str,
    ) -> List[Dict[str, Any]]:
        quote = self._current_region_quote(cache_ref, region)
        if not quote:
            return []

        converted = self._convert_amount(
            quote["price"],
            quote["currency"],
            target_currency,
            rates_cache,
        )
        quote["converted_price"] = converted
        return [quote]

    def _next_sale_window(self, now: datetime) -> Tuple[str, datetime]:
        calendar = [
            ("Spring Sale", 3, 14),
            ("Summer Sale", 6, 27),
            ("Autumn Sale", 11, 26),
            ("Winter Sale", 12, 19),
        ]
        candidates: List[Tuple[str, datetime]] = []
        for name, month, day in calendar:
            dt = datetime(now.year, month, day, tzinfo=timezone.utc)
            if dt < now:
                dt = datetime(now.year + 1, month, day, tzinfo=timezone.utc)
            candidates.append((name, dt))
        return min(candidates, key=lambda item: item[1])

    def _itad_country_from_region(self, region: str) -> str:
        return store_country_from_region(region).upper()

    def _normalize_history_rows(self, raw_rows: Any) -> List[Dict[str, Any]]:
        if not isinstance(raw_rows, list):
            return []
        rows: List[Dict[str, Any]] = []
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            ts = str(row.get("timestamp") or "").strip()
            if not ts:
                continue
            try:
                parsed_ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if parsed_ts.tzinfo is None:
                    parsed_ts = parsed_ts.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            try:
                price = round(float(row.get("price")), 2)
            except (TypeError, ValueError):
                continue
            if price < 0:
                continue

            initial_raw = row.get("initial_price", row.get("regular_price", price))
            try:
                initial_price = round(float(initial_raw), 2)
            except (TypeError, ValueError):
                initial_price = price
            if initial_price < price:
                initial_price = price

            currency = str(row.get("currency") or "").strip().upper()[:6]
            try:
                discount = int(row.get("discount_percent", 0))
            except (TypeError, ValueError):
                discount = 0
            discount = max(0, min(discount, 99))
            on_sale = bool(row.get("on_sale")) or discount > 0 or (initial_price > price + 0.01)

            normalized = {
                "timestamp": parsed_ts.isoformat(),
                "price": price,
                "initial_price": initial_price,
                "currency": currency or "USD",
                "discount_percent": discount,
                "on_sale": on_sale,
            }
            source = str(row.get("source") or "").strip()
            if source:
                normalized["source"] = source
            rows.append(normalized)

        rows.sort(key=lambda item: item["timestamp"])
        return rows

    def _select_verified_history_6m(self, cache: Dict[str, Any], region: str) -> Tuple[List[Dict[str, Any]], str]:
        expected_itad_country = self._itad_country_from_region(region)
        itad_country = str(cache.get("itad_country") or "").strip().upper()
        itad_history = self._normalize_history_rows(cache.get("itad_history"))
        if itad_history and len(itad_history) >= 2 and itad_country == expected_itad_country:
            return itad_history, "itad"

        history_6m = self._normalize_history_rows(cache.get("history_6m"))
        if len(history_6m) >= 2:
            source = str(cache.get("history_source") or "local_tracking").strip() or "local_tracking"
            return history_6m, source

        return [], "none"

    def _build_low_from_history(self, history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if len(history) < 2:
            return None
        best = min(history, key=lambda item: item.get("price", float("inf")))
        return {
            "price": best.get("price"),
            "currency": best.get("currency"),
            "timestamp": best.get("timestamp"),
        }

    def _build_high_from_history(self, history: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if len(history) < 2:
            return None
        best = max(history, key=lambda item: item.get("initial_price", item.get("price", 0)))
        return {
            "price": best.get("initial_price", best.get("price")),
            "currency": best.get("currency"),
            "timestamp": best.get("timestamp"),
        }

    def _build_sale_prediction(self, game_cache: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        next_sale_name, next_sale_dt = self._next_sale_window(now)

        current = game_cache.get("current") or {}
        history = game_cache.get("history") or []
        last_sale = game_cache.get("last_sale")
        probability = 35
        reason = "baseline"

        if current.get("on_sale"):
            probability = 15
            reason = "already_on_sale"
        elif not history:
            probability = 30
            reason = "not_enough_history"
        else:
            on_sale_points = sum(1 for point in history if point.get("on_sale"))
            sale_ratio = on_sale_points / max(len(history), 1)
            probability = 30 + int(sale_ratio * 50)
            reason = "history_density"

            last_sale_dt = self._safe_iso_to_datetime((last_sale or {}).get("timestamp"))
            if last_sale_dt:
                days_since = max(0, int((now - last_sale_dt).days))
                if days_since >= 180:
                    probability += 25
                    reason = "long_time_without_sale"
                elif days_since >= 90:
                    probability += 12
                elif days_since <= 30:
                    probability -= 15

        discount = int(current.get("discount_percent", 0))
        if discount >= 50:
            probability -= 10

        probability = max(5, min(probability, 95))
        return {
            "next_sale_name": next_sale_name,
            "probability": probability,
            "estimated_date": next_sale_dt.date().isoformat(),
            "reason": reason,
        }

    def _convert_alert_for_view(
        self,
        raw_alert: Dict[str, Any],
        target_currency: str,
        rates_cache: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        target_usd = raw_alert.get("target_price_usd")
        if target_usd is None:
            return None

        converted_target = self._convert_amount(target_usd, "USD", target_currency, rates_cache)
        if converted_target is None:
            try:
                converted_target = round(float(raw_alert.get("target_price_input", 0)), 2)
            except (TypeError, ValueError):
                return None

        return {
            "target_price": converted_target,
            "currency": target_currency,
            "active": bool(raw_alert.get("active", False)),
            "updated_at": raw_alert.get("updated_at"),
        }

    def _convert_to_usd(self, amount: Optional[float], source_currency: Optional[str], rates_cache: Dict[str, Any]) -> Optional[float]:
        return self._convert_amount(amount, source_currency, "USD", rates_cache)

    def _normalize_setting_value(self, key: str, value: Any) -> Any:
        if key == "language":
            allowed = {"en", "ru", "zh-CN"}
            return value if value in allowed else "en"

        if key == "currency":
            return str(value or "USD").upper()[:8]

        if key == "region":
            return str(value or "us").lower()[:2]

        if key == "wishlist_mode":
            return value if value in {"steam", "manual"} else "steam"

        if key == "wishlist_api_mode":
            return value if value in {"auto", "official", "legacy"} else "auto"

        if key in {"steam_id", "steam_api_key"}:
            return str(value or "").strip()

        if key == "manual_wishlist":
            return str(value or "")

        if key == "auto_refresh_minutes":
            try:
                minutes = int(value)
            except (TypeError, ValueError):
                return 60
            return min(max(minutes, 5), 24 * 60)

        if key == "discount_notify_threshold":
            try:
                threshold = int(value)
            except (TypeError, ValueError):
                return 50
            return min(max(threshold, 1), 95)

        if key in {"filter_on_sale", "filter_never_discounted"}:
            return bool(value)

        if key in {"view_mode"}:
            return value if value in {"compact", "expanded"} else "compact"

        if key in {"sort_mode"}:
            return value if value in {"wishlist", "discount", "last_sale", "alphabet"} else "wishlist"

        if key in {"filter_price_min", "filter_price_max"}:
            if value in (None, ""):
                return None
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                return None
            return round(parsed, 2)

        if key == "itad_api_key":
            return str(value or "").strip()

        return value

    def _get_region(self, data: Dict[str, Any]) -> str:
        return str(data.get("settings", {}).get("region", "us") or "us").lower()[:2]

    def _extract_appid_from_cache_key(self, key: str) -> Optional[int]:
        raw = str(key).strip()
        if ":" in raw:
            raw = raw.split(":", 1)[0]
        try:
            value = int(raw)
            return value if value > 0 else None
        except (TypeError, ValueError):
            return None

    def _iter_app_cache_refs(self, data: Dict[str, Any], appid: int) -> List[Dict[str, Any]]:
        refs: List[Dict[str, Any]] = []
        prefix = f"{appid}:"
        for key, value in data.get("price_cache", {}).items():
            if not isinstance(value, dict):
                continue
            if key == str(appid) or key.startswith(prefix):
                refs.append(value)
        return refs

    def _resolve_game_cache(self, data: Dict[str, Any], appid: int, region: str) -> Dict[str, Any]:
        cache = data.get("price_cache", {})
        key = build_price_cache_key(appid, region)
        current = cache.get(key)
        if isinstance(current, dict):
            return current

        # Migrate user preferences from legacy non-region key if any,
        # but do NOT use the legacy price data (it may be from a different region).
        legacy = cache.get(str(appid))
        if isinstance(legacy, dict):
            new_entry: Dict[str, Any] = {}
            for pref_key in ("notify_enabled", "pinned"):
                if pref_key in legacy:
                    new_entry[pref_key] = legacy[pref_key]
            cache[key] = new_entry
            return new_entry

        return {}

    def _derive_status(self, game_cache: Dict[str, Any]) -> str:
        current = game_cache.get("current") or {}
        if current.get("on_sale"):
            return "on_sale_now"

        last_sale = game_cache.get("last_sale")
        if not last_sale:
            return "never_discounted"

        last_sale_dt = self._safe_iso_to_datetime(last_sale.get("timestamp"))
        if not last_sale_dt:
            return "long_without_sale"

        now = datetime.now(timezone.utc)
        delta_days = (now - last_sale_dt).days
        if delta_days <= 45:
            return "recently_discounted"
        return "long_without_sale"

    def _normalize_wishlist_game(self, game: Dict[str, Any], fallback_order: int) -> Optional[Dict[str, Any]]:
        if not isinstance(game, dict):
            return None
        try:
            appid = int(game.get("appid", 0))
        except (TypeError, ValueError):
            return None
        if appid <= 0:
            return None
        title = str(game.get("title") or "").strip() or f"App {appid}"
        try:
            order = int(game.get("order", fallback_order))
        except (TypeError, ValueError):
            order = fallback_order
        if order <= 0:
            order = fallback_order
        return {
            "appid": appid,
            "title": title,
            "order": order,
        }

    def _normalize_price_hint(self, raw_hint: Any, fallback_currency: str) -> Optional[Dict[str, Any]]:
        if not isinstance(raw_hint, dict):
            return None

        price_raw = raw_hint.get("price")
        if price_raw is None:
            return None
        try:
            price = round(float(price_raw), 2)
        except (TypeError, ValueError):
            return None
        if price < 0:
            return None

        initial_raw = raw_hint.get("initial_price", price)
        try:
            initial_price = round(float(initial_raw), 2)
        except (TypeError, ValueError):
            initial_price = price
        if initial_price < price:
            initial_price = price

        try:
            discount_percent = int(raw_hint.get("discount_percent", 0))
        except (TypeError, ValueError):
            discount_percent = 0
        discount_percent = max(0, min(discount_percent, 99))

        currency = str(raw_hint.get("currency") or fallback_currency or "USD").strip().upper()
        if not currency or len(currency) > 6:
            currency = str(fallback_currency or "USD").upper()
        if not currency:
            currency = "USD"

        return {
            "price": price,
            "initial_price": initial_price,
            "currency": currency,
            "discount_percent": discount_percent,
            "on_sale": bool(raw_hint.get("on_sale")) or discount_percent > 0,
        }

    def _seed_prices_from_wishlist_hints(self, data: Dict[str, Any], raw_games: List[Dict[str, Any]]) -> None:
        pass

    def _build_game_view(self, game: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
        settings = data["settings"]
        target_currency = settings.get("currency", "USD")
        appid = int(game["appid"])
        region = self._get_region(data)
        cache = self._resolve_game_cache(data, appid, region)
        current = cache.get("current") or {}
        regional_prices = self._build_regional_prices(region, cache, data["rates_cache"], target_currency)
        sale_prediction = self._build_sale_prediction(cache)

        raw_alert = data.get("alerts", {}).get(str(appid))
        target_alert = None
        if isinstance(raw_alert, dict) and raw_alert.get("active"):
            target_alert = self._convert_alert_for_view(raw_alert, target_currency, data["rates_cache"])

        source_currency = current.get("currency")
        converted_price = self._convert_amount(
            current.get("price"), source_currency, target_currency, data["rates_cache"]
        )
        converted_initial_price = self._convert_amount(
            current.get("initial_price"), source_currency, target_currency, data["rates_cache"]
        )
        # When conversion fails (no exchange rates), display native price with its real currency
        if converted_price is None:
            converted_price = current.get("price")
            converted_initial_price = current.get("initial_price")
            # IMPORTANT: override target_currency to match what we're actually showing
            target_currency = source_currency or target_currency
        
        history_preview, history_source = self._select_verified_history_6m(cache, region)
        history_preview = history_preview[-80:] if history_preview else []
        all_time_low = self._build_low_from_history(history_preview)
        if all_time_low is None:
            # Fallback to cache-stored ATL (from CheapShark or previous tracking)
            cached_atl = cache.get("all_time_low")
            if isinstance(cached_atl, dict) and cached_atl.get("price") is not None:
                all_time_low = cached_atl
        all_time_high = self._build_high_from_history(history_preview)
        if all_time_high is None:
            cached_ath = cache.get("all_time_high")
            if isinstance(cached_ath, dict) and cached_ath.get("price") is not None:
                all_time_high = cached_ath
        converted_low = self._convert_amount(
            (all_time_low or {}).get("price"),
            (all_time_low or {}).get("currency"),
            target_currency,
            data["rates_cache"],
        )

        current_title = str(current.get("title") or "").strip()
        game_title = str(game.get("title") or f"App {appid}").strip() or f"App {appid}"
        if game_title.startswith("App ") and current_title and not current_title.startswith("App "):
            game_title = current_title

        # Metacritic: prefer cached (from CheapShark), fallback to current (from Steam API)
        metacritic = cache.get("metacritic_score") or current.get("metacritic_score")
        release_date = current.get("release_date") or cache.get("release_date")
        is_released = current.get("is_released")
        if is_released is None:
            is_released = cache.get("is_released")
        capsule_url = current.get("capsule_url") or cache.get("capsule_url")
        store_url = current.get("store_url") or cache.get("store_url")
        
        # Fallback to other regional caches if global metadata is missing
        if not metacritic or not release_date or is_released is None or not capsule_url or not store_url:
            for other_cache in self._iter_app_cache_refs(data, appid):
                other_current = other_cache.get("current") or {}
                if not metacritic:
                    metacritic = other_cache.get("metacritic_score") or other_current.get("metacritic_score")
                if not release_date:
                    release_date = other_current.get("release_date") or other_cache.get("release_date")
                if is_released is None:
                    is_r = other_current.get("is_released")
                    if is_r is None:
                        is_r = other_cache.get("is_released")
                    is_released = is_r
                if not capsule_url:
                    capsule_url = other_current.get("capsule_url") or other_cache.get("capsule_url")
                if not store_url:
                    store_url = other_current.get("store_url") or other_cache.get("store_url")

        if is_released is None:
            is_released = True
        store_url = store_url or f"https://store.steampowered.com/app/{appid}/"
        all_time_low_verified = all_time_low is not None

        return {
            "appid": appid,
            "title": game_title,
            "wishlist_order": game.get("order", 0),
            "current_price": current.get("price"),
            "initial_price": current.get("initial_price"),
            "current_currency": current.get("currency"),
            "converted_price": converted_price,
            "converted_initial_price": converted_initial_price,
            "converted_currency": target_currency,
            "discount_percent": current.get("discount_percent", 0),
            "on_sale": bool(current.get("on_sale")),
            "last_sale": cache.get("last_sale"),
            "all_time_low": all_time_low,
            "all_time_low_converted": converted_low,
            "all_time_low_verified": all_time_low_verified,
            "all_time_high": all_time_high,
            "history": history_preview,
            "itad_history": cache.get("itad_history", []) if isinstance(cache.get("itad_history", []), list) else [],
            "history_source": history_source,
            "history_unofficial": history_source == "steamdb_unofficial",
            "status": self._derive_status(cache),
            "notify_enabled": bool(cache.get("notify_enabled", False)),
            "pinned": bool(cache.get("pinned", False)),
            "last_error": cache.get("last_error"),
            "regional_prices": regional_prices,
            "sale_prediction": sale_prediction,
            "metacritic_score": metacritic,
            "target_alert": target_alert,
            "release_date": release_date,
            "is_released": is_released,
            "capsule_url": capsule_url,
            "store_url": store_url,
            "price_data_consistent": bool(current.get("price_data_consistent", True)) and (not history_preview or history_preview[-1].get("currency") == target_currency),
            "price_data_error": current.get("price_data_error") or ("currency_mismatch" if history_preview and history_preview[-1].get("currency") != target_currency else None),
        }

    def get_sales_events(self) -> Dict[str, Any]:
        try:
            return self.sales_provider.get_sales_events()
        except Exception as err:
            return {
                "events": [],
                "warning": f"Live Steam sales feed unavailable ({err}).",
                "source": "fallback_calendar",
                "fetched_at": utc_now_iso(),
            }

    def get_bootstrap(self) -> Dict[str, Any]:
        data = self._load()

        settings = data.get("settings", {})
        steam_id = settings.get("steam_id", "")
        if not steam_id or not str(steam_id).strip():
            detected = detect_steam_id()
            if detected:
                settings["steam_id"] = detected
                data["settings"] = settings
                self._save(data)

        region = settings.get("region", "")
        if not region or region == "us":
            detected_region = detect_steam_region()
            if detected_region:
                settings["region"] = detected_region
                data["settings"] = settings
                self._save(data)

        # NOTE: currency rates and sales events are intentionally NOT fetched here.
        # Fetching them on every panel open was a major source of first-open hangs
        # (up to 2+ minutes on slow networks). Rates are loaded lazily via
        # update_currency_rates(); sales are fetched by the frontend when it switches
        # to the Sales tab (separate get_sales_events action).
        games = [self._build_game_view(game, data) for game in data["wishlist"]]
        return {
            "settings": data["settings"],
            "meta": data["meta"],
            "rates_cache": data["rates_cache"],
            "games": games,
            "sales": {},
        }

    def ping(self) -> Dict[str, Any]:
        return {"ok": True}

    def get_settings(self) -> Dict[str, Any]:
        data = self._load()
        return {"settings": data.get("settings", {})}

    def set_settings(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        return self.save_settings(patch)

    def get_manual_list(self) -> Dict[str, Any]:
        data = self._load()
        return {"manual_wishlist": str(data.get("settings", {}).get("manual_wishlist", ""))}

    def set_manual_list(self, manual_wishlist: str) -> Dict[str, Any]:
        return self.save_settings({"manual_wishlist": str(manual_wishlist or "")})

    def save_settings(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        data = self._load()
        for key, value in patch.items():
            if key in data["settings"]:
                data["settings"][key] = self._normalize_setting_value(key, value)
        self._save(data)
        return {"ok": True}

    def sync_wishlist(
        self,
        mode: Optional[str] = None,
        steam_id: Optional[str] = None,
        steam_api_key: Optional[str] = None,
        wishlist_api_mode: Optional[str] = None,
        manual_wishlist: Optional[str] = None,
    ) -> Dict[str, Any]:
        data = self._load()
        settings = data["settings"]

        if mode is not None:
            settings["wishlist_mode"] = self._normalize_setting_value("wishlist_mode", mode)
        if steam_id is not None:
            settings["steam_id"] = self._normalize_setting_value("steam_id", steam_id)
        if steam_api_key is not None:
            settings["steam_api_key"] = self._normalize_setting_value("steam_api_key", steam_api_key)
        if wishlist_api_mode is not None:
            settings["wishlist_api_mode"] = self._normalize_setting_value(
                "wishlist_api_mode", wishlist_api_mode.strip().lower()
            )
        if manual_wishlist is not None:
            settings["manual_wishlist"] = manual_wishlist

        active_mode = settings.get("wishlist_mode", "steam")
        raw_games: List[Dict[str, Any]] = []
        if active_mode == "manual":
            games, status = self.wishlist_provider.parse_manual_wishlist(settings.get("manual_wishlist", ""))
            raw_games = [game for game in games if isinstance(game, dict)]
        else:
            active_steam_id = settings.get("steam_id", "").strip()
            if not active_steam_id:
                detected = detect_steam_id()
                if detected:
                    active_steam_id = detected
                    settings["steam_id"] = detected

            games, status = self.wishlist_provider.fetch_wishlist(
                steam_id=active_steam_id,
                steam_api_key=settings.get("steam_api_key", ""),
                api_mode=settings.get("wishlist_api_mode", "auto"),
            )
            raw_games = [game for game in games if isinstance(game, dict)]

        normalized_games: List[Dict[str, Any]] = []
        seen_ids = set()
        for fallback_order, raw_game in enumerate(raw_games, start=1):
            normalized = self._normalize_wishlist_game(raw_game, fallback_order)
            if not normalized:
                continue
            appid = int(normalized["appid"])
            if appid in seen_ids:
                continue
            seen_ids.add(appid)
            normalized_games.append(normalized)
        games = normalized_games

        auto_price_error = ""
        if status == "ok":
            existing_cache = data["price_cache"]
            valid_ids = {str(game["appid"]) for game in games}
            filtered_cache: Dict[str, Any] = {}
            for key, value in existing_cache.items():
                appid = self._extract_appid_from_cache_key(key)
                if appid is None:
                    continue
                if str(appid) in valid_ids:
                    filtered_cache[key] = value
            data["price_cache"] = filtered_cache
            data["alerts"] = {
                str(appid): value
                for appid, value in data.get("alerts", {}).items()
                if str(appid) in valid_ids
            }
            data["wishlist"] = games
            data["meta"]["wishlist_last_sync"] = utc_now_iso()
            self._seed_prices_from_wishlist_hints(data, raw_games)

            # Try to populate current prices right after successful sync,
            # so cards are not empty on first open.
            try:
                appids = [int(game["appid"]) for game in data["wishlist"]]
                region = self._get_region(data)
                summary = self.price_provider.update_prices(appids, region, data["price_cache"])
                title_map = summary.get("titles", {}) if isinstance(summary, dict) else {}
                for game in data["wishlist"]:
                    appid = int(game.get("appid", 0))
                    if appid <= 0:
                        continue
                    if game.get("title", "").startswith("App "):
                        hinted = title_map.get(appid)
                        if hinted:
                            game["title"] = hinted
                data["meta"]["price_last_sync"] = utc_now_iso()
            except Exception as err:
                # Keep sync successful even if price update is temporarily unavailable.
                auto_price_error = f"auto_price_update_failed:{err.__class__.__name__}:{err}"

        saved = self._save(data)

        # Build the response from already-loaded data instead of calling get_bootstrap()
        # again. The double get_bootstrap() call was adding a full extra round-trip
        # (including sales network fetch) after every wishlist sync.
        games = [self._build_game_view(game, data) for game in data["wishlist"]]
        bootstrap = {
            "settings": data["settings"],
            "meta": data["meta"],
            "rates_cache": data["rates_cache"],
            "games": games,
            "sales": {},
            "sync_status": status,
            "wishlist_count": len(saved["wishlist"]),
        }
        detail = str(getattr(self.wishlist_provider, "last_error_detail", "") or "").strip()
        if status != "ok" and detail:
            bootstrap["sync_status_detail"] = detail
        elif auto_price_error:
            bootstrap["sync_status_detail"] = auto_price_error
        return bootstrap

    def update_prices(self) -> Dict[str, Any]:
        data = self._load()
        appids = [int(game["appid"]) for game in data["wishlist"]]
        region = self._get_region(data)

        summary = self.price_provider.update_prices(appids, region, data["price_cache"])
        title_map = summary.get("titles", {}) if isinstance(summary, dict) else {}

        for game in data["wishlist"]:
            game_cache = self._resolve_game_cache(data, int(game["appid"]), region)
            current = (game_cache or {}).get("current") or {}
            current_title = current.get("title")
            fallback_title = title_map.get(int(game["appid"]))
            best_title = current_title or fallback_title
            if best_title and game.get("title", "").startswith("App "):
                game["title"] = best_title

        data["meta"]["price_last_sync"] = utc_now_iso()

        # ── ITAD: fetch real price history for charts ───────────────────
        itad_key = str(data.get("settings", {}).get("itad_api_key", "") or "").strip()
        if itad_key:
            self.itad_provider.api_key = itad_key
            itad_country = self._itad_country_from_region(region)
            for game in data["wishlist"]:
                appid = int(game["appid"])
                game_cache = self._resolve_game_cache(data, appid, region)
                if game_cache is None:
                    continue
                try:
                    itad_history = self.itad_provider.fetch_price_history(appid, months=6, country=itad_country)
                    if isinstance(itad_history, list) and len(itad_history) >= 2:
                        game_cache["itad_history"] = itad_history
                        game_cache["itad_country"] = itad_country
                        game_cache["history_source"] = "itad"
                        logger.info("ITAD history: %d points for appid %d", len(itad_history), appid)
                except Exception as e:
                    logger.warning("ITAD history fetch failed for %d: %s", appid, e)

        notifications: List[Dict[str, Any]] = []
        threshold = int(data["settings"].get("discount_notify_threshold", 50))
        alerts = data.get("alerts", {})
        for game in data["wishlist"]:
            cache = self._resolve_game_cache(data, int(game["appid"]), region)
            current = cache.get("current") or {}
            notify_enabled = bool(cache.get("notify_enabled", False))

            if notify_enabled and current.get("on_sale"):
                discount_percent = int(current.get("discount_percent", 0))
                if discount_percent >= threshold:
                    discount_signature = (
                        f"{current.get('price')}:{current.get('initial_price')}:{discount_percent}:{threshold}"
                    )
                    if cache.get("last_notified_discount_signature") != discount_signature:
                        notifications.append(
                            {
                                "appid": game["appid"],
                                "title": game["title"],
                                "type": "discount_threshold",
                                "discount_percent": discount_percent,
                            }
                        )
                        cache["last_notified_discount_signature"] = discount_signature

                low = cache.get("all_time_low") or {}
                if low and low.get("price") == current.get("price"):
                    low_signature = f"{low.get('price')}:{low.get('timestamp')}"
                    if cache.get("last_notified_low_signature") != low_signature:
                        notifications.append(
                            {
                                "appid": game["appid"],
                                "title": game["title"],
                                "type": "all_time_low",
                            }
                        )
                        cache["last_notified_low_signature"] = low_signature

            raw_alert = alerts.get(str(game["appid"]))
            if not isinstance(raw_alert, dict) or not raw_alert.get("active"):
                continue

            target_usd = raw_alert.get("target_price_usd")
            current_usd = self._convert_to_usd(
                current.get("price"),
                current.get("currency"),
                data["rates_cache"],
            )
            if current_usd is None or target_usd is None:
                continue

            try:
                target_usd_float = round(float(target_usd), 4)
            except (TypeError, ValueError):
                continue

            if current_usd <= target_usd_float:
                alert_signature = f"{current.get('price')}:{current.get('currency')}:{target_usd_float}"
                if raw_alert.get("last_trigger_signature") != alert_signature:
                    notifications.append(
                        {
                            "appid": game["appid"],
                            "title": game["title"],
                            "type": "price_alert",
                            "discount_percent": int(current.get("discount_percent", 0)),
                            "price": current.get("price"),
                            "currency": current.get("currency"),
                            "target_price": target_usd_float,
                        }
                    )
                    raw_alert["last_trigger_signature"] = alert_signature
            else:
                raw_alert["last_trigger_signature"] = None

        self._save(data)
        bootstrap = self.get_bootstrap()
        bootstrap["price_update_summary"] = summary
        bootstrap["notifications"] = notifications
        return bootstrap

    def update_currency_rates(self, force: bool = False) -> Dict[str, Any]:
        data = self._load()
        result = self.exchange_provider.update_rates(data["rates_cache"], force=force)
        data["rates_cache"] = result["cache"]
        data["meta"]["rates_last_sync"] = data["rates_cache"].get("updated_at")
        self._save(data)

        bootstrap = self.get_bootstrap()
        bootstrap["rates_update"] = {
            "from_cache": result["from_cache"],
            "error": result["error"],
        }
        return bootstrap

    def clear_cache(self) -> Dict[str, Any]:
        data = self._load()
        data["price_cache"] = {}
        data["rates_cache"] = {
            "base": "USD",
            "rates": {"USD": 1.0},
            "updated_at": None,
        }
        data["meta"]["price_last_sync"] = None
        data["meta"]["rates_last_sync"] = None
        self._save(data)
        return self.get_bootstrap()

    def set_game_preferences(
        self,
        appid: int,
        pinned: Optional[bool] = None,
        notify_enabled: Optional[bool] = None,
    ) -> Dict[str, Any]:
        data = self._load()
        region = self._get_region(data)
        key = build_price_cache_key(appid, region)
        cache_refs = self._iter_app_cache_refs(data, appid)
        if not cache_refs:
            cache_refs = [data["price_cache"].setdefault(key, {})]

        for game_cache in cache_refs:
            if pinned is not None:
                game_cache["pinned"] = bool(pinned)
            if notify_enabled is not None:
                game_cache["notify_enabled"] = bool(notify_enabled)
        self._save(data)
        return self.get_bootstrap()

    def set_price_alert(
        self,
        appid: int,
        target_price: float,
        target_currency: str,
        active: bool = True,
    ) -> Dict[str, Any]:
        if appid <= 0:
            raise ValueError("invalid_appid")
        try:
            parsed_target = float(target_price)
        except (TypeError, ValueError):
            raise ValueError("invalid_alert_price") from None
        if parsed_target <= 0:
            raise ValueError("invalid_alert_price")

        data = self._load()
        if not any(int(game.get("appid", 0)) == appid for game in data.get("wishlist", [])):
            raise ValueError("invalid_appid")
        normalized_currency = str(target_currency or "").upper().strip()
        if not normalized_currency:
            raise ValueError("alert_currency_unsupported")

        target_usd = self._convert_to_usd(parsed_target, normalized_currency, data["rates_cache"])
        if target_usd is None:
            raise ValueError("alert_currency_unsupported")

        alerts = data.setdefault("alerts", {})
        raw = alerts.get(str(appid), {})
        if not isinstance(raw, dict):
            raw = {}
        raw.update(
            {
                "active": bool(active),
                "target_price_input": round(parsed_target, 2),
                "target_currency": normalized_currency,
                "target_price_usd": round(target_usd, 4),
                "updated_at": utc_now_iso(),
            }
        )
        alerts[str(appid)] = raw
        self._save(data)
        return self.get_bootstrap()

    def clear_price_alert(self, appid: int) -> Dict[str, Any]:
        if appid <= 0:
            raise ValueError("invalid_appid")
        data = self._load()
        data.get("alerts", {}).pop(str(appid), None)
        self._save(data)
        return self.get_bootstrap()

    def analyze_deals(self, query: str = "", history: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        data = self._load()
        language = data.get("settings", {}).get("language", "en")
        region = self._get_region(data).upper()
        games = [self._build_game_view(game, data) for game in data["wishlist"]]
        lowered_query = str(query or "").lower()
        history = history or []

        if not games:
            if language == "ru":
                return {"reply": "Список игр пуст. Сначала синхронизируй вишлист, и я дам рекомендации."}
            if language == "zh-CN":
                return {"reply": "愿望单为空。先同步愿望单，我再给出建议。"}
            return {"reply": "Wishlist is empty. Sync it first and I will suggest best deals."}

        best_discount = max(games, key=lambda game: int(game.get("discount_percent", 0)))
        best_wait = max(games, key=lambda game: int((game.get("sale_prediction") or {}).get("probability", 0)))

        best_global_gap: Optional[Dict[str, Any]] = None
        for game in games:
            regional_prices = [item for item in game.get("regional_prices", []) if item.get("converted_price") is not None]
            if len(regional_prices) < 2:
                continue
            home = next((item for item in regional_prices if str(item.get("region", "")).upper() == region), regional_prices[0])
            global_best = min(regional_prices, key=lambda item: item["converted_price"])
            home_price = float(home.get("converted_price", 0))
            best_price = float(global_best.get("converted_price", 0))
            if home_price <= 0 or home_price <= best_price:
                continue

            diff_percent = round(((home_price - best_price) / home_price) * 100, 1)
            candidate = {
                "title": game.get("title", "Unknown"),
                "home_region": str(home.get("region", "")).upper(),
                "home_price": round(home_price, 2),
                "best_region": str(global_best.get("region", "")).upper(),
                "best_price": round(best_price, 2),
                "diff_percent": diff_percent,
            }
            if best_global_gap is None or candidate["diff_percent"] > best_global_gap["diff_percent"]:
                best_global_gap = candidate

        if language == "ru":
            lines: List[str] = [
                f"Картофельный аналитик: регион {region}, игр в списке: {len(games)}.",
                f"Лучшая скидка сейчас: {best_discount['title']} (-{best_discount['discount_percent']}%).",
                (
                    f"Вероятнее всего дождаться распродажи у {best_wait['title']} "
                    f"({best_wait['sale_prediction']['probability']}%, ~{best_wait['sale_prediction']['estimated_date']})."
                ),
            ]
            if best_global_gap:
                lines.append(
                    f"Самый заметный региональный разрыв: {best_global_gap['title']} "
                    f"({best_global_gap['home_region']} -> {best_global_gap['best_region']}, выгода ~{best_global_gap['diff_percent']}%)."
                )
            if any(token in lowered_query for token in ("ждать", "wait", "подожд")):
                lines.append("Если цель сэкономить максимум: ориентируйся на игры с вероятностью распродажи от 70% и выше.")
            elif any(token in lowered_query for token in ("куп", "buy", "сейчас")):
                lines.append("Если покупать сейчас: выбирай позиции с текущей скидкой и статусом исторического минимума по региону.")
            if history:
                lines.append("Контекст предыдущего чата учтен.")
            return {"reply": "\n".join(lines)}

        if language == "zh-CN":
            lines = [
                f"Potato 分析：当前区域 {region}，愿望单共 {len(games)} 个游戏。",
                f"当前折扣最高：{best_discount['title']}（-{best_discount['discount_percent']}%）。",
                f"最建议等待：{best_wait['title']}（概率 {best_wait['sale_prediction']['probability']}%，预计 {best_wait['sale_prediction']['estimated_date']}）。",
            ]
            if best_global_gap:
                lines.append(
                    f"区域价差最大：{best_global_gap['title']}（{best_global_gap['home_region']} -> {best_global_gap['best_region']}，约省 {best_global_gap['diff_percent']}%）。"
                )
            return {"reply": "\n".join(lines)}

        lines_en = [
            f"Potato analyst: region {region}, wishlist size {len(games)}.",
            f"Best active discount: {best_discount['title']} (-{best_discount['discount_percent']}%).",
            f"Best wait candidate: {best_wait['title']} ({best_wait['sale_prediction']['probability']}%, ~{best_wait['sale_prediction']['estimated_date']}).",
        ]
        if best_global_gap:
            lines_en.append(
                f"Largest regional gap: {best_global_gap['title']} "
                f"({best_global_gap['home_region']} -> {best_global_gap['best_region']}, ~{best_global_gap['diff_percent']}% better)."
            )
        return {"reply": "\n".join(lines_en)}
