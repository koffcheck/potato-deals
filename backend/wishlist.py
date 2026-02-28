import json
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

from .http_utils import RequestFailedError, RateLimitError, fetch_json_with_retry


class WishlistProvider:
    def __init__(self, request_timeout: int = 7) -> None:
        self.request_timeout = request_timeout
        self.last_error_detail = ""
        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PotatoDeals/2.0"
        )

    def _set_error_detail(self, value: str) -> None:
        self.last_error_detail = str(value or "").strip()[:300]

    def _fetch_json(self, url: str) -> Dict[str, Any]:
        return fetch_json_with_retry(
            url,
            timeout=self.request_timeout,
            user_agent=self.user_agent,
            max_retries=1,
        )

    def _safe_int(self, value: Any) -> Optional[int]:
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            return None
        if parsed <= 0:
            return None
        return parsed

    def _is_placeholder_title(self, value: Any) -> bool:
        title = str(value or "").strip()
        if not title.lower().startswith("app "):
            return False
        suffix = title[4:].strip()
        return suffix.isdigit()

    def _safe_currency(self, value: Any, fallback: str = "USD") -> str:
        text = str(value or "").strip().upper()
        if not text or len(text) > 6:
            return fallback
        return text

    def _extract_public_price_snapshot(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None

        subs = item.get("subs")
        best_sub: Optional[Dict[str, Any]] = None
        if isinstance(subs, list):
            for entry in subs:
                if not isinstance(entry, dict):
                    continue
                if self._safe_int(entry.get("price")) is not None:
                    best_sub = entry
                    break
                if best_sub is None:
                    best_sub = entry

        is_free = bool(item.get("is_free_game"))
        if best_sub and bool(best_sub.get("is_free_license")):
            is_free = True

        currency = self._safe_currency(
            (best_sub or {}).get("currency") or item.get("currency"),
            fallback="USD",
        )
        if is_free:
            return {
                "price": 0.0,
                "initial_price": 0.0,
                "currency": currency,
                "discount_percent": 0,
                "on_sale": False,
            }

        if best_sub is None:
            return None

        final_cents = self._safe_int(best_sub.get("price"))
        if final_cents is None:
            return None
        initial_cents = self._safe_int(best_sub.get("price_original")) or final_cents

        discount = self._safe_int(best_sub.get("discount_pct")) or 0
        if discount <= 0 and initial_cents > 0 and final_cents < initial_cents:
            discount = int(round((initial_cents - final_cents) * 100 / initial_cents))
        discount = max(0, min(discount, 99))

        return {
            "price": round(final_cents / 100.0, 2),
            "initial_price": round(initial_cents / 100.0, 2),
            "currency": currency,
            "discount_percent": discount,
            "on_sale": discount > 0,
        }

    def _merge_wishlists(self, primary: List[Dict[str, Any]], fallback: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: Dict[int, Dict[str, Any]] = {}
        for fallback_order, game in enumerate(primary, start=1):
            appid = self._safe_int(game.get("appid"))
            if appid is None:
                continue
            merged[appid] = {
                "appid": appid,
                "title": str(game.get("title") or f"App {appid}"),
                "order": self._safe_int(game.get("order")) or fallback_order,
            }
            price_snapshot = game.get("wishlist_price")
            if isinstance(price_snapshot, dict):
                merged[appid]["wishlist_price"] = price_snapshot

        for fallback_order, game in enumerate(fallback, start=1):
            appid = self._safe_int(game.get("appid"))
            if appid is None:
                continue
            incoming_title = str(game.get("title") or f"App {appid}")
            incoming_order = self._safe_int(game.get("order")) or fallback_order
            incoming_price = game.get("wishlist_price")
            if appid not in merged:
                merged[appid] = {
                    "appid": appid,
                    "title": incoming_title,
                    "order": incoming_order,
                }
                if isinstance(incoming_price, dict):
                    merged[appid]["wishlist_price"] = incoming_price
                continue

            current = merged[appid]
            if self._is_placeholder_title(current.get("title")) and not self._is_placeholder_title(incoming_title):
                current["title"] = incoming_title
            current["order"] = min(int(current.get("order", incoming_order)), incoming_order)
            if "wishlist_price" not in current and isinstance(incoming_price, dict):
                current["wishlist_price"] = incoming_price

        return self._normalize_games(merged.values())

    def _normalize_games(self, games: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        dedup: Dict[int, Dict[str, Any]] = {}
        for fallback_order, game in enumerate(games, start=1):
            appid = self._safe_int(game.get("appid"))
            if appid is None:
                continue
            title = str(game.get("title") or "").strip() or f"App {appid}"
            order = self._safe_int(game.get("order")) or fallback_order
            price_snapshot = game.get("wishlist_price")
            has_price_snapshot = isinstance(price_snapshot, dict)
            if appid not in dedup:
                dedup[appid] = {
                    "appid": appid,
                    "title": title,
                    "order": order,
                }
                if has_price_snapshot:
                    dedup[appid]["wishlist_price"] = price_snapshot
                continue

            existing = dedup[appid]
            if existing["title"].startswith("App ") and title and not title.startswith("App "):
                existing["title"] = title
            existing["order"] = min(existing["order"], order)
            if "wishlist_price" not in existing and has_price_snapshot:
                existing["wishlist_price"] = price_snapshot

        normalized = sorted(dedup.values(), key=lambda x: x.get("order", 0))
        for index, game in enumerate(normalized, start=1):
            game["order"] = index
        return normalized

    def _parse_official_payload(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = payload.get("response", payload)
        if not isinstance(response, dict):
            return []

        raw_items: List[Dict[str, Any]] = []

        if isinstance(response.get("items"), list):
            for item in response["items"]:
                if not isinstance(item, dict):
                    continue
                raw_items.append(
                    {
                        "appid": item.get("appid") or item.get("app_id"),
                        "title": item.get("name") or item.get("title"),
                        "order": item.get("priority") or item.get("position") or item.get("order"),
                    }
                )

        if not raw_items and isinstance(response.get("appids"), list):
            for idx, appid in enumerate(response["appids"], start=1):
                raw_items.append({"appid": appid, "order": idx})

        if not raw_items and isinstance(response.get("wishlist"), list):
            for idx, item in enumerate(response["wishlist"], start=1):
                if isinstance(item, dict):
                    raw_items.append(
                        {
                            "appid": item.get("appid"),
                            "title": item.get("name") or item.get("title"),
                            "order": item.get("priority") or item.get("position") or idx,
                        }
                    )
                else:
                    raw_items.append({"appid": item, "order": idx})

        if not raw_items:
            discovered = self._extract_appids_recursive(response)
            raw_items = [{"appid": appid, "order": idx} for idx, appid in enumerate(discovered, start=1)]

        return self._normalize_games(raw_items)

    def _extract_appids_recursive(self, node: Any) -> List[int]:
        appids: List[int] = []

        def walk(current: Any) -> None:
            if isinstance(current, dict):
                for key, value in current.items():
                    key_lower = str(key).lower()
                    if key_lower == "appid":
                        parsed = self._safe_int(value)
                        if parsed is not None:
                            appids.append(parsed)
                        continue

                    if key_lower in ("appids", "wishlist", "items", "games") and isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                parsed = self._safe_int(item.get("appid") or item.get("app_id"))
                                if parsed is not None:
                                    appids.append(parsed)
                                walk(item)
                            else:
                                parsed = self._safe_int(item)
                                if parsed is not None:
                                    appids.append(parsed)
                        continue

                    walk(value)
                return

            if isinstance(current, list):
                for item in current:
                    walk(item)

        walk(node)

        unique: List[int] = []
        seen = set()
        for appid in appids:
            if appid in seen:
                continue
            seen.add(appid)
            unique.append(appid)
        return unique

    def fetch_official_wishlist(self, steam_id: str, steam_api_key: str) -> Tuple[List[Dict[str, Any]], str]:
        self._set_error_detail("")
        if not steam_id.strip():
            self._set_error_detail("steam_id_missing")
            return [], "steam_id_missing"
        if not steam_api_key.strip():
            self._set_error_detail("steam_api_key_missing")
            return [], "steam_api_key_missing"

        input_json = quote(json.dumps({"steamid": steam_id.strip()}, separators=(",", ":")))
        urls = [
            f"https://api.steampowered.com/IWishlistService/GetWishlist/v1/?key={steam_api_key.strip()}&input_json={input_json}",
            f"https://api.steampowered.com/IWishlistService/GetWishlist/v1/?key={steam_api_key.strip()}&steamid={steam_id.strip()}",
        ]

        had_network_or_parse_error = False

        for url in urls:
            try:
                payload = self._fetch_json(url)
            except RateLimitError:
                self._set_error_detail("rate_limited")
                return [], "wishlist_rate_limited"
            except RequestFailedError as err:
                message = str(err)
                self._set_error_detail(message)
                if message in ("http_error_401", "http_error_403"):
                    return [], "steam_api_key_invalid"
                had_network_or_parse_error = True
                continue

            games = self._parse_official_payload(payload)
            if games:
                return games, "ok"

        if had_network_or_parse_error:
            return [], "wishlist_network_error"
        self._set_error_detail("empty_or_private")
        return [], "wishlist_empty_or_private"

    def fetch_official_wishlist_without_key(self, steam_id: str) -> Tuple[List[Dict[str, Any]], str]:
        self._set_error_detail("")
        raw_id = steam_id.strip()
        if not raw_id:
            self._set_error_detail("steam_id_missing")
            return [], "steam_id_missing"
        if not raw_id.isdigit():
            self._set_error_detail("steam_id_missing")
            return [], "steam_id_missing"

        input_json = quote(json.dumps({"steamid": raw_id}, separators=(",", ":")))
        urls = [
            f"https://api.steampowered.com/IWishlistService/GetWishlist/v1/?steamid={raw_id}",
            f"https://api.steampowered.com/IWishlistService/GetWishlist/v1/?input_json={input_json}",
        ]

        had_network_or_parse_error = False
        for url in urls:
            try:
                payload = self._fetch_json(url)
            except RateLimitError:
                self._set_error_detail("rate_limited")
                return [], "wishlist_rate_limited"
            except RequestFailedError as err:
                message = str(err)
                self._set_error_detail(message)
                if message in ("http_error_401", "http_error_403"):
                    return [], "steam_api_key_missing"
                had_network_or_parse_error = True
                continue

            games = self._parse_official_payload(payload)
            if games:
                return games, "ok"

        if had_network_or_parse_error:
            return [], "wishlist_network_error"
        self._set_error_detail("empty_or_private")
        return [], "wishlist_empty_or_private"

    def fetch_public_wishlist(self, steam_id: str) -> Tuple[List[Dict[str, Any]], str]:
        self._set_error_detail("")
        raw_id = steam_id.strip()
        if not raw_id:
            self._set_error_detail("steam_id_missing")
            return [], "steam_id_missing"

        use_profile_path = raw_id.isdigit()

        all_games: List[Dict[str, Any]] = []
        page = 0
        while True:
            if use_profile_path:
                candidate_urls = [
                    f"https://store.steampowered.com/wishlist/profiles/{raw_id}/wishlistdata/?p={page}",
                    f"https://store.steampowered.com/wishlist/profiles/{raw_id}/wishlistdata?p={page}",
                ]
            else:
                vanity = quote(raw_id, safe="")
                candidate_urls = [
                    f"https://store.steampowered.com/wishlist/id/{vanity}/wishlistdata/?p={page}",
                    f"https://store.steampowered.com/wishlist/id/{vanity}/wishlistdata?p={page}",
                ]

            payload: Optional[Dict[str, Any]] = None
            last_error: Optional[str] = None
            for url in candidate_urls:
                try:
                    payload = self._fetch_json(url)
                    break
                except RateLimitError:
                    self._set_error_detail("rate_limited")
                    return [], "wishlist_rate_limited"
                except RequestFailedError as err:
                    message = str(err)
                    self._set_error_detail(message)
                    last_error = message
                    if message in ("http_error_401", "http_error_403"):
                        return [], "wishlist_private"
                    continue

            if payload is None:
                if last_error and last_error.startswith("http_error_"):
                    return [], "wishlist_http_error"
                self._set_error_detail(last_error or "network_or_parse_error")
                return [], "wishlist_network_error"

            if not payload:
                break

            chunk: List[Dict[str, Any]] = []
            for appid_str, item in payload.items():
                appid = self._safe_int(appid_str)
                if appid is None:
                    continue
                if not isinstance(item, dict):
                    item = {}
                payload_item = {
                    "appid": appid,
                    "title": item.get("name", f"App {appid}"),
                    "order": self._safe_int(item.get("priority")) or len(all_games) + len(chunk) + 1,
                }
                wishlist_price = self._extract_public_price_snapshot(item)
                if wishlist_price:
                    payload_item["wishlist_price"] = wishlist_price
                chunk.append(payload_item)

            all_games.extend(chunk)
            if len(chunk) < 100:
                break
            page += 1

        normalized = self._normalize_games(all_games)
        if not normalized:
            self._set_error_detail("empty_or_private")
        return normalized, "ok"

    def fetch_wishlist(self, steam_id: str, steam_api_key: str, api_mode: str) -> Tuple[List[Dict[str, Any]], str]:
        self._set_error_detail("")
        mode = (api_mode or "auto").strip().lower()
        api_key = steam_api_key.strip()

        if mode == "official" and not api_key:
            return [], "steam_api_key_missing"

        if mode == "legacy":
            return self.fetch_public_wishlist(steam_id)

        if mode == "official":
            games, status = self.fetch_official_wishlist(steam_id, api_key)
            if status == "ok":
                return games, status
            return [], status

        # auto mode:
        # 1) Prefer public wishlist because it is the most complete for title/price hints.
        # 2) Fall back to official endpoints only when public failed.
        public_games, public_status = self.fetch_public_wishlist(steam_id)
        public_detail = self.last_error_detail
        if public_status == "ok":
            return public_games, public_status

        if public_status == "wishlist_rate_limited":
            return [], public_status

        if api_key:
            official_games, official_status = self.fetch_official_wishlist(steam_id, api_key)
            official_detail = self.last_error_detail
            if official_status == "ok":
                # Best effort: also try public once more to enrich titles, but keep official result stable.
                retry_public_games, retry_public_status = self.fetch_public_wishlist(steam_id)
                if retry_public_status == "ok" and retry_public_games:
                    return self._merge_wishlists(official_games, retry_public_games), "ok"
                self._set_error_detail(official_detail)
                return official_games, official_status
            if official_status == "wishlist_rate_limited":
                return [], official_status
            self._set_error_detail(official_detail or public_detail)
            return [], official_status

        # No key: use keyless official only as backup for appids.
        official_games, official_status = self.fetch_official_wishlist_without_key(steam_id)
        official_detail = self.last_error_detail
        if official_status == "ok":
            # Return official results directly; avoid a redundant public retry
            # that was adding a 6th sequential HTTP request in the worst case.
            self._set_error_detail(official_detail)
            return official_games, official_status
        if official_status == "wishlist_rate_limited":
            return [], official_status

        self._set_error_detail(public_detail or official_detail)
        return [], public_status

    def parse_manual_wishlist(self, manual_text: str) -> Tuple[List[Dict[str, Any]], str]:
        lines = [line.strip() for line in manual_text.splitlines() if line.strip()]
        games: List[Dict[str, Any]] = []

        for index, line in enumerate(lines, start=1):
            title = None
            appid_str = line
            for sep in ("|", ",", ";"):
                if sep in line:
                    left, right = line.split(sep, 1)
                    appid_str = left.strip()
                    title = right.strip()
                    break

            appid = self._safe_int(appid_str)
            if appid is None:
                continue

            games.append(
                {
                    "appid": appid,
                    "title": title or f"App {appid}",
                    "order": index,
                }
            )

        normalized = self._normalize_games(games)
        if not normalized:
            return [], "manual_wishlist_empty"
        return normalized, "ok"
