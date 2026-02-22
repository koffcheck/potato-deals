import os
import re
from typing import Any, Dict, List, Optional


def _possible_loginusers_paths() -> List[str]:
    home = os.path.expanduser("~")
    return [
        os.path.join(home, ".local", "share", "Steam", "config", "loginusers.vdf"),
        os.path.join(home, ".steam", "steam", "config", "loginusers.vdf"),
    ]


def detect_steam_id() -> Optional[str]:
    best_user: Optional[Dict[str, Any]] = None

    for path in _possible_loginusers_paths():
        if not os.path.exists(path):
            continue

        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as handle:
                text = handle.read()
        except OSError:
            continue

        users = re.finditer(r'"(\d{17})"\s*\{([^{}]*)\}', text, re.S)
        for match in users:
            steam_id = match.group(1)
            block = match.group(2)

            most_recent = re.search(r'"MostRecent"\s+"(\d+)"', block)
            allow_auto_login = re.search(r'"AllowAutoLogin"\s+"(\d+)"', block)
            timestamp = re.search(r'"Timestamp"\s+"(\d+)"', block)

            candidate = {
                "steam_id": steam_id,
                "most_recent": int(most_recent.group(1)) if most_recent else 0,
                "allow_auto_login": int(allow_auto_login.group(1)) if allow_auto_login else 0,
                "timestamp": int(timestamp.group(1)) if timestamp else 0,
            }

            if best_user is None:
                best_user = candidate
                continue

            rank = (
                candidate["most_recent"],
                candidate["allow_auto_login"],
                candidate["timestamp"],
            )
            best_rank = (
                best_user["most_recent"],
                best_user["allow_auto_login"],
                best_user["timestamp"],
            )
            if rank > best_rank:
                best_user = candidate

    if not best_user:
        return None
    return best_user["steam_id"]


def _possible_config_paths() -> List[str]:
    home = os.path.expanduser("~")
    return [
        os.path.join(home, ".local", "share", "Steam", "config", "config.vdf"),
        os.path.join(home, ".steam", "steam", "config", "config.vdf"),
        os.path.join(home, ".local", "share", "Steam", "registry.vdf"),
        os.path.join(home, ".steam", "registry.vdf"),
    ]


def detect_steam_region() -> Optional[str]:
    """Try to detect the Steam store region from local Steam config files."""
    # Mapping from common Steam language/country values to our region codes
    country_to_region: Dict[str, str] = {
        "US": "us", "DE": "eu", "FR": "eu", "IT": "eu", "ES": "eu",
        "AT": "eu", "NL": "eu", "BE": "eu", "PT": "eu", "FI": "eu",
        "IE": "eu", "SK": "eu", "SI": "eu", "GR": "eu", "LU": "eu",
        "GB": "gb", "RU": "ru", "KZ": "kz", "UZ": "uz", "TR": "tr",
        "AR": "ar", "UA": "ua", "BR": "br", "CN": "cn", "IN": "in",
    }

    for path in _possible_config_paths():
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as handle:
                text = handle.read()
        except OSError:
            continue

        # Look for country code in config.vdf (WebStorage or ConnectCache)
        country_match = re.search(
            r'"(?:country|CountryCode|WebStorage)"[^{]*?"([A-Z]{2})"',
            text, re.IGNORECASE
        )
        if country_match:
            code = country_match.group(1).upper()
            region = country_to_region.get(code)
            if region:
                return region
            # If country code is 2 letters and in our store mapping, use directly
            if len(code) == 2:
                return code.lower()

        # Fallback: derive from language setting
        lang_match = re.search(r'"language"\s+"(\w+)"', text, re.IGNORECASE)
        if lang_match:
            lang = lang_match.group(1).lower()
            lang_region = {
                "russian": "ru", "english": "us", "german": "eu",
                "french": "eu", "spanish": "eu", "portuguese": "br",
                "schinese": "cn", "tchinese": "cn", "turkish": "tr",
                "ukrainian": "ua", "brazilian": "br",
            }
            if lang in lang_region:
                return lang_region[lang]

    return None

