import copy
import json
import os
import tempfile
import threading
from datetime import datetime, timezone
from typing import Any, Dict


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_data() -> Dict[str, Any]:
    return {
        "settings": {
            "language": "en",
            "currency": "USD",
            "region": "us",
            "wishlist_mode": "steam",
            "steam_id": "",
            "steam_api_key": "",
            "wishlist_api_mode": "auto",
            "manual_wishlist": "",
            "auto_refresh_minutes": 60,
            "discount_notify_threshold": 50,
            "view_mode": "compact",
            "sort_mode": "wishlist",
            "filter_on_sale": False,
            "filter_never_discounted": False,
            "filter_price_min": None,
            "filter_price_max": None,
            "itad_api_key": "",
        },
        "wishlist": [],
        "price_cache": {},
        "alerts": {},
        "rates_cache": {
            "base": "USD",
            "rates": {"USD": 1.0},
            "updated_at": None,
        },
        "meta": {
            "wishlist_last_sync": None,
            "price_last_sync": None,
            "rates_last_sync": None,
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        },
    }


class DataStore:
    def __init__(self, base_dir: str, filename: str = "potato_data.json") -> None:
        self.base_dir = base_dir
        self.filename = filename
        self.file_path = os.path.join(base_dir, filename)
        self._lock = threading.Lock()
        self._ensure_storage()

    def _backup_current_file(self, reason: str) -> None:
        if not os.path.exists(self.file_path):
            return
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = f"{self.file_path}.{reason}.{timestamp}.bak"
        try:
            os.replace(self.file_path, backup_path)
        except OSError:
            pass

    def _ensure_storage(self) -> None:
        try:
            self._ensure_file()
        except OSError:
            # Last-resort fallback to avoid plugin crashes on permission issues.
            fallback_dir = os.path.join(tempfile.gettempdir(), "potato-deals")
            self.base_dir = fallback_dir
            self.file_path = os.path.join(fallback_dir, self.filename)
            self._ensure_file()

    def _ensure_file(self) -> None:
        os.makedirs(self.base_dir, exist_ok=True)
        # Clean stale temp file left from an interrupted previous write.
        temp_path = f"{self.file_path}.tmp"
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        if os.path.exists(self.file_path):
            try:
                if os.path.getsize(self.file_path) == 0:
                    self._backup_current_file("empty")
            except OSError:
                pass
        if not os.path.exists(self.file_path):
            self._atomic_write(default_data())

    def _atomic_write(self, data: Dict[str, Any]) -> None:
        os.makedirs(self.base_dir, exist_ok=True)
        temp_path = f"{self.file_path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
        os.replace(temp_path, self.file_path)

    def _merge_defaults(self, data: Dict[str, Any]) -> Dict[str, Any]:
        merged = default_data()
        for key in ("settings", "wishlist", "price_cache", "alerts", "rates_cache", "meta"):
            if key in data:
                if isinstance(merged[key], dict) and isinstance(data[key], dict):
                    merged[key].update(data[key])
                else:
                    merged[key] = data[key]
        merged["meta"]["updated_at"] = merged["meta"].get("updated_at") or utc_now_iso()
        return merged

    def load(self) -> Dict[str, Any]:
        with self._lock:
            try:
                with open(self.file_path, "r", encoding="utf-8") as handle:
                    raw = json.load(handle)
                if not isinstance(raw, dict):
                    self._backup_current_file("invalid_root")
                    raw = default_data()
                    try:
                        self._atomic_write(raw)
                    except OSError:
                        pass
            except (FileNotFoundError, json.JSONDecodeError, OSError):
                if os.path.exists(self.file_path):
                    self._backup_current_file("corrupt")
                raw = default_data()
                try:
                    self._atomic_write(raw)
                except OSError:
                    pass
            merged = self._merge_defaults(raw)
            return copy.deepcopy(merged)

    def save(self, data: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            normalized = self._merge_defaults(data)
            normalized["meta"]["updated_at"] = utc_now_iso()
            try:
                self._atomic_write(normalized)
            except OSError:
                pass
            return copy.deepcopy(normalized)
