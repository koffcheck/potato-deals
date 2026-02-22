import asyncio
import importlib
import json
import os
import shutil
import sys
import tempfile
import traceback
from typing import Any, Dict, List, Optional


class Plugin:
    service: Any
    _startup_error: Optional[str]
    _startup_traceback: str
    _storage_dir: str

    def _safe_remove_file(self, path: str) -> None:
        try:
            if os.path.isfile(path):
                os.remove(path)
        except Exception:
            pass

    def _read_json_file(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                return data
            return None
        except Exception:
            return None

    def _files_have_equal_json(self, left_path: str, right_path: str) -> bool:
        left_data = self._read_json_file(left_path)
        if left_data is None:
            return False
        right_data = self._read_json_file(right_path)
        if right_data is None:
            return False
        return left_data == right_data

    def _is_writable_dir(self, path: str) -> bool:
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, ".potato_write_probe")
            with open(probe, "w", encoding="utf-8") as handle:
                handle.write("ok")
            os.remove(probe)
            return True
        except Exception:
            return False

    def _possible_storage_dirs(self, plugin_dir: str) -> List[str]:
        candidates: List[str] = []

        def add(path: Any) -> None:
            value = str(path or "").strip()
            if value and value not in candidates:
                candidates.append(value)

        # Prefer Decky-provided persistent dirs to avoid writing into plugin code dir.
        add(os.environ.get("DECKY_PLUGIN_RUNTIME_DIR", ""))
        add(os.environ.get("DECKY_PLUGIN_SETTINGS_DIR", ""))

        try:
            import decky  # type: ignore

            add(getattr(decky, "DECKY_PLUGIN_RUNTIME_DIR", ""))
            add(getattr(decky, "DECKY_PLUGIN_SETTINGS_DIR", ""))
        except Exception:
            pass

        add(plugin_dir)
        add(os.path.join(tempfile.gettempdir(), "potato-deals"))
        return candidates

    def _remove_dir_if_empty(self, path: str) -> None:
        try:
            if os.path.isdir(path) and not os.listdir(path):
                os.rmdir(path)
        except Exception:
            pass

    def _cleanup_transient_files(self, path: str, remove_startup_log: bool = False) -> None:
        if not path:
            return
        self._safe_remove_file(os.path.join(path, "potato_data.json.tmp"))
        self._safe_remove_file(os.path.join(path, "potato_data.json.bak"))
        if remove_startup_log:
            self._safe_remove_file(os.path.join(path, "startup_error.log"))

    def _cleanup_legacy_named_dirs(self, storage_dir: str) -> None:
        parent_dir = os.path.dirname(storage_dir)
        if not parent_dir or not os.path.isdir(parent_dir):
            return

        current_name = os.path.basename(storage_dir).lower()
        legacy_names = {
            "potata-deals",
            "potata deals",
            "potato_deals",
            "potatodeals",
            "potatadeals",
        }
        if current_name in legacy_names:
            legacy_names.discard(current_name)

        target_file = os.path.join(storage_dir, "potato_data.json")
        for entry in os.listdir(parent_dir):
            if entry.lower() not in legacy_names:
                continue
            candidate_dir = os.path.join(parent_dir, entry)
            if not os.path.isdir(candidate_dir):
                continue
            self._cleanup_transient_files(candidate_dir)
            candidate_file = os.path.join(candidate_dir, "potato_data.json")

            if os.path.isfile(candidate_file) and not os.path.exists(target_file):
                try:
                    os.makedirs(storage_dir, exist_ok=True)
                    shutil.copy2(candidate_file, target_file)
                except Exception:
                    pass

            if os.path.isfile(candidate_file) and os.path.isfile(target_file):
                if self._files_have_equal_json(candidate_file, target_file):
                    self._safe_remove_file(candidate_file)

            self._remove_dir_if_empty(candidate_dir)

    def _cleanup_bytecode_cache(self, plugin_dir: str) -> None:
        for path in (
            os.path.join(plugin_dir, "__pycache__"),
            os.path.join(plugin_dir, "backend", "__pycache__"),
        ):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
            except Exception:
                pass

    def _clear_previous_startup_logs(self, plugin_dir: str, storage_dir: str) -> None:
        for path in {
            plugin_dir,
            storage_dir,
            os.path.join(tempfile.gettempdir(), "potato-deals"),
        }:
            self._cleanup_transient_files(path, remove_startup_log=True)

    def _set_startup_error(self, error: Exception, storage_dir: str, plugin_dir: str) -> None:
        self._startup_error = f"backend_init_failed:{error.__class__.__name__}:{error}"
        self._startup_traceback = traceback.format_exc(limit=20)
        self._storage_dir = storage_dir

        fallback_dirs = [
            storage_dir,
            plugin_dir,
            os.path.join(tempfile.gettempdir(), "potato-deals"),
        ]
        for target_dir in fallback_dirs:
            if not target_dir:
                continue
            try:
                os.makedirs(target_dir, exist_ok=True)
                with open(os.path.join(target_dir, "startup_error.log"), "w", encoding="utf-8") as handle:
                    handle.write(f"error={self._startup_error}\n")
                    handle.write(f"storage_dir={storage_dir}\n")
                    handle.write(f"plugin_dir={plugin_dir}\n")
                    handle.write("traceback_begin\n")
                    handle.write(self._startup_traceback)
                    handle.write("traceback_end\n")
                break
            except Exception:
                continue

    def _resolve_storage_dir(self, plugin_dir: str) -> str:
        for candidate in self._possible_storage_dirs(plugin_dir):
            if not self._is_writable_dir(candidate):
                continue

            legacy_file = os.path.join(plugin_dir, "potato_data.json")
            target_file = os.path.join(candidate, "potato_data.json")
            if os.path.isfile(legacy_file) and not os.path.exists(target_file):
                try:
                    os.makedirs(candidate, exist_ok=True)
                    shutil.copy2(legacy_file, target_file)
                except Exception:
                    pass
            return candidate

        return os.path.join(tempfile.gettempdir(), "potato-deals")

    def _cleanup_stale_files(self, plugin_dir: str, storage_dir: str) -> None:
        # One-time hygiene on startup: remove stale temp files from interrupted writes.
        self._cleanup_transient_files(plugin_dir)
        self._cleanup_transient_files(storage_dir)
        self._cleanup_transient_files(os.path.join(tempfile.gettempdir(), "potato-deals"))
        self._cleanup_bytecode_cache(plugin_dir)

        # Prefer runtime storage as the single source of truth if both files exist.
        if storage_dir != plugin_dir:
            legacy_file = os.path.join(plugin_dir, "potato_data.json")
            runtime_file = os.path.join(storage_dir, "potato_data.json")
            if os.path.isfile(legacy_file) and os.path.isfile(runtime_file):
                # Remove only obvious duplicate legacy file to avoid deleting user data.
                if self._files_have_equal_json(legacy_file, runtime_file):
                    self._safe_remove_file(legacy_file)

        self._cleanup_legacy_named_dirs(storage_dir)

    def _load_service_class(self, plugin_dir: str) -> Any:
        backend_dir = os.path.join(plugin_dir, "backend")
        if not os.path.isdir(backend_dir):
            raise FileNotFoundError(f"backend_dir_missing:{backend_dir}")

        # Decky can start python with another CWD, so imports from plugin root
        # must be pinned explicitly.
        if plugin_dir not in sys.path:
            sys.path.insert(0, plugin_dir)

        existing_backend = sys.modules.get("backend")
        if existing_backend is not None:
            candidates = []
            existing_file = str(getattr(existing_backend, "__file__", "") or "")
            if existing_file:
                candidates.append(existing_file)
            existing_paths = getattr(existing_backend, "__path__", None)
            if existing_paths:
                candidates.extend(str(path) for path in existing_paths)

            plugin_real = os.path.realpath(plugin_dir)
            same_plugin_backend = any(
                os.path.realpath(candidate).startswith(plugin_real)
                for candidate in candidates
                if candidate
            )
            if not same_plugin_backend:
                for module_name in list(sys.modules.keys()):
                    if module_name == "backend" or module_name.startswith("backend."):
                        sys.modules.pop(module_name, None)

        module = importlib.import_module("backend.service")
        service_class = getattr(module, "PotatoDealsService", None)
        if service_class is None:
            raise RuntimeError("potato_service_class_missing")
        return service_class

    async def _main(self) -> None:
        self.service = None
        self._startup_error = None
        self._startup_traceback = ""
        self._storage_dir = ""
        plugin_dir = os.path.dirname(os.path.realpath(__file__))
        storage_dir = plugin_dir
        try:
            storage_dir = self._resolve_storage_dir(plugin_dir)
            self._cleanup_stale_files(plugin_dir, storage_dir)
            service_class = self._load_service_class(plugin_dir)
            self.service = service_class(storage_dir)
            self._storage_dir = storage_dir
            self._clear_previous_startup_logs(plugin_dir, storage_dir)
        except Exception as err:  # pylint: disable=broad-except
            self._set_startup_error(err, storage_dir, plugin_dir)

    async def _unload(self) -> None:
        pass

    def _dispatch_action(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self._startup_error:
            if action == "get_startup_error":
                return {
                    "error": self._startup_error,
                    "traceback": self._startup_traceback,
                    "storage_dir": self._storage_dir,
                }
            raise RuntimeError(self._startup_error)
        if self.service is None:
            raise RuntimeError("backend_not_ready")
        if action == "ping":
            return self.service.ping()
        if action == "get_bootstrap":
            return self.service.get_bootstrap()
        if action == "get_sales_events":
            return self.service.get_sales_events()
        if action == "get_settings":
            return self.service.get_settings()
        if action == "save_settings":
            return self.service.save_settings(payload.get("patch", {}))
        if action == "set_settings":
            return self.service.set_settings(payload.get("patch", {}))
        if action == "get_manual_list":
            return self.service.get_manual_list()
        if action == "set_manual_list":
            return self.service.set_manual_list(str(payload.get("manual_wishlist") or ""))
        if action == "sync_wishlist":
            return self.service.sync_wishlist(
                mode=payload.get("mode"),
                steam_id=payload.get("steam_id"),
                steam_api_key=payload.get("steam_api_key"),
                wishlist_api_mode=payload.get("wishlist_api_mode"),
                manual_wishlist=payload.get("manual_wishlist"),
            )
        if action == "update_prices":
            return self.service.update_prices()
        if action == "update_currency_rates":
            return self.service.update_currency_rates(force=bool(payload.get("force", False)))
        if action == "update_rates":
            return self.service.update_currency_rates(force=bool(payload.get("force", False)))
        if action == "clear_cache":
            return self.service.clear_cache()
        if action == "set_game_preferences":
            try:
                appid = int(payload.get("appid", 0))
            except (TypeError, ValueError):
                raise ValueError("invalid_appid") from None
            if appid <= 0:
                raise ValueError("invalid_appid")
            return self.service.set_game_preferences(
                appid=appid,
                pinned=payload.get("pinned"),
                notify_enabled=payload.get("notify_enabled"),
            )
        if action == "set_price_alert":
            try:
                appid = int(payload.get("appid", 0))
            except (TypeError, ValueError):
                raise ValueError("invalid_appid") from None
            return self.service.set_price_alert(
                appid=appid,
                target_price=payload.get("target_price"),
                target_currency=str(payload.get("target_currency") or ""),
                active=bool(payload.get("active", True)),
            )
        if action == "clear_price_alert":
            try:
                appid = int(payload.get("appid", 0))
            except (TypeError, ValueError):
                raise ValueError("invalid_appid") from None
            return self.service.clear_price_alert(appid=appid)
        if action == "analyze_deals":
            return self.service.analyze_deals(
                query=str(payload.get("query") or ""),
                history=payload.get("history"),
            )
        if action == "get_startup_error":
            return {
                "error": None,
                "traceback": "",
                "storage_dir": self._storage_dir,
            }
        raise ValueError("unknown_action")

    async def _run_dispatch(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Keep backend calls on the main plugin event loop for maximum compatibility
        # with Decky environments where thread workers can stall IPC.
        return self._dispatch_action(action, payload)

    async def _safe(self, action: str, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        normalized_payload = payload or {}
        try:
            # Heavy network/file work is executed in a worker thread to keep Decky event loop responsive.
            result = await self._run_dispatch(action, normalized_payload)
            return {"success": True, "result": result}
        except Exception as err:  # pylint: disable=broad-except
            return {"success": False, "error": str(err)}

    async def get_bootstrap(self) -> Dict[str, Any]:
        return await self._safe("get_bootstrap", {})

    async def get_sales_events(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("get_sales_events", payload)

    async def save_settings(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("save_settings", payload)

    async def sync_wishlist(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("sync_wishlist", payload)

    async def update_prices(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("update_prices", payload)

    async def update_currency_rates(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("update_currency_rates", payload)

    async def clear_cache(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("clear_cache", payload)

    async def set_game_preferences(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("set_game_preferences", payload)

    async def set_price_alert(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("set_price_alert", payload)

    async def clear_price_alert(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("clear_price_alert", payload)

    async def analyze_deals(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("analyze_deals", payload)

    async def get_startup_error(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self._safe("get_startup_error", payload)
