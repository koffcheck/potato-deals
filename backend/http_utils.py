import json
import ssl
import subprocess
import time
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class RateLimitError(Exception):
    pass


class RequestFailedError(Exception):
    pass


def _retry_after_seconds(headers: Optional[Any]) -> Optional[float]:
    if headers is None:
        return None
    try:
        raw = headers.get("Retry-After")
    except Exception:
        return None
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    return value


def _urlopen_body(request: Request, timeout: int, insecure: bool = False) -> str:
    if insecure:
        context = ssl._create_unverified_context()
        with urlopen(request, timeout=timeout, context=context) as response:
            return response.read().decode("utf-8")
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


def _curl_body(url: str, timeout: int, user_agent: str) -> str:
    output = subprocess.check_output(
        [
            "curl",
            "-fsSL",
            "--connect-timeout",
            str(max(2, min(timeout, 8))),
            "--max-time",
            str(max(3, timeout)),
            "-A",
            user_agent,
            "-H",
            "Accept: application/json,text/plain,*/*",
            "-H",
            "Accept-Language: en-US,en;q=0.9",
            "-H",
            "Cache-Control: no-cache",
            "-H",
            "Pragma: no-cache",
            "-H",
            "Referer: https://store.steampowered.com/",
            url,
        ],
        stderr=subprocess.STDOUT,
    )
    return output.decode("utf-8")


def _classify_network_error(err: Exception) -> str:
    if isinstance(err, json.JSONDecodeError):
        return "parse_error"
    if isinstance(err, TimeoutError):
        return "timeout"

    raw = str(err).lower()
    if "certificate verify failed" in raw or "ssl" in raw or "tls" in raw:
        return "ssl_error"
    if "could not resolve host" in raw or "name or service not known" in raw:
        return "dns_error"
    if "network is unreachable" in raw:
        return "network_unreachable"
    if "timed out" in raw or "timeout" in raw:
        return "timeout"
    return "network_or_parse_error"


def fetch_json_with_retry(
    url: str,
    *,
    timeout: int,
    user_agent: str,
    max_retries: int = 3,
    backoff_seconds: float = 0.6,
) -> Dict[str, Any]:
    body = fetch_text_with_retry(
        url,
        timeout=timeout,
        user_agent=user_agent,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )
    try:
        return json.loads(body)
    except json.JSONDecodeError as err:
        raise RequestFailedError("parse_error") from err


def fetch_text_with_retry(
    url: str,
    *,
    timeout: int,
    user_agent: str,
    max_retries: int = 3,
    backoff_seconds: float = 0.6,
) -> str:
    attempt = 0
    while True:
        request = Request(
            url,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": "https://store.steampowered.com/",
            },
        )
        try:
            used_curl = False
            try:
                body = _urlopen_body(request, timeout=timeout)
            except (URLError, ssl.SSLError) as err:
                reason = str(getattr(err, "reason", err) or err or "")
                ssl_related = isinstance(err, ssl.SSLError) or (
                    "CERTIFICATE_VERIFY_FAILED" in reason
                    or "SSL" in reason.upper()
                    or "TLS" in reason.upper()
                )
                if ssl_related:
                    try:
                        body = _urlopen_body(request, timeout=timeout, insecure=True)
                    except Exception:
                        used_curl = True
                        body = _curl_body(url, timeout, user_agent)
                else:
                    used_curl = True
                    body = _curl_body(url, timeout, user_agent)
            if not body and not used_curl:
                body = _curl_body(url, timeout, user_agent)
            return body
        except HTTPError as err:
            if err.code == 429 and attempt >= max_retries:
                raise RateLimitError("rate_limited") from err

            if err.code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                retry_after = _retry_after_seconds(err.headers)
                delay = retry_after if retry_after is not None else min(backoff_seconds * (2**attempt), 8.0)
                time.sleep(max(0.1, delay))
                attempt += 1
                continue

            if err.code == 429:
                raise RateLimitError("rate_limited") from err
            raise RequestFailedError(f"http_error_{err.code}") from err
        except (URLError, TimeoutError, OSError, ssl.SSLError, subprocess.SubprocessError) as err:
            if attempt < max_retries:
                delay = min(backoff_seconds * (2**attempt), 8.0)
                time.sleep(max(0.1, delay))
                attempt += 1
                continue
            raise RequestFailedError(_classify_network_error(err)) from err
