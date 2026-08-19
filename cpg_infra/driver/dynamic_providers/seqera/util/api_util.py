import json
import os
import urllib.error
import urllib.request
from typing import Optional


_SERVER_URL: Optional[str] = None
_BEARER_AUTH = os.environ.get("SEQERA_BEARER_AUTH") #TODO read from env variable or secret manager


def configure(server_url: str) -> None:
    global _SERVER_URL
    _SERVER_URL = server_url


class SeqeraAPIError(RuntimeError):
    """Exception raised when a Seqera Platform API call fails."""

    def __init__(self, method: str, url: str, code: int, body: str, cause: Exception):
        super().__init__(f"{method} {url} failed with {code}: {body}")
        self.method = method
        self.url = url
        self.status_code = code
        self.body = body
        self.__cause__ = cause


def call_seqera_api(
    method: str,
    path: str,
    data: Optional[dict] = None,
) -> dict:
    """Call Seqera Platform API."""
    if _SERVER_URL is None:
        raise RuntimeError("Seqera API URL not set")
    if _BEARER_AUTH is None:
        raise RuntimeError(
            "SEQERA_BEARER_AUTH not set"
        )
    url = f"{_SERVER_URL}{path}"
    headers = {"Authorization": f"Bearer {_BEARER_AUTH}"}
    request_body = None
    if data is not None:
        request_body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=request_body, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            response_body = response.read()
            return json.loads(response_body.decode("utf-8")) if response_body else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise SeqeraAPIError(method, url, e.code, body, e) from e