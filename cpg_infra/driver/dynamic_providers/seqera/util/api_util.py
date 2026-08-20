import os
from typing import Optional

import requests

_SERVER_URL: Optional[str] = None
_BEARER_AUTH = os.environ.get(
    'SEQERA_BEARER_AUTH'
)  # TODO read from env variable or secret manager


def configure(server_url: str) -> None:
    global _SERVER_URL  # TODO a better option than a global variable.
    _SERVER_URL = server_url


class SeqeraAPIError(RuntimeError):
    """Exception raised when a Seqera Platform API call fails."""

    def __init__(
        self, method: str, url: str, code: int, body: str, cause: Exception
    ) -> None:
        super().__init__(f'{method} {url} failed with {code}: {body}')
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
        raise RuntimeError('Seqera API URL not set')
    if _BEARER_AUTH is None:
        raise RuntimeError('SEQERA_BEARER_AUTH not set')
    url = f'{_SERVER_URL}{path}'
    headers = {'Authorization': f'Bearer {_BEARER_AUTH}'}

    response = requests.request(method, url, headers=headers, json=data, timeout=60)

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise SeqeraAPIError(method, url, response.status_code, response.text, e) from e

    return response.json() if response.content else {}
