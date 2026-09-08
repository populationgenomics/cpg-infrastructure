import os
from functools import cached_property
from typing import ClassVar, Optional

import requests
from google.cloud import secretmanager

_SERVER_URL_ENV = 'SEQERA_SERVER_URL'
_TOKEN_SECRET_NAME_ENV = 'SEQERA_TOKEN_SECRET_NAME'  # noqa: S105


class SeqeraAPIError(Exception):
    """Exception raised when a Seqera Platform API call fails."""

    def __init__(self, method: str, url: str, code: int, body: str) -> None:
        super().__init__(f'{method} {url} failed with {code}: {body}')
        self.method = method
        self.url = url
        self.status_code = code
        self.body = body


class SeqeraApiClient:
    """A singleton HTTP client to call Seqera Platform API - Used by Dynamic Resource Providers."""

    _instance: ClassVar['SeqeraApiClient | None'] = None

    def __init__(self, server_url: str, token_secret_name: str) -> None:
        self.server_url = server_url
        self.token_secret_name = token_secret_name

    @classmethod
    def _get(cls: type['SeqeraApiClient']) -> 'SeqeraApiClient':
        """Return the process-wide singleton, building it from env vars if needed."""
        if cls._instance is None:
            server_url = os.environ.get(_SERVER_URL_ENV)
            token_secret_name = os.environ.get(_TOKEN_SECRET_NAME_ENV)
            if not server_url or not token_secret_name:
                raise RuntimeError(
                    f'{_SERVER_URL_ENV} and {_TOKEN_SECRET_NAME_ENV} '
                    'must be set before using SeqeraApiClient.'
                )
            cls._instance = cls(
                server_url=server_url, token_secret_name=token_secret_name
            )
        assert cls._instance is not None
        return cls._instance

    @cached_property
    def _access_token(self) -> str:
        client = secretmanager.SecretManagerServiceClient()
        resp = client.access_secret_version(request={'name': self.token_secret_name})
        return resp.payload.data.decode('utf-8')

    @classmethod
    def call(
        cls: type['SeqeraApiClient'],
        method: str,
        path: str,
        data: Optional[dict] = None,
    ) -> dict:
        """Call Seqera Platform API."""
        client = cls._get()
        url = f'{client.server_url}{path}'
        headers = {'Authorization': f'Bearer {client._access_token}'}  # noqa: SLF001

        response = requests.request(method, url, headers=headers, json=data, timeout=60)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise SeqeraAPIError(
                method,
                url,
                response.status_code,
                response.text,
            ) from e

        return response.json() if response.content else {}
