from functools import cached_property
from typing import ClassVar, Optional

import requests
from google.cloud import secretmanager


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


class SeqeraApiClient:
    """A singleton HTTP client to call Seqera Platform API - Used by Dynamic Resource Providers."""

    _instance: ClassVar['SeqeraApiClient | None'] = None
    server_url: str
    token_secret_name: str

    def __new__(
        cls,
        server_url: Optional[str] = None,
        token_secret_name: Optional[str] = None,
    ) -> 'SeqeraApiClient':
        instance = cls._instance
        if instance is None:
            if server_url is None or token_secret_name is None:
                raise RuntimeError(
                    'SeqeraApiClient first construction requires both '
                    'server_url and token_secret_name.'
                )
            instance = super().__new__(cls)
            instance.server_url = server_url
            instance.token_secret_name = token_secret_name
            cls._instance = instance
        return instance

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    @cached_property
    def _access_token(self) -> str:
        client = secretmanager.SecretManagerServiceClient()
        resp = client.access_secret_version(request={'name': self.token_secret_name})
        return resp.payload.data.decode('utf-8')

    def call(
        self,
        method: str,
        path: str,
        data: Optional[dict] = None,
    ) -> dict:
        """Call Seqera Platform API."""
        url = f'{self.server_url}{path}'
        headers = {'Authorization': f'Bearer {self._access_token}'}

        response = requests.request(method, url, headers=headers, json=data, timeout=60)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise SeqeraAPIError(
                method,
                url,
                response.status_code,
                response.text,
                e,
            ) from e

        return response.json() if response.content else {}
