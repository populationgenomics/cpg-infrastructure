"""Tests for the SeqeraApiClient singleton HTTP client."""

from __future__ import annotations

from http import HTTPMethod, HTTPStatus
from unittest import TestCase
from unittest.mock import MagicMock, patch

from cpg_infra.driver.dynamic_providers.seqera.util.api_util import (
    SeqeraAPIError,
    SeqeraApiClient,
)


class SeqeraApiClientTestBase(TestCase):
    """Resets the class-level singleton between tests."""

    def setUp(self) -> None:
        SeqeraApiClient._instance = None

    def tearDown(self) -> None:
        SeqeraApiClient._instance = None


class TestSingletonBehaviour(SeqeraApiClientTestBase):
    def test_first_construction_requires_both_args(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SeqeraApiClient()
        self.assertIn('server_url', str(ctx.exception))
        self.assertIn('token_secret_name', str(ctx.exception))

    def test_first_construction_stores_config(self) -> None:
        client = SeqeraApiClient(
            server_url='https://seqera.example.com/api',
            token_secret_name='projects/p/secrets/s/versions/latest',
        )
        self.assertEqual(client.server_url, 'https://seqera.example.com/api')
        self.assertEqual(
            client.token_secret_name, 'projects/p/secrets/s/versions/latest',
        )

    def test_subsequent_construction_returns_same_instance(self) -> None:
        first = SeqeraApiClient(
            server_url='https://seqera.example.com/api',
            token_secret_name='projects/p/secrets/s/versions/latest',
        )
        second = SeqeraApiClient()
        self.assertIs(first, second)

    def test_subsequent_construction_ignores_new_args(self) -> None:
        first = SeqeraApiClient(
            server_url='https://original.example.com/api',
            token_secret_name='projects/p/secrets/original/versions/latest',
        )
        second = SeqeraApiClient(
            server_url='https://overridden.example.com/api',
            token_secret_name='projects/p/secrets/overridden/versions/latest',
        )
        self.assertIs(first, second)
        self.assertEqual(second.server_url, 'https://original.example.com/api')
        self.assertEqual(
            second.token_secret_name, 'projects/p/secrets/original/versions/latest',
        )


class TestAccessTokenCaching(SeqeraApiClientTestBase):
    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.util.api_util.secretmanager'
    )
    def test_access_token_fetches_from_secret_manager_once(
        self, mock_sm: MagicMock,
    ) -> None:
        mock_client = MagicMock()
        mock_client.access_secret_version.return_value.payload.data = b'tok-abc'
        mock_sm.SecretManagerServiceClient.return_value = mock_client

        client = SeqeraApiClient(
            server_url='https://seqera.example.com/api',
            token_secret_name='projects/p/secrets/s/versions/latest',
        )

        self.assertEqual(client._access_token, 'tok-abc')
        # cached_property: second access returns the cached value without
        # re-fetching from Secret Manager.
        self.assertEqual(client._access_token, 'tok-abc')
        mock_client.access_secret_version.assert_called_once_with(
            request={'name': 'projects/p/secrets/s/versions/latest'},
        )


class TestCall(SeqeraApiClientTestBase):
    def _configured_client(self, token: str = 'token') -> SeqeraApiClient:
        client = SeqeraApiClient(
            server_url='https://seqera.example.com/api',
            token_secret_name='projects/p/secrets/s/versions/latest',
        )

        client.__dict__['_access_token'] = token
        return client

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_success_returns_parsed_json(self, mock_requests: MagicMock) -> None:
        client = self._configured_client()
        response = MagicMock()
        response.content = b'{"ok": true}'
        response.json.return_value = {'ok': True}
        response.raise_for_status.return_value = None
        mock_requests.request.return_value = response

        result = client.call(HTTPMethod.GET, '/orgs/1')

        self.assertEqual(result, {'ok': True})
        mock_requests.request.assert_called_once_with(
            HTTPMethod.GET,
            'https://seqera.example.com/api/orgs/1',
            headers={'Authorization': 'Bearer token'},
            json=None,
            timeout=60,
        )

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_sends_json_body_when_data_provided(
        self, mock_requests: MagicMock,
    ) -> None:
        client = self._configured_client()
        response = MagicMock()
        response.content = b''
        response.raise_for_status.return_value = None
        mock_requests.request.return_value = response

        client.call(HTTPMethod.POST, '/orgs/1/workspaces', {'name': 'ws'})

        _, kwargs = mock_requests.request.call_args
        self.assertEqual(kwargs['json'], {'name': 'ws'})

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_returns_empty_dict_when_body_empty(
        self, mock_requests: MagicMock,
    ) -> None:
        client = self._configured_client()
        response = MagicMock()
        response.content = b''
        response.raise_for_status.return_value = None
        mock_requests.request.return_value = response

        self.assertEqual(client.call(HTTPMethod.DELETE, '/orgs/1/workspaces/42'), {})

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_raises_seqera_api_error_on_http_error(
        self, mock_requests: MagicMock,
    ) -> None:
        import requests as real_requests

        mock_requests.HTTPError = real_requests.HTTPError

        client = self._configured_client()
        response = MagicMock()
        response.status_code = HTTPStatus.NOT_FOUND
        response.text = '{"error": "not found"}'
        response.content = b'{"error": "not found"}'
        response.raise_for_status.side_effect = real_requests.HTTPError('404')
        mock_requests.request.return_value = response

        with self.assertRaises(SeqeraAPIError) as ctx:
            client.call(HTTPMethod.GET, '/orgs/1/workspaces/999')

        self.assertEqual(ctx.exception.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(ctx.exception.body, '{"error": "not found"}')
        self.assertEqual(ctx.exception.method, HTTPMethod.GET)