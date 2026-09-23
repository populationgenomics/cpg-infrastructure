"""Tests for the SeqeraApiClient singleton HTTP client."""

from __future__ import annotations

import os
from http import HTTPMethod, HTTPStatus
from unittest import TestCase
from unittest.mock import MagicMock, patch

from cpg_infra.driver.dynamic_providers.seqera.util.api_util import (
    SeqeraApiClient,
    SeqeraAPIError,
)

_TEST_SERVER_URL = 'https://seqera.example.com/api'
_TEST_TOKEN_SECRET_NAME = 'projects/p/secrets/s/versions/latest'  # noqa: S105


class SeqeraApiClientTestBase(TestCase):
    """Resets the class-level singleton and env vars between tests."""

    def setUp(self) -> None:
        SeqeraApiClient._instance = None  # noqa: SLF001
        os.environ['SEQERA_SERVER_URL'] = _TEST_SERVER_URL
        os.environ['SEQERA_TOKEN_SECRET_NAME'] = _TEST_TOKEN_SECRET_NAME

    def tearDown(self) -> None:
        SeqeraApiClient._instance = None  # noqa: SLF001
        os.environ.pop('SEQERA_SERVER_URL', None)
        os.environ.pop('SEQERA_TOKEN_SECRET_NAME', None)


class TestSingletonResolution(SeqeraApiClientTestBase):
    def test_first_use_builds_instance_from_env(self) -> None:
        instance = SeqeraApiClient._get()  # noqa: SLF001
        self.assertEqual(instance.server_url, _TEST_SERVER_URL)
        self.assertEqual(instance.token_secret_name, _TEST_TOKEN_SECRET_NAME)

    def test_repeat_use_returns_same_instance(self) -> None:
        first = SeqeraApiClient._get()  # noqa: SLF001
        second = SeqeraApiClient._get()  # noqa: SLF001
        self.assertIs(first, second)

    def test_env_changes_after_first_use_are_ignored(self) -> None:
        first = SeqeraApiClient._get()  # noqa: SLF001
        os.environ['SEQERA_SERVER_URL'] = 'https://overridden.example.com/api'
        os.environ['SEQERA_TOKEN_SECRET_NAME'] = (
            'projects/p/secrets/other/versions/latest'  # noqa: S105
        )
        second = SeqeraApiClient._get()  # noqa: SLF001
        self.assertIs(first, second)
        self.assertEqual(second.server_url, _TEST_SERVER_URL)

    def test_missing_env_raises(self) -> None:
        os.environ.pop('SEQERA_SERVER_URL', None)
        os.environ.pop('SEQERA_TOKEN_SECRET_NAME', None)
        with self.assertRaises(RuntimeError) as ctx:
            SeqeraApiClient._get()  # noqa: SLF001
        self.assertIn('SEQERA_SERVER_URL', str(ctx.exception))
        self.assertIn('SEQERA_TOKEN_SECRET_NAME', str(ctx.exception))

    def test_missing_token_secret_name_raises(self) -> None:
        os.environ.pop('SEQERA_TOKEN_SECRET_NAME', None)
        with self.assertRaises(RuntimeError):
            SeqeraApiClient._get()  # noqa: SLF001

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.secretmanager')
    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_lazy_initializes_singleton_from_env(
        self,
        mock_requests: MagicMock,
        mock_sm: MagicMock,
    ) -> None:
        """The classmethod facade must build the singleton on first use — no
        prior _get() required. This is what makes the Pulumi dynamic-provider
        subprocess work end-to-end: fresh interpreter -> bare
        SeqeraApiClient.call(...) resolves via env vars."""
        mock_sm_client = MagicMock()
        mock_sm_client.access_secret_version.return_value.payload.data = b'tok'
        mock_sm.SecretManagerServiceClient.return_value = mock_sm_client
        response = MagicMock()
        response.content = b'{}'
        response.json.return_value = {}
        response.raise_for_status.return_value = None
        mock_requests.request.return_value = response

        self.assertIsNone(SeqeraApiClient._instance)  # noqa: SLF001
        SeqeraApiClient.call(HTTPMethod.GET, '/orgs/1')

        self.assertIsNotNone(SeqeraApiClient._instance)  # noqa: SLF001
        # And _get() returns the same instance the classmethod built.
        self.assertIs(SeqeraApiClient._instance, SeqeraApiClient._get())  # noqa: SLF001


class TestAccessTokenCaching(SeqeraApiClientTestBase):
    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.secretmanager')
    def test_access_token_fetches_from_secret_manager_once(
        self,
        mock_sm: MagicMock,
    ) -> None:
        mock_client = MagicMock()
        mock_client.access_secret_version.return_value.payload.data = b'tok-abc'
        mock_sm.SecretManagerServiceClient.return_value = mock_client

        instance = SeqeraApiClient._get()  # noqa: SLF001

        self.assertEqual(instance._access_token, 'tok-abc')  # noqa: SLF001
        # cached_property: second access returns the cached value without
        # re-fetching from Secret Manager.
        self.assertEqual(instance._access_token, 'tok-abc')  # noqa: SLF001
        mock_client.access_secret_version.assert_called_once_with(
            request={'name': _TEST_TOKEN_SECRET_NAME},
        )


class TestCall(SeqeraApiClientTestBase):
    def setUp(self) -> None:
        super().setUp()
        # Prepopulate access token so tests don't hit Secret Manager.
        SeqeraApiClient._instance = SeqeraApiClient(  # noqa: SLF001
            server_url=_TEST_SERVER_URL,
            token_secret_name=_TEST_TOKEN_SECRET_NAME,
        )
        SeqeraApiClient._instance.__dict__['_access_token'] = 'token'  # noqa: SLF001, S105

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_success_returns_parsed_json(self, mock_requests: MagicMock) -> None:
        response = MagicMock()
        response.content = b'{"ok": true}'
        response.json.return_value = {'ok': True}
        response.raise_for_status.return_value = None
        mock_requests.request.return_value = response

        result = SeqeraApiClient.call(HTTPMethod.GET, '/orgs/1')

        self.assertEqual(result, {'ok': True})
        mock_requests.request.assert_called_once_with(
            HTTPMethod.GET,
            f'{_TEST_SERVER_URL}/orgs/1',
            headers={'Authorization': 'Bearer token'},
            json=None,
            timeout=60,
        )

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_sends_json_body_when_data_provided(
        self,
        mock_requests: MagicMock,
    ) -> None:
        response = MagicMock()
        response.content = b''
        response.raise_for_status.return_value = None
        mock_requests.request.return_value = response

        SeqeraApiClient.call(HTTPMethod.POST, '/orgs/1/workspaces', {'name': 'ws'})

        _, kwargs = mock_requests.request.call_args
        self.assertEqual(kwargs['json'], {'name': 'ws'})

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_returns_empty_dict_when_body_empty(
        self,
        mock_requests: MagicMock,
    ) -> None:
        response = MagicMock()
        response.content = b''
        response.raise_for_status.return_value = None
        mock_requests.request.return_value = response

        self.assertEqual(
            SeqeraApiClient.call(HTTPMethod.DELETE, '/orgs/1/workspaces/42'), {}
        )

    @patch('cpg_infra.driver.dynamic_providers.seqera.util.api_util.requests')
    def test_call_raises_seqera_api_error_on_http_error(
        self,
        mock_requests: MagicMock,
    ) -> None:
        import requests as real_requests

        mock_requests.HTTPError = real_requests.HTTPError

        response = MagicMock()
        response.status_code = HTTPStatus.NOT_FOUND
        response.text = '{"error": "not found"}'
        response.content = b'{"error": "not found"}'
        response.raise_for_status.side_effect = real_requests.HTTPError('404')
        mock_requests.request.return_value = response

        with self.assertRaises(SeqeraAPIError) as ctx:
            SeqeraApiClient.call(HTTPMethod.GET, '/orgs/1/workspaces/999')

        self.assertEqual(ctx.exception.status_code, HTTPStatus.NOT_FOUND)
        self.assertEqual(ctx.exception.body, '{"error": "not found"}')
        self.assertEqual(ctx.exception.method, HTTPMethod.GET)
