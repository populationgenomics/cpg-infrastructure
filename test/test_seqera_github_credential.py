"""Tests for the SeqeraGithubCredential dynamic provider."""

from __future__ import annotations

from http import HTTPStatus
from unittest import TestCase
from unittest.mock import MagicMock, patch

from cpg_infra.driver.dynamic_providers.seqera.resources.seqera_github_credential import (
    _GithubCredentialProvider,
)
from cpg_infra.driver.dynamic_providers.seqera.util.api_util import SeqeraAPIError

_SECRET_NAME = 'projects/1/secrets/gh-pat'  # noqa: S105
_VERSION_V7 = f'{_SECRET_NAME}/versions/7'
_VERSION_V8 = f'{_SECRET_NAME}/versions/8'
_TOKEN = 'ghp_fake'  # noqa: S105
_CRED_ID = 'cred-abc'

_INPUTS = {
    'workspace_id': 123,
    'name': 'cpg-github-auth-token',
    'username': 'cpg-github-bot',
    'access_token_secret_name': _SECRET_NAME,
    'base_url': 'https://github.com/organizations/populationgenomics',
    'resolved_secret_version': None,
    'credentials_id': None,
}


def _mock_secret_manager(mock_sm: MagicMock, version_name: str) -> MagicMock:
    client = MagicMock()
    client.get_secret_version.return_value.name = version_name
    payload = MagicMock()
    payload.name = version_name
    payload.payload.data = _TOKEN.encode('utf-8')
    client.access_secret_version.return_value = payload
    mock_sm.SecretManagerServiceClient.return_value = client
    return client


class TestCreate(TestCase):
    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.util.credentials_util.SeqeraApiClient',
    )
    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.resources.seqera_github_credential.secretmanager',
    )
    def test_create_posts_credentials_and_records_version(
        self,
        mock_sm: MagicMock,
        mock_api: MagicMock,
    ) -> None:
        _mock_secret_manager(mock_sm, _VERSION_V7)
        mock_api.call.return_value = {'credentialsId': _CRED_ID}

        result = _GithubCredentialProvider().create(_INPUTS)

        self.assertEqual(result.id, _CRED_ID)
        self.assertEqual(result.outs['credentials_id'], _CRED_ID)
        self.assertEqual(result.outs['resolved_secret_version'], _VERSION_V7)
        # Token is never returned in outs.
        self.assertNotIn(_TOKEN, str(result.outs))

        call_args = mock_api.call.call_args
        self.assertIn(f'workspaceId={_INPUTS["workspace_id"]}', call_args.args[1])
        body = call_args.args[2]['credentials']
        self.assertEqual(body['provider'], 'github')
        self.assertEqual(body['keys']['password'], _TOKEN)

    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.util.credentials_util.SeqeraApiClient',
    )
    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.resources.seqera_github_credential.secretmanager',
    )
    def test_create_raises_when_api_omits_credentials_id(
        self,
        mock_sm: MagicMock,
        mock_api: MagicMock,
    ) -> None:
        _mock_secret_manager(mock_sm, _VERSION_V7)
        mock_api.call.return_value = {}

        with self.assertRaises(ValueError):
            _GithubCredentialProvider().create(_INPUTS)


class TestDiff(TestCase):
    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.resources.seqera_github_credential.secretmanager',
    )
    def test_diff_detects_secret_rotation(self, mock_sm: MagicMock) -> None:
        _mock_secret_manager(mock_sm, _VERSION_V8)
        olds = {**_INPUTS, 'resolved_secret_version': _VERSION_V7}

        result = _GithubCredentialProvider().diff('id', olds, _INPUTS)

        self.assertTrue(result.changes)
        self.assertIsNone(result.replaces)

    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.resources.seqera_github_credential.secretmanager',
    )
    def test_diff_no_change_when_version_and_fields_stable(
        self, mock_sm: MagicMock,
    ) -> None:
        _mock_secret_manager(mock_sm, _VERSION_V7)
        olds = {**_INPUTS, 'resolved_secret_version': _VERSION_V7}

        result = _GithubCredentialProvider().diff('id', olds, _INPUTS)

        self.assertFalse(result.changes)

    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.resources.seqera_github_credential.secretmanager',
    )
    def test_diff_replaces_on_workspace_or_name_change(
        self, mock_sm: MagicMock,
    ) -> None:
        _mock_secret_manager(mock_sm, _VERSION_V7)
        olds = {**_INPUTS, 'resolved_secret_version': _VERSION_V7, 'workspace_id': 999}

        result = _GithubCredentialProvider().diff('id', olds, _INPUTS)

        self.assertTrue(result.changes)
        self.assertEqual(result.replaces, ['workspace_id'])


class TestUpdate(TestCase):
    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.util.credentials_util.SeqeraApiClient',
    )
    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.resources.seqera_github_credential.secretmanager',
    )
    def test_update_reposts_token_and_refreshes_version(
        self,
        mock_sm: MagicMock,
        mock_api: MagicMock,
    ) -> None:
        _mock_secret_manager(mock_sm, _VERSION_V8)
        mock_api.call.return_value = {}
        olds = {**_INPUTS, 'resolved_secret_version': _VERSION_V7}

        result = _GithubCredentialProvider().update(_CRED_ID, olds, _INPUTS)

        self.assertEqual(result.outs['resolved_secret_version'], _VERSION_V8)
        self.assertEqual(result.outs['credentials_id'], _CRED_ID)

        call_args = mock_api.call.call_args
        self.assertIn(f'/credentials/{_CRED_ID}', call_args.args[1])
        self.assertEqual(
            call_args.args[2]['credentials']['keys']['password'], _TOKEN,
        )


class TestDelete(TestCase):
    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.util.credentials_util.SeqeraApiClient',
    )
    def test_delete_calls_api(self, mock_api: MagicMock) -> None:
        mock_api.call.return_value = {}

        _GithubCredentialProvider().delete(
            _CRED_ID, {**_INPUTS, 'credentials_id': _CRED_ID},
        )

        mock_api.call.assert_called_once()
        self.assertIn(_CRED_ID, mock_api.call.call_args.args[1])

    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.util.credentials_util.SeqeraApiClient',
    )
    def test_delete_swallows_404(self, mock_api: MagicMock) -> None:
        mock_api.call.side_effect = SeqeraAPIError(
            'DELETE', '/credentials/x', HTTPStatus.NOT_FOUND, 'not found',
        )

        # Should not raise.
        _GithubCredentialProvider().delete(
            _CRED_ID, {**_INPUTS, 'credentials_id': _CRED_ID},
        )

    @patch(
        'cpg_infra.driver.dynamic_providers.seqera.util.credentials_util.SeqeraApiClient',
    )
    def test_delete_reraises_non_404(self, mock_api: MagicMock) -> None:
        mock_api.call.side_effect = SeqeraAPIError(
            'DELETE',
            '/credentials/x',
            HTTPStatus.INTERNAL_SERVER_ERROR,
            'boom',
        )

        with self.assertRaises(SeqeraAPIError):
            _GithubCredentialProvider().delete(
                _CRED_ID, {**_INPUTS, 'credentials_id': _CRED_ID},
            )
