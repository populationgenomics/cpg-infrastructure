"""Tests for GitHub WIF impersonation bindings (issue #409)."""

from __future__ import annotations

from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pulumi

from cpg_infra.github_wif.driver import get_sub_claim_prefix, grant_wif_impersonation

POOL = 'principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/github-pool/subject/'
IMMUTABLE_PREFIX = 'repo:populationgenomics@69879407/popgen-ancestry@1362376390'


class _Mocks(pulumi.runtime.Mocks):
    def new_resource(
        self, args: pulumi.runtime.MockResourceArgs
    ) -> tuple[str | None, dict[str, Any]]:
        return f'{args.name}-id', dict(args.inputs)

    def call(self, args: pulumi.runtime.MockCallArgs) -> dict[str, Any]:  # noqa: ARG002
        return {}


def _customization_response(body: dict[str, Any]) -> MagicMock:
    response = MagicMock()
    response.json.return_value = body
    return response


@patch.dict('os.environ', {'GITHUB_TOKEN': 'test-token'})
class TestGetSubClaimPrefix(TestCase):
    @patch('cpg_infra.github_wif.driver.requests.get')
    def test_returns_prefix_from_github(self, get: MagicMock):
        get.return_value = _customization_response(
            {
                'use_default': True,
                'use_immutable_subject': True,
                'sub_claim_prefix': IMMUTABLE_PREFIX,
            }
        )

        self.assertEqual(
            get_sub_claim_prefix('populationgenomics/popgen-ancestry'),
            IMMUTABLE_PREFIX,
        )
        self.assertEqual(
            get.call_args.args[0],
            'https://api.github.com/repos/populationgenomics/popgen-ancestry/actions/oidc/customization/sub',
        )

    @patch('cpg_infra.github_wif.driver.requests.get')
    def test_rejects_custom_template(self, get: MagicMock):
        get.return_value = _customization_response(
            {
                'use_default': False,
                'include_claim_keys': ['repo', 'context'],
                'sub_claim_prefix': 'repo:populationgenomics/my-repo',
            }
        )

        with self.assertRaisesRegex(ValueError, 'custom OIDC subject claim template'):
            get_sub_claim_prefix('populationgenomics/my-repo')

    @patch.dict('os.environ', {}, clear=True)
    def test_requires_github_token(self):
        with self.assertRaisesRegex(ValueError, 'GITHUB_TOKEN'):
            get_sub_claim_prefix('populationgenomics/my-repo')


class TestGrantWifImpersonation(TestCase):
    def setUp(self):
        pulumi.runtime.set_mocks(_Mocks(), preview=False)

    @pulumi.runtime.test
    def test_binds_immutable_subject(self):
        binding = grant_wif_impersonation(
            'cpg-proj-popgen-ancestry-development-wif-binding',
            'projects/cpg-proj/serviceAccounts/sa@cpg-proj.iam.gserviceaccount.com',
            '123',
            IMMUTABLE_PREFIX,
            'development',
        )

        def check(member: str) -> None:
            self.assertEqual(
                member, f'{POOL}{IMMUTABLE_PREFIX}:environment:development'
            )

        return binding.member.apply(check)

    @pulumi.runtime.test
    def test_binds_name_only_subject(self):
        binding = grant_wif_impersonation(
            'cpg-proj-my-repo-production-wif-binding',
            'projects/cpg-proj/serviceAccounts/sa@cpg-proj.iam.gserviceaccount.com',
            '123',
            'repo:populationgenomics/my-repo',
            'production',
        )

        def check(member: str) -> None:
            self.assertEqual(
                member,
                f'{POOL}repo:populationgenomics/my-repo:environment:production',
            )

        return binding.member.apply(check)
