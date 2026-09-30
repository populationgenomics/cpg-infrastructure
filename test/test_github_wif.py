"""Tests for GitHub WIF impersonation bindings (issue #409)."""

from __future__ import annotations

from typing import Any
from unittest import TestCase

import pulumi

REPO_ID = 1362376390


class _Mocks(pulumi.runtime.Mocks):
    def __init__(self) -> None:
        self.resources: dict[str, dict[str, Any]] = {}

    def new_resource(
        self, args: pulumi.runtime.MockResourceArgs
    ) -> tuple[str | None, dict[str, Any]]:
        self.resources[args.name] = dict(args.inputs)
        return f'{args.name}-id', dict(args.inputs)

    def call(self, args: pulumi.runtime.MockCallArgs) -> dict[str, Any]:
        if args.token == 'github:index/getRepository:getRepository':  # noqa: S105
            return {'repoId': REPO_ID, 'fullName': args.args['fullName']}
        return {}


_mocks = _Mocks()
pulumi.runtime.set_mocks(_mocks, preview=False)

from cpg_infra.github_wif.driver import grant_wif_impersonation  # noqa: E402

POOL = 'principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/github-pool/subject/'


class TestGrantWifImpersonation(TestCase):
    @pulumi.runtime.test
    def test_binds_legacy_and_immutable_subjects(self):
        bindings = grant_wif_impersonation(
            'cpg-proj-popgen-ancestry-development-wif-binding',
            'projects/cpg-proj/serviceAccounts/sa@cpg-proj.iam.gserviceaccount.com',
            '123',
            'populationgenomics/popgen-ancestry',
            'development',
        )

        def check(members: list[str]) -> None:
            self.assertEqual(
                members,
                [
                    f'{POOL}repo:populationgenomics/popgen-ancestry:environment:development',
                    f'{POOL}repo:populationgenomics@69879407/popgen-ancestry@{REPO_ID}'
                    ':environment:development',
                ],
            )
            # Legacy binding keeps its original resource name
            self.assertIn(
                'cpg-proj-popgen-ancestry-development-wif-binding', _mocks.resources
            )
            self.assertIn(
                'cpg-proj-popgen-ancestry-development-wif-binding-immutable',
                _mocks.resources,
            )

        return pulumi.Output.all(*[b.member for b in bindings]).apply(check)
