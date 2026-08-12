"""
Smoke tests for the ``cpg_infra.driver`` package.

The `driver.py` → `driver/` package refactor in SET-1247 split ~1000 lines
of code across ten modules with a mix of top-level and TYPE_CHECKING imports.
These tests catch the classes of regression that split introduces:

  * a circular import that only surfaces on cold import
  * a missing re-export in ``driver/__init__.py``
  * a broken backward-compat alias (e.g. ``CPGInfrastructure.GroupProvider``
    no longer being the same object as ``GroupProvider``)
  * the side-effect imports in ``dataset_infrastructure.py`` not
    registering all expected ``CloudInfraBase`` subclasses
  * pytest accidentally collecting the legacy ``test()`` helper in
    ``driver/__init__.py``, which would run ``infra.main()`` against a live
    Pulumi engine
"""

from unittest import TestCase


class TestDriverPackageImports(TestCase):
    """Verify the driver package's public surface and backward-compat aliases."""

    def test_public_symbols_importable(self):
        """All names re-exported by ``driver/__init__.py`` resolve cleanly."""
        # Import inside the test so an import-time failure is attributed here
        # rather than to test discovery.
        from cpg_infra.driver import (  # noqa: F401
            CPGDatasetCloudInfrastructure,
            CPGDatasetInfrastructure,
            CPGInfrastructure,
            Group,
            GroupMember,
            GroupProvider,
            MainUploadBucket,
            NAME_TO_INFRA_CLASS,
            SampleMetadataAccessorMembership,
            access_levels,
            compute_hash,
            dict_to_toml,
        )

    def test_backward_compat_aliases(self):
        """Legacy nested-class access paths still resolve to the hoisted classes."""
        from cpg_infra.driver import (
            CPGInfrastructure,
            Group,
            GroupMember,
            GroupProvider,
        )

        self.assertIs(CPGInfrastructure.GroupProvider, GroupProvider)
        self.assertIs(GroupProvider.Group, Group)
        self.assertIs(Group.GroupMember, GroupMember)

    def test_infra_registry_populated(self):
        """``NAME_TO_INFRA_CLASS`` covers every concrete CloudInfraBase subclass."""
        from cpg_infra.driver import NAME_TO_INFRA_CLASS

        self.assertEqual(
            set(NAME_TO_INFRA_CLASS.keys()),
            {'gcp', 'azure', 'dry-run'},
        )

    def test_compute_hash_doctest(self):
        """The doctest example on ``compute_hash`` still holds."""
        from cpg_infra.driver import compute_hash

        self.assertEqual(
            compute_hash('dataset', 'hello.world@email.com', ''),
            'HW-d51b65ee',
        )

    def test_legacy_test_helper_not_collected_by_pytest(self):
        """``driver.test`` must carry ``__test__ = False`` so pytest skips it.

        The function calls ``infra.main()`` against a real Pulumi engine; if
        pytest ever collected it we would fire a deploy on every CI run.
        """
        from cpg_infra.driver import test as legacy_test_helper

        self.assertFalse(
            getattr(legacy_test_helper, '__test__', True),
            'driver.test lost its __test__ = False guard; pytest will try to '
            'collect it and run infra.main().',
        )
