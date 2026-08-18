"""Per-dataset Seqera Platform integration.

Creates the GCP service accounts, Workload Identity Federation pool +
OIDC provider, and IAM bindings that let Seqera Cloud run Google Batch
jobs on the dataset's behalf. Workspaces themselves are created manually
in Seqera and referenced via CPGInfrastructureConfig.seqera.workspace_ids.

Gating (should_setup_seqera) lives on CPGDatasetCloudInfrastructure —
this class assumes it is only instantiated when it should run.
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cpg_infra.config import SeqeraAccount
    from cpg_infra.driver.dataset_cloud_infrastructure import (
        CPGDatasetCloudInfrastructure,
    )


class DatasetSeqeraInfrastructure:
    """Owns all Seqera-related resources for one dataset."""

    def __init__(self, parent: CPGDatasetCloudInfrastructure) -> None:
        self._parent = parent
        self._config = parent.config
        self._dataset_config = parent.dataset_config
        self._infra = parent.infra

    @cached_property
    def accounts_by_access_level(self) -> dict[str, SeqeraAccount]:
        """The three Seqera SAs keyed by access level.

        Populated when setup() runs; empty until then.
        """
        return {}

    def setup(self) -> None:
        """Materialise WIF pool, provider, SAs, and IAM bindings.

        Idempotent — cached_properties gate resource creation.
        """
        # Filled in by Task 6.
        return
